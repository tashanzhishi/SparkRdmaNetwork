//
// Created by wyb on 17-5-24.
//

#include "rdma_event.h"

#include <cstddef>
#include <poll.h>
#include <fcntl.h>
#include <infiniband/verbs.h>

#include <queue>

#include "rdma_logger.h"
#include "rdma_thread.h"
#include "rdma_memory_pool.h"
#include "jni_common.h"

namespace SparkRdmaNetwork {

RdmaEvent::RdmaEvent() : thread_pool_(20) {
  wakeup_fd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (wakeup_fd_ < 0) {
    RDMA_ERROR("eventfd error, {}", strerror(errno));
    abort();
  }
  pollset_.push_back(wakeup_fd_);
}

RdmaEvent::~RdmaEvent() {
  thread_pool_.close();
}

int RdmaEvent::EventfdWakeup() {
  int ret;
  do {
    ret = eventfd_write(wakeup_fd_, 1);
  } while (ret < 0 && errno == EINTR);
  if (ret < 0) {
    RDMA_ERROR("eventfd_write error, {}", strerror(errno));
    abort();
  }
  return 0;
}

int RdmaEvent::EventfdConsume() {
  eventfd_t value;
  int ret;
  do {
    ret = eventfd_read(wakeup_fd_, &value);
  } while (ret < 0 && errno == EINTR);
  if (ret < 0 && errno != EAGAIN) {
    RDMA_ERROR("eventfd_read error, {}", strerror(errno));
    abort();
  }
  return 0;
}

// one poll thread
int RdmaEvent::Poll(int timeout) {
  size_t fd_count = pollset_.size();
  struct pollfd pfds[fd_count];
  pfds[0].fd = wakeup_fd_;
  pfds[0].events = POLLIN;
  pfds[0].revents = 0;
  for (int i = 1; i < fd_count; ++i) {
    pfds[i].fd = pollset_[i];
    pfds[i].events = POLLIN;
    pfds[i].revents = 0;
  }

  int ret = 0;
  do {
    ret = poll(pfds, fd_count, timeout);
    if (ret == -1) {
      RDMA_ERROR("poll error: {}", strerror(errno));
      abort();
    }
  } while (ret == 0 || (ret == -1 && errno == EINTR));

  if (pfds[0].revents & POLLIN) {
    RDMA_INFO("add a fd to pollset");
    EventfdConsume();
  }

  for (int i = 1; i < fd_count; ++i) {
    if (pfds[i].revents & POLLIN) {
      RdmaChannel *channel = nullptr;
      {
        ReadLock(RdmaChannel::Fd2ChannelLock);
        channel = RdmaChannel::Fd2Channel.at(pfds[i].fd);
      }
      HandleFunction handle = std::bind(&RdmaEvent::HandleChannelEvent, this, std::placeholders::_1);
      thread_pool_.submit(std::bind(handle, channel));
    }
  }

  return 0;
}

void RdmaEvent::HandleChannelEvent(void *rdma_channel) {
  RdmaChannel *channel = (RdmaChannel *)rdma_channel;
  ibv_comp_channel *recv_cq_channel = channel->get_completion_cq()->get_recv_cq_channel();
  ibv_cq *ev_cq;
  void *ev_ctx;

  if (ibv_get_cq_event(recv_cq_channel, &ev_cq, &ev_ctx) < 0) {
    RDMA_ERROR("ibv_get_cq_event error, {}", strerror(errno));
    abort();
  }

  ibv_ack_cq_events(ev_cq, 1);

  if (ibv_req_notify_cq(ev_cq, 0) < 0) {
    RDMA_ERROR("ibv_req_notify_cq error, {}", strerror(errno));
    abort();
  }

  int event_num;
  do {
    ibv_wc wc;
    event_num = ibv_poll_cq(ev_cq, 1, &wc);
    if (event_num < 0) {
      RDMA_ERROR("ibv_poll_cq poll failed");
      abort();
    } else if (event_num == 0) {
      continue;
    } else {
      for (int i = 0; i < event_num; ++i) {
        if (wc.status != IBV_WC_SUCCESS) {
          RDMA_ERROR("ibv_poll_cq error, {}", QueuePair::WcStatusToString(wc.status));
          abort();
        }
        if (wc.opcode != IBV_WC_RECV) {
          RDMA_ERROR("all poll event must be recv");
          abort();
        }
        BufferDescriptor *bd = (BufferDescriptor *)wc.wr_id;
        bd->bytes_ = wc.byte_len;
        RdmaDataHeader *header = (RdmaDataHeader *)bd->buffer_;
        if (header->data_type == TYPE_SMALL_DATA) {
          channel->recv_data_.push(bd);
        } else if (header->data_type == TYPE_BIG_DATA) {
          channel->recv_data_.push(bd);
        } else if (header->data_type == TYPE_RPC_REQ) {
          channel->req_rpc_.push(bd);
        } else if (header->data_type == TYPE_RPC_ACK) {
          channel->ack_rpc_.push(bd);
        } else {
          RDMA_ERROR("unknow recv data header, maybe send error or recv error");
          abort();
        }
      }
    }
  } while (event_num);
}

// recv small data, or big data successed writed
void RdmaEvent::HandleRecvDataEvent(void *rdma_channel) {
  RdmaChannel *channel = (RdmaChannel *)rdma_channel;
  while (!channel->recv_data_.empty()) {
    BufferDescriptor bd;
    channel->recv_data_.pop(bd);

    RdmaDataHeader *header = (RdmaDataHeader *)bd.buffer_;
    uint8_t *copy_buff = nullptr;
    int start = 0, len = 0;
    if (header->data_type == TYPE_SMALL_DATA) {
      copy_buff = bd.buffer_;
      start = sizeof(RdmaDataHeader);
      len = header->data_len - sizeof(RdmaDataHeader);
    } else if (header->data_type == TYPE_WRITE_SUCCESS) {
      RdmaRpc *rpc = (RdmaRpc *)bd.buffer_;
      copy_buff = (uint8_t *)rpc->addr;
      start = sizeof(RdmaRpc);
      len = rpc->data_len - sizeof(RdmaRpc);
      RFREE(rpc, k1KB);
    } else {
      RDMA_ERROR("the recv buffer type must be small_data or write_success");
      abort();
    }

    jbyteArray jba = jni_alloc_byte_array(len);
    set_byte_array_region(jba, start, len, copy_buff);
    jni_channel_callback(channel->get_ip().c_str(), jba, len);

    RFREE(copy_buff, start + len);

    RDMA_DEBUG("handle a data success");
  }
  channel->recv_data_running_ = false;
}

// recv a rpc contain a size which writed by peer, and should malloc registed memory
// and send the addr to peer
void RdmaEvent::HandleReqRpcEvent(void *rdma_channel) {
  RdmaChannel *channel = (RdmaChannel *)rdma_channel;
  QueuePair *qp = channel->get_queue_pair();
  while (!channel->req_rpc_.empty()) {
    BufferDescriptor bd;
    channel->req_rpc_.pop(bd);
    GPR_ASSERT(sizeof(RdmaDataHeader) == bd.bytes_);
    RdmaDataHeader *header = (RdmaDataHeader *)bd.buffer_;
    uint32_t len = header->data_len;
    uint32_t data_id = header->data_id;

    uint8_t *buffer = (uint8_t*)RMALLOC(len);
    uint32_t rkey = GET_MR(buffer)->rkey;

    // send addr to peer
    RdmaRpc *ack_data = (RdmaRpc *)RMALLOC(sizeof(RdmaRpc));
    ack_data->data_type = TYPE_RPC_ACK;
    ack_data->addr = (uint64_t)buffer;
    ack_data->rkey = rkey;
    ack_data->data_id = data_id;

    bd.buffer_ = (uint8_t*)ack_data;
    bd.bytes_ = sizeof(RdmaRpc);
    bd.mr_ = GET_MR(ack_data);
    bd.channel_ = channel;
    if (qp->PostSendAndWait(&bd, 1, false) < 0) {
      RDMA_ERROR("PostSendAndWait ack rpc failed");
      abort();
    }
    RFREE(ack_data, sizeof(RdmaRpc));
  }
  channel->recv_data_running_ = false;
}

// recv addr of peer, so write big data to peer and send write success message
void RdmaEvent::HandleAckRpcEvent(void *rdma_channel) {
  RdmaChannel *channel = (RdmaChannel *)rdma_channel;
  QueuePair *qp = channel->get_queue_pair();
  while (!channel->ack_rpc_.empty()) {
    BufferDescriptor bd;
    channel->ack_rpc_.pop(bd);
    GPR_ASSERT(sizeof(RdmaRpc) == bd.bytes_);
    RdmaRpc *header = (RdmaRpc *)bd.buffer_;
    uint64_t addr = header->addr;
    uint32_t rkey = header->rkey;
    uint32_t data_id = header->data_id;

    std::pair<BufferDescriptor*, int> write_data = channel->get_data_from_id(data_id);
    if (write_data.first == nullptr && write_data.second == 0) {
      RDMA_ERROR("get big data from data_id failed. data_id: {}", data_id);
      abort();
    } else {
      BufferDescriptor *buff = write_data.first;
      int num = write_data.second;
      RdmaDataHeader *rdma_header = (RdmaDataHeader *)buff[0].buffer_;
      rdma_header->data_type = TYPE_BIG_DATA;

      qp->PostWriteAndWait(buff, num, addr, rkey);
      for (int i = 0; i < num; ++i) {
        RFREE(buff[i].buffer_, buff[i].bytes_);
      }
      if (num == 1) {
        delete buff;
      } else if (num >= 2) {
        delete[] buff;
      } else {
        RDMA_ERROR("buffer num must be 1, 2, 3, but it is {}", num);
        abort();
      }

      header->data_type = TYPE_WRITE_SUCCESS;
      if (qp->PostSendAndWait(&bd, 1, false) < 0) {
        RDMA_ERROR("PostSendAndWait write success failed");
        abort();
      }
      RFREE(header, k1KB);
    }
  }
  channel->ack_rpc_running_ = false;
}

} // namespace SparkRdmaNetwork

