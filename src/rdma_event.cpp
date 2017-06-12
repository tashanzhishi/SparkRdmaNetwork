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

boost::basic_thread_pool RdmaEvent::thread_pool_(20);

RdmaEvent::RdmaEvent(std::string ip, ibv_comp_channel *recv_cq_channel, QueuePair *qp) {
  kill_fd_ = EventfdCreate();
  if (kill_fd_ < 0) {
    RDMA_ERROR("create wakeup_fd failed");
    abort();
  }
  ip_ = ip;
  recv_cq_channel_ = recv_cq_channel;
  fd_ = recv_cq_channel->fd;
  qp_ = qp;

  PollFunction poll_func = std::bind(&RdmaEvent::PollThreadFunc, this);
  thread_pool_.submit(poll_func);
}

RdmaEvent::~RdmaEvent() {
  KillPollThread();
}

int RdmaEvent::EventfdCreate() {
  int fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (fd < 0) {
    RDMA_ERROR("eventfd error, {}", strerror(errno));
    return -1;
  }
  return fd;
}

int RdmaEvent::EventfdWakeup(int fd) {
  int ret;
  do {
    ret = eventfd_write(fd, 1);
  } while (ret < 0 && errno == EINTR);
  if (ret < 0) {
    RDMA_ERROR("eventfd_write error, {}", strerror(errno));
    abort();
  }
  return 0;
}

int RdmaEvent::EventfdConsume(int fd) {
  eventfd_t value;
  int ret;
  do {
    ret = eventfd_read(fd, &value);
  } while (ret < 0 && errno == EINTR);
  if (ret < 0 && errno != EAGAIN) {
    RDMA_ERROR("eventfd_read error, {}", strerror(errno));
    abort();
  }
  return 0;
}

int RdmaEvent::KillPollThread() {
  EventfdWakeup(kill_fd_);
  return 0;
}

void RdmaEvent::PollThreadFunc() {
  while (1) {
    int ret = Poll(500);
    if (ret == 1) {
      RDMA_INFO("poll thrad end");
      return;
    } else if (ret == -1) {
      RDMA_ERROR("poll thread failed");
      abort();
    }
  }
}

// one poll thread
int RdmaEvent::Poll(int timeout) {
  struct pollfd pfds[2];
  pfds[0].fd = kill_fd_;
  pfds[0].events = POLLIN;
  pfds[0].revents = 0;
  pfds[1].fd = fd_;
  pfds[1].events = POLLIN;
  pfds[1].revents = 0;

  int ret = 0;
  do {
    ret = poll(pfds, 2, -1);
    if (ret == -1) {
      RDMA_ERROR("poll error: {}", strerror(errno));
      return -1;
    }
  } while (ret == 0 || (ret == -1 && errno == EINTR));

  if (pfds[0].revents & POLLIN) {
    RDMA_INFO("kill poll thread {}", ip_);
    EventfdConsume(pfds[0].fd);
    close(kill_fd_);
    return 1;
  }
  int recv_event_num = 0, send_event_num = 0;;
  if (pfds[1].revents & POLLIN) {
    ibv_cq *ev_cq;
    void *ev_ctx;
    if (ibv_get_cq_event(recv_cq_channel_, &ev_cq, &ev_ctx) < 0) {
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
        break;
      } else {
        if (wc.status != IBV_WC_SUCCESS) {
          RDMA_ERROR("ibv_poll_cq error, {}", QueuePair::WcStatusToString(wc.status));
          abort();
        }
        if (wc.opcode != IBV_WC_RECV) {
          RDMA_ERROR("all poll event must be recv");
          abort();
        }
        BufferDescriptor *bd = (BufferDescriptor *) wc.wr_id;
        bd->bytes_ = wc.byte_len;
        RdmaDataHeader *header = (RdmaDataHeader *) bd->buffer_;
        if (header->data_type == TYPE_SMALL_DATA || header->data_type == TYPE_WRITE_SUCCESS) {
          {
            std::lock_guard lock(recv_data_lock_);
            recv_data_[header->data_id] = bd;
          }
          recv_event_num++;
          {
            std::lock_guard lock(recv_running_lock_);
            if (recv_runing_ == 0) {
              HandleFunction handle = std::bind(&RdmaEvent::HandleRecvDataEvent, this);
              thread_pool_.submit(handle);
              recv_runing_ = 1;
            }
          }
        } else if (header->data_type == TYPE_RPC_REQ || header->data_type == TYPE_RPC_ACK) {
          send_data_.push(bd);
          send_event_num++;
          {
            std::lock_guard lock(send_running_lock_);
            if (send_running_ == 0) {
              HandleFunction handle = std::bind(&RdmaEvent::HandleSendDataEvent, this);
              thread_pool_.submit(handle);
              send_running_ = 1;
            }
          }
        } else {
          RDMA_ERROR("unknow recv data header, maybe send error or recv error");
          abort();
        }
      }
    } while (event_num);
  }

  return 0;
}

// recv small data, or big data successed writed
void RdmaEvent::HandleRecvDataEvent() {
  while (1) {
    BufferDescriptor *bd = nullptr;
    uint32_t data_id = recv_data_id_;
    {
      std::lock_guard lock(recv_data_lock_);
      if (recv_data_.find(data_id) == recv_data_.end()) {
        recv_runing_ = 0;
        return;
      }
      bd = recv_data_[data_id];
      recv_data_.erase(data_id);
    }

    int recv_or_free = 0;
    RdmaDataHeader *header = (RdmaDataHeader *)bd->buffer_;
    uint8_t *copy_buff = nullptr;
    int start = 0, len = 0;
    if (header->data_type == TYPE_SMALL_DATA) {
      copy_buff = bd->buffer_;
      start = sizeof(RdmaDataHeader);
      len = header->data_len - sizeof(RdmaDataHeader);
      recv_or_free = 1;
    } else if (header->data_type == TYPE_WRITE_SUCCESS) {
      RdmaRpc *rpc = (RdmaRpc *)bd->buffer_;
      copy_buff = (uint8_t *)rpc->addr;
      start = sizeof(RdmaDataHeader);
      len = rpc->data_len - sizeof(RdmaDataHeader);

      bd->bytes_ = k1KB;
      if (qp_->PostReceiveOneWithBuffer(bd, BIG_SIGN) < 0) {
        RDMA_ERROR("post recv with reuse data failed");
        abort();
      }
      recv_or_free = 2;
    } else {
      RDMA_ERROR("the recv buffer type must be small_data or write_success");
      abort();
    }

    jbyteArray jba = jni_alloc_byte_array(len);
    set_byte_array_region(jba, start, len, copy_buff);
    jni_channel_callback(ip_.c_str(), jba, len);

    if (recv_or_free == 1) {
      bd->bytes_ = k1KB;
      if (qp_->PostReceiveOneWithBuffer(bd, SMALL_SIGN) < 0) {
        RDMA_ERROR("post recv with reuse data failed");
        abort();
      }
    } else if (recv_or_free == 2) {
      RFREE(copy_buff, start + len);
    } else {
      RDMA_ERROR("recv_or_free = 1 or 2, but now is {}", recv_or_free);
      abort();
    }

    recv_data_id_++;
    RDMA_DEBUG("handle a data success {}", data_id);
  }
}

void RdmaEvent::HandleSendDataEvent() {
  while (1) {
    if (send_data_.empty()) {
      std::lock_guard lock(send_running_lock_);
      if (send_data_.empty()) {
        send_running_ = 0;
        return;
      }
    }
    BufferDescriptor *bd;
    send_data_.pop(bd);
    RdmaDataHeader *header = (RdmaDataHeader *)bd->buffer_;
    if (header->data_type == TYPE_RPC_REQ) {
      HandleReqRpcEvent(bd);
    } else if (header->data_type == TYPE_RPC_ACK) {
      HandleAckRpcEvent(bd);
    } else {
      RDMA_ERROR("handle send event's data_type must be rpc_req or rpc_ack");
      abort();
    }
  }
}

// recv a rpc contain a size which writed by peer, and should malloc registed memory
// and send the addr to peer
// must not modify bd->buffer_, bd->mr_.
void RdmaEvent::HandleReqRpcEvent(BufferDescriptor *bd) {
  GPR_ASSERT(sizeof(RdmaDataHeader) == bd->bytes_);
  RdmaDataHeader *header = (RdmaDataHeader *)bd->buffer_;
  GPR_ASSERT(header->data_type == TYPE_RPC_REQ);

  uint32_t len = header->data_len;
  uint32_t data_id = header->data_id;

  uint8_t *buffer = (uint8_t *)RMALLOC(len);
  uint32_t rkey = GET_MR(buffer)->rkey;

  // send addr to peer
  RdmaRpc *ack_data = (RdmaRpc*)bd->buffer_;
  ack_data->data_type = TYPE_RPC_ACK;
  ack_data->addr = (uint64_t) buffer;
  ack_data->rkey = rkey;
  ack_data->data_id = data_id;

  bd->bytes_ = sizeof(RdmaRpc);
  bd->channel_ = nullptr;
  if (qp_->PostSendAndWait(bd, 1, BIG_SIGN) < 0) {
    RDMA_ERROR("PostSendAndWait ack rpc failed");
    abort();
  }

  bd->bytes_ = k1KB;
  if (qp_->PostReceiveOneWithBuffer(bd, BIG_SIGN) < 0) {
    RDMA_ERROR("post recv with reuse data failed");
    abort();
  }
}

// recv addr of peer, so write big data to peer and send write success message
void RdmaEvent::HandleAckRpcEvent(BufferDescriptor *bd) {
  GPR_ASSERT(sizeof(RdmaRpc) == bd->bytes_);
  RdmaRpc *header = (RdmaRpc *)bd->buffer_;
  GPR_ASSERT(header->data_type == TYPE_RPC_ACK);

  uint64_t addr = header->addr;
  uint32_t rkey = header->rkey;
  uint32_t data_id = header->data_id;

  std::pair<BufferDescriptor*, int> write_data = GetDataById(data_id);
  if (write_data.first == nullptr && write_data.second == 0) {
    RDMA_ERROR("get big data from data_id failed. data_id: {}", data_id);
    abort();
  } else {
    BufferDescriptor *buff = write_data.first;
    int num = write_data.second;
    RdmaDataHeader *rdma_header = (RdmaDataHeader *)buff[0].buffer_;
    rdma_header->data_type = TYPE_BIG_DATA;

    qp_->PostWriteAndWait(buff, num, addr, rkey);
    for (int i = 0; i < num; ++i) {
      RFREE(buff[i].buffer_, buff[i].bytes_);
    }
    if (num == 1) {
      delete buff;
    } else if (num == 2 || num == 3) {
      delete[] buff;
    } else {
      RDMA_ERROR("buffer num must be 1, 2, 3, but it is {}", num);
      abort();
    }

    header->data_type = TYPE_WRITE_SUCCESS;
    if (qp_->PostSendAndWait(bd, 1, false) < 0) {
      RDMA_ERROR("PostSendAndWait write success failed");
      abort();
    }

    bd->bytes_ = k1KB;
    if (qp_->PostReceiveOneWithBuffer(bd, BIG_SIGN) < 0) {
      RDMA_ERROR("post recv with reuse data failed");
      abort();
    }
  }
}

std::pair<BufferDescriptor*, int> RdmaEvent::GetDataById(uint32_t id) {
  std::lock_guard lock(id2data_lock_);
  if (id2data_.find(id) == id2data_.end()) {
    RDMA_ERROR("id2data canot find data_id {}", id);
    return std::pair<BufferDescriptor*, int>(nullptr, 0);
  }
  std::pair<BufferDescriptor*, int> data = id2data_[id];
  id2data_.erase(id);
  return data;
}

void RdmaEvent::PutDataById(uint32_t id, BufferDescriptor *buf, int num) {
  std::lock_guard lock(id2data_lock_);
  id2data_[id] = std::pair<BufferDescriptor*, int>(buf, num);
}

} // namespace SparkRdmaNetwork

