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

namespace SparkRdmaNetwork {

RdmaEventLoop::RdmaEventLoop() : thread_pool_(20){
  pollset_.push_back(wakeup_fd_);
}

RdmaEventLoop::~RdmaEventLoop() {
  thread_pool_.close();
}

// one poll thread
int RdmaEventLoop::Poll(int timeout) {
  size_t fd_count = pollset_.size();
  struct pollfd pfds[fd_count];
  pfds[0].fd = wakeup_fd_;
  pfds[0].events = POLLIN;
  pfds[0].revents = 0;
  for (int i = 0; i < fd_count; ++i) {
    pfds[i].fd = pollset_[i];
    pfds[i].events = 0;
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

  for (int i = 1; i < fd_count; ++i) {
    if (pfds[i].revents & POLLIN) {
      //ibv_comp_channel *recv_cq_channel= nullptr;
      RdmaChannel *channel = nullptr;
      {
        ReadLock(RdmaChannel::Fd2ChannelLock);
        //recv_cq_channel = RdmaChannel::Fd2Channel.at(pfds[i].fd)->get_completion_cq()->get_recv_cq_channel();
        channel = RdmaChannel::Fd2Channel.at(pfds[i].fd);
      }
      HandleFunction handle = std::bind(&RdmaEventLoop::HandleChannelEvent, this, std::placeholders::_1);
      thread_pool_.submit(std::bind(handle, channel));
    }
  }

  return 0;
}

void RdmaEventLoop::HandleChannelEvent(void *rdma_channel) {
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
          channel->small_data_.push(bd);
        } else if (header->data_type == TYPE_BIG_DATA) {
          channel->big_data_.push(bd);
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

} // namespace SparkRdmaNetwork

