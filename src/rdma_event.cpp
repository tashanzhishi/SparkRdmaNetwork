//
// Created by wyb on 17-5-24.
//

#include "rdma_event.h"

#include <cstddef>
#include <poll.h>
#include <fcntl.h>
#include <infiniband/verbs.h>

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
      ibv_comp_channel *recv_cq_channel= nullptr;
      {
        ReadLock(RdmaChannel::Fd2ChannelLock);
        recv_cq_channel = RdmaChannel::Fd2Channel.at(pfds[i].fd)->get_completion_cq()->get_recv_cq_channel();
      }
      HandleFunction handle = std::bind(&RdmaEventLoop::HandleChannelEvent, this, std::placeholders::_1);
      thread_pool_.submit(std::bind(handle, recv_cq_channel));
    }
  }

  return 0;
}

void RdmaEventLoop::HandleChannelEvent(void *rdma_channel) {
  ibv_comp_channel *recv_cq_channel = (ibv_comp_channel *)rdma_channel;
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

  int event_num = 1;
  do {
    ibv_wc wc;
    event_num = ibv_poll_cq(ev_cq, 1, &wc);
    if (event_num < 0) {
      RDMA_ERROR("ibv_poll_cq poll failed");
      abort();
    }
  } while (event_num);
}

} // namespace SparkRdmaNetwork

