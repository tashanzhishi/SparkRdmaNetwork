//
// Created by wyb on 17-5-24.
//

#ifndef SPARKRDMA_RDMA_EVENT_LOOP_H
#define SPARKRDMA_RDMA_EVENT_LOOP_H

#include <sys/eventfd.h>

#include <vector>
#include <functional>

#include "rdma_channel.h"
#include "rdma_thread.h

namespace SparkRdmaNetwork {

typedef std::function<void(void *)> HandleFunction;

class RdmaEvent {
public:
  RdmaEvent();
  ~RdmaEvent();

  int Poll(int timeout);

  void Pollset_add(int fd) {
    pollset_.push_back(fd);
    EventfdWakeup();
  };

  void HandleChannelEvent(void *rdma_channel);
  void HandleRecvDataEvent(void *rdma_channel);
  void HandleReqRpcEvent(void *rdma_channel);
  void HandleAckRpcEvent(void *rdma_channel);

private:
  int EventfdWakeup();
  int EventfdConsume();
  int wakeup_fd_;
  std::vector<int> pollset_;
  boost::basic_thread_pool thread_pool_;

  // no copy and =
  RdmaEvent(RdmaEvent &) = delete;
  RdmaEvent &operator=(RdmaEvent &) = delete;
};

} // namespace SparkRdmaNetwork

#endif //SPARKRDMA_RDMA_EVENT_LOOP_H
