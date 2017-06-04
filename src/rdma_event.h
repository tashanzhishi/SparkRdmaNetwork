//
// Created by wyb on 17-5-24.
//

#ifndef SPARKRDMA_RDMA_EVENT_LOOP_H
#define SPARKRDMA_RDMA_EVENT_LOOP_H

#include <vector>
#include <functional>

#include "rdma_channel.h"
#include "rdma_thread.h

namespace SparkRdmaNetwork {

typedef std::function<void(void *)> HandleFunction;

class RdmaEventLoop {
public:
  RdmaEventLoop();
  ~RdmaEventLoop();

  int Poll(int timeout);

  void Pollset_add(int fd) {
    pollset_.push_back(fd);
  };

  void HandleChannelEvent(void *rdma_channel);
  void HandleRecvDataEvent(void *rdma_channel);
  void HandleReqRpcEvent(void *rdma_channel);
  void HandleAckRpcEvent(void *rdma_channel);

private:
  int wakeup_fd_;
  std::vector<int> pollset_;
  boost::basic_thread_pool thread_pool_;

  // no copy and =
  RdmaEventLoop(RdmaEventLoop &) = delete;
  RdmaEventLoop &operator=(RdmaEventLoop &) = delete;
};

} // namespace SparkRdmaNetwork

#endif //SPARKRDMA_RDMA_EVENT_LOOP_H
