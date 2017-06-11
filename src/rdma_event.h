//
// Created by wyb on 17-5-24.
//

#ifndef SPARKRDMA_RDMA_EVENT_LOOP_H
#define SPARKRDMA_RDMA_EVENT_LOOP_H

#include <sys/eventfd.h>

#include <vector>
#include <functional>
#include <map>
#include <mutex>
#include <infiniband/verbs.h>
#include <boost/lockfree/queue.hpp>

#include "rdma_thread.h
#include "rdma_infiniband.h"

namespace SparkRdmaNetwork {

typedef std::function<void()> HandleFunction;
typedef std::function<void()> PollFunction;

class RdmaEvent {
public:
  RdmaEvent(std::string ip, ibv_comp_channel *recv_cq_channel, QueuePair *qp);
  ~RdmaEvent();

  int KillPollThread();

  void PutDataById(uint32_t id, BufferDescriptor *buf, int num);

private:
  int Poll(int timeout);
  void PollThreadFunc();

  int EventfdCreate();
  int EventfdWakeup(int fd);
  int EventfdConsume(int fd);

  std::pair<BufferDescriptor*, int> GetDataById(uint32_t id);

  void HandleRecvDataEvent();
  void HandleSendDataEvent();
  void HandleReqRpcEvent(BufferDescriptor *);
  void HandleAckRpcEvent(BufferDescriptor *);

  static boost::basic_thread_pool thread_pool_;

  int kill_fd_;
  std::string ip_;
  int fd_;
  ibv_comp_channel *recv_cq_channel_;
  QueuePair *qp_;
  void *rdma_channel;

  std::map<uint32_t, BufferDescriptor*> recv_data_;
  std::mutex recv_data_lock_;
  std::atomic_uint recv_data_id_;
  boost::lockfree::queue<BufferDescriptor*> send_data_;
  int send_running_, recv_runing_;
  std::mutex send_running_lock_, recv_running_lock_;

  std::map<uint32_t, std::pair<BufferDescriptor*, int> > id2data_;
  std::mutex id2data_lock_;

  // no copy and =
  RdmaEvent(RdmaEvent &) = delete;
  RdmaEvent &operator=(RdmaEvent &) = delete;
};

} // namespace SparkRdmaNetwork

#endif //SPARKRDMA_RDMA_EVENT_LOOP_H
