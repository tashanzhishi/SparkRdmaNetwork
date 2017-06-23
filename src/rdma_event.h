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

#include "rdma_thread.h"
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

  static boost::basic_thread_pool thread_pool_;

private:
  int PollSendCq(int timeout);
  int PollRecvCq(int timeout);
  void PollRecvThreadFunc();
  void PollSendThreadFunc();

  int EventfdCreate();
  int EventfdWakeup(int fd);
  int EventfdConsume(int fd);

  std::pair<BufferDescriptor*, int> GetDataById(uint32_t id);

  void HandleRecvDataEvent();
  void HandleRecvReqRpc(BufferDescriptor *);
  void HandleRecvAckRpc(BufferDescriptor *);

  int kill_send_fd_;
  int kill_recv_fd_;
  std::string ip_;
  int send_fd_;
  int recv_fd_;
  ibv_comp_channel *send_cq_channel_;
  ibv_comp_channel *recv_cq_channel_;
  QueuePair *qp_;
  void *rdma_channel;

  std::map<uint32_t, BufferDescriptor*> recv_data_;
  std::mutex recv_data_lock_;
  std::atomic_uint recv_data_id_;
  int recv_runing_;

  std::map<uint32_t, std::pair<BufferDescriptor*, int> > id2data_;
  std::mutex id2data_lock_;

  // no copy and =
  RdmaEvent(RdmaEvent &) = delete;
  RdmaEvent &operator=(RdmaEvent &) = delete;
};

int test_print(uint8_t *out, int len, char *mark);

} // namespace SparkRdmaNetwork

#endif //SPARKRDMA_RDMA_EVENT_LOOP_H
