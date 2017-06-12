//
// Created by wyb on 17-5-15.
//

#ifndef SPARKRDMA_RDMA_CHANNEL_H
#define SPARKRDMA_RDMA_CHANNEL_H

#include <cstdint>

#include <string>
#include <map>
#include <atomic>
#include <boost/thread/shared_mutex.hpp>
#include <boost/lockfree/queue.hpp>

#include "rdma_socket.h"
#include "rdma_infiniband.h"
#include "rdma_thread.h"
#include "rdma_memory_pool.h"
#include "rdma_event.h"

namespace SparkRdmaNetwork {

class RdmaChannel {
public:
  static RdmaChannel *GetChannelByIp(const std::string &ip);
  static void DestroyAllChannel();

  RdmaChannel(const char *host = nullptr, uint16_t port = kDefaultPort);
  ~RdmaChannel();

  // connect to server
  int Init(const char *host, uint16_t port);
  // the host and port is not used
  int SendMsg(const char *host, uint16_t port, uint8_t *msg, uint32_t len);
  int SendMsgWithHeader(const char *host, uint16_t port,
                        uint8_t *header, const uint32_t header_len,
                        uint8_t* body, const uint32_t body_len);

  int InitChannel(std::shared_ptr<RdmaSocket> socket, bool is_accept);

  inline CompletionQueue *get_completion_cq() const { return cq_;}
  inline QueuePair *get_queue_pair() const { return qp_; }
  inline std::string get_ip() const { return ip_; }

private:
  static std::map<std::string, RdmaChannel *> Ip2Channel;
  static boost::shared_mutex Ip2ChannelLock;

  //static std::map<int, RdmaChannel*> Fd2Channel;
  //static boost::shared_mutex Fd2ChannelLock;

  std::string ip_;
  uint16_t port_;

  CompletionQueue *cq_;
  QueuePair *qp_;
  RdmaEvent *event_;
  int is_ready_;

  std::atomic_uint data_id_;
  std::mutex channel_lock_;
};

} // namespace SparkRdmaNetwork

#endif //SPARKRDMA_RDMA_CHANNEL_H
