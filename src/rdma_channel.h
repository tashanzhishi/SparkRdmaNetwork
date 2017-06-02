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

namespace SparkRdmaNetwork {

typedef RdmaInfiniband::QueuePair QueuePair;
typedef RdmaInfiniband::CompletionQueue CompletionQueue;
typedef RdmaInfiniband::BufferDescriptor BufferDescriptor;

class RdmaChannel {
public:
  static const std::string get_ip_from_host(const std::string& host);
  static RdmaChannel *get_channel_from_ip(const std::string& ip) const;

  static std::map<std::string, std::string> Host2Ip;
  static boost::shared_mutex Host2IpLock;
  static std::map<std::string, RdmaChannel *> Ip2Channel;
  static boost::shared_mutex Ip2ChannelLock;
  static std::map<int, RdmaChannel*> Fd2Channel;
  static boost::shared_mutex Fd2ChannelLock;

  RdmaChannel(const char *host = nullptr, uint16_t port = kDefaultPort);
  ~RdmaChannel();

  // connect to server
  int Init(const char *host, uint16_t port);
  // the msg, header, body have been registered
  // the host and port is not used
  int SendMsg(const char *host, uint16_t port, uint8_t *msg, uint32_t len);
  int SendMsgWithHeader(const char *host, uint16_t port,
                        uint8_t *header, const uint32_t header_len,
                        uint8_t* body, const uint32_t body_len);

  CompletionQueue *get_completion_cq() const { return cq_;}

  int InitChannel(std::shared_ptr<RdmaSocket> socket);


  boost::lockfree::queue<BufferDescriptor*> small_data_, big_data_, req_rpc_, ack_rpc_;
  std::atomic_bool small_data_running_, big_data_running_, req_rpc_running_, ack_rpc_running_;

private:
  std::string ip_;
  uint16_t port_;

  CompletionQueue *cq_;
  QueuePair *qp_;
  int is_ready;

  std::atomic_uint data_id_;
  std::map<uint32_t, std::pair<uint8_t*,uint8_t*> > id2data_;
  std::mutex id2data_lock_;
  std::mutex channel_lock_;
};

} // namespace SparkRdmaNetwork

#endif //SPARKRDMA_RDMA_CHANNEL_H
