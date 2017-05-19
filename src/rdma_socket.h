//
// Created by wyb on 17-5-14.
//

#ifndef SPARKRDMA_RDMA_LINK_H
#define SPARKRDMA_RDMA_LINK_H

#include <string>

#include <cstdint>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <netdb.h>

#include "rdma_logger.h"

namespace SparkRdmaNetwork {

const int kIpCharSize = 32;
const uint16_t kDefaultPort = 6789;

struct RdmaConnectionInfo {
  uint16_t lid;
  uint32_t qpn;
  uint32_t psn;
};

class RdmaSocket {
public:
  static std::string GetIpFromHost(const char *host);
  static const std::string& get_local_ip() const;

  RdmaSocket(const char *host = nullptr, const uint16_t port = kDefaultPort);
  ~RdmaSocket();

  void Socket();
  // for server
  void Bind();
  void Listen();
  RdmaSocket* Accept();
  // for client
  void Connect();

  std::string get_ip() const { return ip_; }
  int WriteInfo(RdmaConnectionInfo& info);
  int ReadInfo(RdmaConnectionInfo& info);


private:
  static std::string local_ip_;

  std::string ip_;
  uint16_t port_;
  int socket_fd_;
  struct sockaddr_in addr_;
};

} // namespace SparkRdmaNetwork

#endif //SPARKRDMA_RDMA_LINK_H
