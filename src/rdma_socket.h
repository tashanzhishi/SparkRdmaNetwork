//
// Created by wyb on 17-5-14.
//

#ifndef SPARKRDMA_RDMA_LINK_H
#define SPARKRDMA_RDMA_LINK_H

#include <string>
#include <boost/thread/shared_mutex.hpp>

#include <cstdint>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <netdb.h>

#include "rdma_protocol.h"
#include "rdma_logger.h"

namespace SparkRdmaNetwork {

const int kIpCharSize = 32;
const uint16_t kDefaultPort = 6789;
const std::string kIsServer("SERVER");
const std::string kLocalIp("localhost_");


class RdmaSocket {
public:
  static std::string GetIpFromHost(const char *host);
  static const std::string& GetLocalIp();

  RdmaSocket(const std::string ip = kIsServer, const uint16_t port = kDefaultPort);
  ~RdmaSocket();

  void Socket();
  // for server
  void Bind();
  void Listen();
  std::shared_ptr<RdmaSocket> Accept();
  // for client
  void Connect();

  std::string get_ip() const { return ip_; }
  int get_socket_fd() const { return socket_fd_; }
  int WriteInfo(RdmaConnectionInfo& info);
  int ReadInfo(RdmaConnectionInfo& info);

  static std::string local_ip_;
  static std::map<std::string, std::string> Host2Ip;
  static boost::shared_mutex Host2IpLock;

private:

  std::string ip_;
  uint16_t port_;
  int socket_fd_;
  struct sockaddr_in addr_;
};

} // namespace SparkRdmaNetwork

#endif //SPARKRDMA_RDMA_LINK_H
