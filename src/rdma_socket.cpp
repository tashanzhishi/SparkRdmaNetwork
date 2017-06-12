//
// Created by wyb on 17-5-14.
//

#include "rdma_socket.h"

#include "rdma_logger.h"
#include "rdma_thread.h"


namespace SparkRdmaNetwork {

std::string RdmaSocket::local_ip_ = kLocalIp;

std::string RdmaSocket::GetIpByHost(const char *host) {
  {
    ReadLock rd_lock(Host2IpLock);
    if (Host2Ip.find(host) != Host2Ip.end())
      return Host2Ip.at(host);
  }
  WriteLock wr_lock(Host2IpLock);
  if (Host2Ip.find(host) != Host2Ip.end())
    return Host2Ip.at(host);
  struct hostent *he = gethostbyname(host);
  if (he == NULL) {
    RDMA_ERROR("gethostbyname {} failed", host);
    abort();
  }
  char ip_str[kIpCharSize] = {'\0'};
  inet_ntop(he->h_addrtype, he->h_addr, ip_str, kIpCharSize);
  std::string ip(ip_str);
  Host2Ip[host] = ip;
  return ip;
}

const std::string& RdmaSocket::GetLocalIp() {
  if (local_ip_ == kLocalIp) {
    char host_name[kIpCharSize] = {'\0'};
    if (gethostname(host_name, sizeof(host_name)) != 0) {
      RDMA_ERROR("gethostname error: {}", strerror(errno));
      abort();
    }
    local_ip_ = GetIpByHost(host_name);
  }
  return local_ip_;
};


RdmaSocket::RdmaSocket(const std::string ip, const uint16_t port) {
  port_ = port;
  ip_ = ip;

  memset(&addr_, 0, sizeof(addr_));
  addr_.sin_family = AF_INET;
  addr_.sin_port = htons(port);
  if (ip != kIsServer) { // client
    addr_.sin_addr.s_addr = inet_addr(ip_.c_str());
  } else { // server
    addr_.sin_addr.s_addr = htonl(INADDR_ANY);
  }

  socket_fd_ = 0;
}

RdmaSocket::~RdmaSocket() {
  close(socket_fd_);
}

void RdmaSocket::Socket() {
  socket_fd_ = socket(AF_INET, SOCK_STREAM, 0);
  if (socket_fd_ < 0) {
    RDMA_ERROR("{} socket error: {}", ip_, strerror(errno));
    abort();
  }
  RDMA_DEBUG("socket success");

  // server
  if (ip_ == kIsServer) {
    int reuse = 1;
    if (setsockopt(socket_fd_, SOL_SOCKET, SO_REUSEADDR, (char *)&reuse, sizeof(int)) < 0) {
      RDMA_ERROR("setsockopt reuse error: {}", strerror(errno));
      abort();
    }
    RDMA_DEBUG("setsockopt reuse success");
  }
}

void RdmaSocket::Bind() {
  if (bind(socket_fd_, (struct sockaddr*)(&addr_), sizeof(addr_)) != 0) {
    RDMA_ERROR("bind error: {}", strerror(errno));
    abort();
  }
  RDMA_DEBUG("bind success");
}

void RdmaSocket::Listen() {
  if (listen(socket_fd_, 1024) != 0) {
    RDMA_ERROR("listen error: {}", strerror(errno));
    abort();
  }
  RDMA_DEBUG("listen success");
}

std::shared_ptr<RdmaSocket> RdmaSocket::Accept() {
  struct sockaddr_in addr, client_addr;
  socklen_t socklen = sizeof(addr);
  int fd = accept(socket_fd_, (struct sockaddr *)&addr, &socklen);
  if (fd == -1) {
    RDMA_ERROR("accept error, {}", strerror(errno));
    return nullptr;
  }

  char remote_ip[kIpCharSize] = {'\0'};
  memcpy(&client_addr, &addr, sizeof(addr));
  strcpy(remote_ip, inet_ntoa(client_addr.sin_addr));
  RDMA_DEBUG("accept %s, fd=%d", remote_ip, fd);

  std::shared_ptr<RdmaSocket> client(new RdmaSocket(remote_ip));
  client->socket_fd_ = fd;
  memcpy(&client->addr_, &client_addr, sizeof(client_addr));
  client->ip_ = std::string(remote_ip);
  return client;
}

void RdmaSocket::Connect() {
  if (connect(socket_fd_, (struct sockaddr*)(&addr_), sizeof(addr_)) != 0) {
    RDMA_ERROR("connect {} error: {}", ip_,strerror(errno));
    abort();
  }
  RDMA_TRACE("connect {} success", ip_);
}

int RdmaSocket::WriteInfo(RdmaConnectionInfo& info) {
  RdmaConnectionInfo tmp;
  tmp.lid = htons(info.lid);
  tmp.psn = htonl(info.psn);
  tmp.small_qpn = htonl(info.small_qpn);
  tmp.big_qpn = htonl(info.big_qpn);

  if (write(socket_fd_, &tmp, sizeof(tmp)) < 0) {
    RDMA_ERROR("write infomation to {} failed", ip_);
    return -1;
  }
  return 0;
}

int RdmaSocket::ReadInfo(RdmaConnectionInfo& info) {
  RdmaConnectionInfo tmp;
  if (read(socket_fd_, &tmp, sizeof(tmp)) < 0) {
    RDMA_ERROR("read information from {} failed", ip_);
    return -1;
  }
  info.lid = ntohs(tmp.lid);
  info.psn = ntohl(tmp.psn);
  info.small_qpn = ntohl(tmp.small_qpn);
  info.big_qpn = ntohl(tmp.big_qpn);
  return 0;
}

} // namespace SparkRdmaNetwork