//
// Created by wyb on 17-5-14.
//

#include "rdma_socket.h"


#include "rdma_logger.h"


namespace SparkRdmaNetwork {

std::string RdmaSocket::local_ip_ = "";

std::string RdmaSocket::GetIpFromHost(const char *host) {
  if (host == nullptr)
    return "";
  struct hostent *he = gethostbyname(host);
  if (he == NULL) {
    RDMA_ERROR("gethostbyname: {} failed", host);
    abort();
  }
  char ip_str[kIpCharSize] = {'\0'};
  inet_ntop(he->h_addrtype, he->h_addr, ip_str, kIpCharSize);
  std::string ip(ip_str);
  return ip;
}

const std::string& RdmaSocket::get_local_ip() const {
  if (local_ip_ == "") {
    char host_name[kIpCharSize] = {'\0'};
    if (gethostname(host_name, sizeof(host_name)) != 0) {
      RDMA_ERROR("gethostname error: {}", strerror(errno));
      abort();
    }
    local_ip_ = GetIpFromHost(host_name);
  }
  return local_ip_;
};


RdmaSocket::RdmaSocket(const char *host, const uint16_t port) {
  port_ = port;
  ip_ = GetIpFromHost(host);

  memset(&addr_, 0, sizeof(addr_));
  addr_.sin_family = AF_INET;
  addr_.sin_port = htons(port);
  if (host != nullptr) { // client
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
  RDMA_TRACE("socket success");

  // server
  if (ip_ == "") {
    int reuse = 1;
    if (setsockopt(socket_fd_, SOL_SOCKET, SO_REUSEADDR, (char *)&reuse, sizeof(int)) < 0) {
      RDMA_ERROR("setsockopt reuse error: {}", strerror(errno));
      abort();
    }
    RDMA_TRACE("setsockopt reuse success");
  }
}

void RdmaSocket::Bind() {
  if (bind(socket_fd_, static_cast<struct sockaddr*>(&addr_), sizeof(addr_)) != 0) {
    RDMA_ERROR("bind error: {}", strerror(errno));
    abort();
  }
  RDMA_TRACE("bind success");
}

void RdmaSocket::Listen() {
  if (listen(socket_fd_, 1024) != 0) {
    RDMA_ERROR("listen error: {}", strerror(errno));
    abort();
  }
  RDMA_TRACE("listen success");
}

RdmaSocket* RdmaSocket::Accept() {

}

void RdmaSocket::Connect() {
  if (connect(socket_fd_, static_cast<struct sockaddr*>(&addr_), sizeof(addr_)) != 0) {
    RDMA_ERROR("connect {} error: {}", ip_,strerror(errno));
    abort();
  }
  RDMA_TRACE("connect {} success", ip_);
}

int RdmaSocket::WriteInfo(RdmaConnectionInfo& info) {
  RdmaConnectionInfo tmp = {
      .lid = htons(info.lid),
      .psn = htonl(info.psn),
      .qpn = htonl(info.qpn),
  };
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
  info.qpn = ntohl(tmp.qpn);
  return 0;
}

} // namespace SparkRdmaNetwork