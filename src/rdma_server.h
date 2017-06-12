//
// Created by wyb on 17-5-25.
//

#ifndef SPARKRDMA_RDMA_SERVER_H
#define SPARKRDMA_RDMA_SERVER_H

#include <cstdint>

#include <string>
#include <thread>

#include "rdma_infiniband.h"
#include "rdma_socket.h"
#include "rdma_event.h"

namespace SparkRdmaNetwork {

class RdmaServer {
 public:
  RdmaServer() : server_(nullptr) {}
  ~RdmaServer() {};

  int InitServer(const char *host = nullptr, uint16_t port = kDefaultPort);
  void DestroyServer();

  void AcceptThreadFunc(const char *host, uint16_t port);

  std::thread AcceptThread;

 private:
  int InitServerSocket(const char *host, uint16_t port);

  RdmaSocket *server_;

  RdmaServer(RdmaServer &) = delete;
  RdmaServer &operator=(RdmaServer &) = delete;
};

} // namespace SparkRdmaNetwork

#endif //SPARKRDMA_RDMA_SERVER_H
