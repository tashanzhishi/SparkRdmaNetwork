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

namespace SparkRdmaNetwork {

class RdmaServer {
public:
  int InitServer(const char *host = nullptr, uint16_t port = kDefaultPort);
  void DestroyServer();

  std::thread AcceptThread;
  void Accept(const char *host, uint16_t port);

private:
  int InitServerSocket(const char *host, uint16_t port);

  RdmaSocket *server_;
};

} // namespace SparkRdmaNetwork

#endif //SPARKRDMA_RDMA_SERVER_H
