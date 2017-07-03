//
// Created by wyb on 17-5-25.
//

#include "rdma_server.h"

#include "rdma_logger.h"
#include "rdma_channel.h"
#include "rdma_thread.h"
#include "rdma_memory_pool.h"
#include "rdma_event.h"

namespace SparkRdmaNetwork {

int RdmaServer::InitServer(const char *host, uint16_t port) {
  if (port == 0) {
    port = kDefaultPort;
  }
  RDMA_INFO("init server {}:{}", host, port);

  RdmaInfiniband *infiniband = RdmaInfiniband::GetRdmaInfiniband();
  INIT_MEMORY_POOL();
  InitServerSocket(host, port);
  return 0;
}

void RdmaServer::DestroyServer() {
  RDMA_INFO("destroy server and will free all resource");
  RdmaChannel::DestroyAllChannel();

  int listen_fd = server_->get_socket_fd();
  shutdown(listen_fd, SHUT_RDWR);
  close(listen_fd);
  delete server_;
  server_ = nullptr;

  RdmaMemoryPool::GetMemoryPool()->destory();

  //delete RdmaInfiniband::GetRdmaInfiniband();
}

int RdmaServer::InitServerSocket(const char *host, uint16_t port) {
  std::string ip = RdmaSocket::GetIpByHost(host);
  RdmaSocket::GetLocalIp();

  server_ = new RdmaSocket(kIsServer, port);
  server_->Socket();
  server_->Bind();
  server_->Listen();
  auto accept_func =
      std::bind(&RdmaServer::AcceptThreadFunc, this, std::placeholders::_1, std::placeholders::_2);
  AcceptThread = std::thread(accept_func, host, port);
  return 0;
}

void RdmaServer::AcceptThreadFunc(const char *host, uint16_t port) {
  while (1) {
    std::shared_ptr<RdmaSocket> client = server_->Accept();
    if (client == nullptr)
      return;
    RdmaChannel *channel = RdmaChannel::GetChannelByIp(client->get_ip());
    if (channel == nullptr) {
      channel = new RdmaChannel(client->get_ip().c_str());
    }
    channel->InitChannel(client, true);
  }
}

} // namespace SparkRdmaNetwork