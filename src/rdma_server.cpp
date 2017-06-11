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
  RDMA_INFO("init server");

  RdmaInfiniband *infiniband = RdmaInfiniband::GetRdmaInfiniband();
  InitServerSocket(host, port);
  return 0;
}

void RdmaServer::DestroyServer() {
  RDMA_INFO("destroy server and will free all resource");

  RdmaMemoryPool::GetMemoryPool()->destory();

  int listen_fd = server_->get_socket_fd();
  shutdown(listen_fd, SHUT_RDWR);
  close(listen_fd);
  delete server_;

  for (auto &kv : RdmaChannel::Ip2Channel) {
    RdmaChannel *channel = kv.second;
    delete channel;
  }

  delete RdmaInfiniband::GetRdmaInfiniband();
}

int RdmaServer::InitServerSocket(const char *host, uint16_t port) {
  std::string ip = RdmaSocket::GetIpFromHost(host);
  RdmaSocket::GetLocalIp();

  server_ = new RdmaSocket(kIsServer, port);
  server_->Socket();
  server_->Bind();
  server_->Listen();
  AcceptThread = std::thread(AcceptThreadFunc, host, port);
  return 0;
}

void RdmaServer::AcceptThreadFunc(const char *host, uint16_t port) {
  while (1) {
    std::shared_ptr<RdmaSocket> client = server_->Accept();
    if (client == nullptr)
      return;
    RdmaChannel *channel = RdmaChannel::GetChannelByIp(client->get_ip());
    channel->InitChannel(client, true);
  }
}

} // namespace SparkRdmaNetwork