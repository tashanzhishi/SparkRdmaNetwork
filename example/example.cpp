//
// Created by wyb on 17-6-13.
//
#include <thread>
#include <cstdint>
#include "../src/rdma_channel.h"
#include "../src/rdma_server.h"

using namespace SparkRdmaNetwork;
using namespace std;

RdmaServer *server = nullptr;
uint16_t port = 12347;
char localhost[] = "127.0.0.1";

void init() {
  server = new RdmaServer();
  server->InitServer(localhost, port);
  sleep(1);
}

void test_send_msg(const char *host) {
  std::string ip = RdmaSocket::GetIpByHost(host);
  RDMA_INFO("send to {}", ip);
  RdmaChannel *channel = new RdmaChannel(host, port);
  RDMA_INFO("create RdmaChannel success");
  channel->Init(host, port);
  RDMA_INFO("init channel success");
  int msg1_len = 32 - 9;
  char *msg1 = (char*)RMALLOC(msg1_len);
  strcpy(msg1, "msg1\n");
  channel->SendMsg(host, port, (uint8_t*)msg1, msg1_len);
  sleep(1);
  channel->DestroyAllChannel();
}

int main (int argc, char *argv[]) {
  init();
  if (argc > 1) {
    test_send_msg(argv[1]);
  }
  sleep(5);
  server->DestroyServer();
  return 0;
}