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
char head1[7]={"head1\n"}, head2[7]={"head2\n"}, head3[7]={"head3\n"}, head4[7]={"head4\n"}, head7[7]={"head7\n"};
char msg1[32-9]={"msg1\n"}, msg2[1024-9-7]={"msg2\n"};// msg3[1024*4]={"msg3=4KB\n"}, msg4[1024*1024*1]={"msg4=1MB\n"}, msg5[1024*1024*64]={"msg7=64MB\n"};

void init() {
  server = new RdmaServer();
  server->InitServer(localhost, port);
}

void test_send_msg(const char *host) {
  std::string ip = RdmaSocket::GetIpByHost(host);
  RdmaChannel *channel = RdmaChannel::GetChannelByIp(ip);
  channel->Init(host, port);
  channel->SendMsg(host, port, (uint8_t*)msg1, sizeof(msg1));
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