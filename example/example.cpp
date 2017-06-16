//
// Created by wyb on 17-6-13.
//
#include <thread>
#include <cstdint>
#include <atomic>
#include <functional>
#include "../src/rdma_channel.h"
#include "../src/rdma_server.h"

using namespace SparkRdmaNetwork;
using namespace std;

RdmaServer *server = nullptr;
uint16_t port = 12347;
char localhost[] = "127.0.0.1";
atomic_int id(1);

void send_thread(RdmaChannel *channel, const char *host, int tid);

void init() {
  server = new RdmaServer();
  server->InitServer(localhost, port);
  sleep(1);
}

void test_client(char *host) {
  std::string ip = RdmaSocket::GetIpByHost(host);
  RDMA_INFO("send to {}", ip);
  RdmaChannel *channel = RdmaChannel::GetChannelByIp(ip);
  if (channel == nullptr) {
    channel = new RdmaChannel(host, port);
    RDMA_INFO("create RdmaChannel success");
    channel->Init(host, port);
    RDMA_INFO("init channel success");
  }
  int num = 5;
  thread ths[num];
  for (int i = 0; i < num; ++i) {
    ths[i] = thread(send_thread, channel, host, i+1);
  }
  for (int j = 0; j < num; ++j) {
    ths[j].join();
  }
}

void send_thread(RdmaChannel *channel, const char *host, int tid) {
  int msg_len, head_len, body_len;
  switch (tid) {
    case 1:
      msg_len = k32B - 9;
      head_len = 10;
      body_len = msg_len - head_len;
      break;
    case 2:
      msg_len = k1KB - 9;
      head_len = 100;
      body_len = msg_len - head_len;
      break;
    case 3:
      msg_len = k1KB*4;
      head_len = 100;
      body_len = msg_len - head_len;
      break;
    case 4:
      msg_len = k1MB*2;
      head_len = 100;
      body_len = msg_len - head_len;
      break;
    case 5:
      msg_len = k32MB*2;
      head_len = 100;
      body_len = msg_len - head_len;
      break;
  }
  char mark_begin[10], mark_end[10], mark_head[10];

  int num = 3;
  int send_id;
  for (int i = 0; i < num; ++i) {
    send_id = atomic_fetch_add(&id, 1);
    sprintf(mark_begin, "m%d:%d ", send_id, i);
    sprintf(mark_head, "h%d:%d ", send_id, i);
    sprintf(mark_end, "nd%d\n", send_id);

    char *msg = (char *) RMALLOC(msg_len);
    strcpy(msg, mark_begin);
    for (int i = strlen(mark_begin); i < msg_len; ++i)
      msg[i] = '\0';
    strcpy(msg + msg_len - strlen(mark_end) - 1, mark_end);
    channel->SendMsg(host, port, (uint8_t *) msg, msg_len);

    send_id = atomic_fetch_add(&id, 1);
    sprintf(mark_head, "h%d:%d ", send_id, i);
    sprintf(mark_end, "nd%d\n", send_id);

    char *head = (char*)RMALLOC(head_len);
    strcpy(head, mark_head);
    char *body = (char *)RMALLOC(body_len);
    memset(body, 0, body_len);
    strcpy(body+body_len-strlen(mark_end)-1, mark_end);
    channel->SendMsgWithHeader(host, port, (uint8_t*)head, head_len, (uint8_t*)body, body_len);
  }
  sleep(1);
}

int main (int argc, char *argv[]) {
  init();
  if (argc > 1) {
    test_client(argv[1]);
  }
  sleep(5);
  server->DestroyServer();
  sleep(1);
  return 0;
}