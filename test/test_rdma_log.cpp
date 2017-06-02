#include <string>
#include <thread>
#include <pthread.h>

#include "rdma_logger.h"

using namespace SparkRdmaNetwork;

void test1() {
  std::string output = "world";
  char name[] = "wyb";
  int years = 25;

  RDMA_INFO("hello, {}. my name is {}, and now is {}.", output, name, years);
  RDMA_TRACE("hello, {}. my name is {}, and now is {}.", output, name, years);
  RDMA_DEBUG("hello, {}. my name is {}, and now is {}.", output, name, years);
  RDMA_ERROR("hello, {}. my name is {}, and now is {}.", output, name, years);
}

void thread_func(int n) {
  RDMA_TRACE("now thread id is {}, {}", pthread_self(), n);
  RDMA_DEBUG("now thread id is {}, {}", pthread_self(), n);
  RDMA_INFO("now thread id is {}, {}", pthread_self(), n);
  RDMA_ERROR("now thread id is {}, {}", pthread_self(), n);

  usleep(3);

  RDMA_TRACE("now thread id is {}, {}", pthread_self(), n);
  RDMA_DEBUG("now thread id is {}, {}", pthread_self(), n);
  RDMA_INFO("now thread id is {}, {}", pthread_self(), n);
  RDMA_ERROR("now thread id is {}, {}", pthread_self(), n);
}

void test2() {
  std::thread ths[10];
  for (int i = 0; i < 10; ++i) {
    ths[i] = std::thread(thread_func, i);
  }
  for (int i = 0; i < 10; ++i) {
    ths[i].join();
  }
  RDMA_INFO("all thread end");
}

int main() {
  test1();
  //test2();
  return 0;
}