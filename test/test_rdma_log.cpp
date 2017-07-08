#include <string>
#include <thread>
#include <pthread.h>
#include <sys/time.h>
#include <iostream>

#include "rdma_logger.h"

using namespace SparkRdmaNetwork;
using namespace std;

void test1() {
  std::string output = "world";
  char name[] = "wyb";
  int years = 25;

  RDMA_INFO("hello, {}. my name is {}, and now is {}.", output, name, years);
  RDMA_TRACE("hello, {}. my name is {}, and now is {}.", output, name, years);
  RDMA_DEBUG("hello, {}. my name is {}, and now is {}.", output, name, years);
  RDMA_ERROR("hello, {}. my name is {}, and now is {}.", output, name, years);
}
inline long cost_time(struct timeval& start, struct timeval& end) {
  return (end.tv_sec - start.tv_sec)*1000000 + end.tv_usec - start.tv_usec;
}
void thread_func(int n) {
  RDMA_TRACE("now thread id is {}, {}", pthread_self(), n);
  RDMA_DEBUG("now thread id is {}, {}", pthread_self(), n);
  RDMA_INFO("now thread id is {}, {}", pthread_self(), n);
  RDMA_ERROR("now thread id is {}, {}", pthread_self(), n);

  usleep(3);
  struct timeval start, end;
  gettimeofday(&start, nullptr);
  RDMA_TRACE("now thread id is {}, {}", pthread_self(), n);
  gettimeofday(&end, nullptr);
  cout << "---- " << cost_time(start, end) << endl;
  RDMA_DEBUG("now thread id is {}, {}", pthread_self(), n);
  RDMA_INFO("now thread id is {}, {}", pthread_self(), n);
  RDMA_ERROR("now thread id is {}, {}", pthread_self(), n);
}

void test2() {
  int num = 50;
  std::thread ths[num];
  for (int i = 0; i < num; ++i) {
    ths[i] = std::thread(thread_func, i);
  }
  for (int i = 0; i < num; ++i) {
    ths[i].join();
  }
  RDMA_INFO("all thread end");
}

int main() {
  //test1();
  test2();
  return 0;
}