#include "rdma_memory_pool.h"

using namespace SparkRdmaNetwork;

void test1() {
  const std::size_t len = 96;
  uint8_t *x = (uint8_t *)RdmaMemoryPool::malloc(len);
  RdmaMemoryPool::free(x, len);
}

void thread_func(int n) {
  std::size_t len;
  if (n == 0) {
    len = k32B*5+1;
  } else if (n == 1) {
    len = k1KB*3-1;
  } else if (n == 2) {
    len = k32KB*5+12;
  } else if (n == 3) {
    len = k1MB*6+1213;
  } else if (n == 4) {
    len = k32MB*1;
  }

  const int num = 10;
  uint8_t *x[num];
  for (int i = 0; i < num; ++i) {
    x[i] = (uint8_t *)RdmaMemoryPool::malloc(len);
  }

  usleep(6-n);

  for (int i = 0; i < num; ++i) {
    RdmaMemoryPool::free(x[i], len);
  }
}

void test2() {
  std::thread ths[5];
  for (int i = 0; i < 5; ++i) {
    ths[i] = std::thread(thread_func, i);
  }
  for (int i = 0; i < 5; ++i) {
    ths[i].join();
  }
  RdmaMemoryPool::destory();
  RDMA_INFO("all thread end");
}

int main() {
  test2();
  return 0;
}