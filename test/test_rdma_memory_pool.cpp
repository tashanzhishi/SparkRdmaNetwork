#include <iostream>

#include "rdma_memory_pool.h"

using namespace SparkRdmaNetwork;
using namespace std;

void test1() {
  //const int len[] = {21, k32B*12-23, k1KB*9-567, k32KB*7-1024*12, k1MB*21-102400};
  const int len[] = {21, 200, kInitSize32B*32-1, 300, kInitSize32B*32-1, kInitSize32B*31, 1};
  const int num = sizeof(len)/ sizeof(int);
  uint8_t *x[num];

  RdmaMemoryPool *pool = RdmaMemoryPool::GetMemoryPool(nullptr);

  for (int i = 0; i < num; ++i) {
    x[i] = (uint8_t *)pool->malloc(len[i]);
    cout << (void*)x[i] << endl;
  }

  pool->print_set();
  pool->print_map();

  for (int i = 0; i < num; ++i) {
    cout << pool->get_head_addr(x[i]) << " " << pool->get_mr_from_addr(x[i]) << endl;
  }

  for (int i = 0; i < num; ++i) {
    pool->free(x[i], len[i]);
  }

  pool->print_set();
  pool->print_map();

  pool->destory();

  pool->print_set();
  pool->print_map();
}

/*void thread_func(int n) {
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
}*/

int main() {
  test1();
  //test2();
  return 0;
}