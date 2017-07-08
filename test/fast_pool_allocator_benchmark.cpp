#include <boost/pool/pool_alloc.hpp>
#include <list>
#include <vector>
#include <array>
#include <random>
#include <iostream>
#include <thread>
#include <atomic>

#include <unistd.h>
#include <time.h>
#include <stdlib.h>

#define N 10
#define PID (std::this_thread::get_id())
#define COST_TIME(start, end) ((end.tv_sec-start.tv_sec)*1000000+(end.tv_usec-start.tv_usec))
struct data {
  uint8_t chunk[100];
};
const int thread_num = 1;
const std::size_t max_memory = 5000;
data xxx;

struct my_allocator {
  typedef std::size_t size_type;
  typedef std::ptrdiff_t difference_type;
  static size_type total_size;

  static char *malloc(const size_type bytes) {
    total_size += bytes;
    char *ma = static_cast<char *>((std::malloc)(bytes));
    std::cout << PID << ": malloc " << (void*)ma << " " << bytes << std::endl;
    return ma;
  }

  static void free(char *const block) {
    if (block == (char*)&xxx)
      return;
    std::cout << PID << ": free " << (void*)block << std::endl;
    (std::free)(block);
  }
};
std::size_t my_allocator::total_size = 0;


#define pool_size 10
typedef boost::fast_pool_allocator<data, my_allocator,
  boost::details::pool::default_mutex, pool_size, 1> fast_allocator;
typedef boost::singleton_pool<boost::fast_pool_allocator_tag, sizeof(data), my_allocator,
  boost::details::pool::default_mutex, pool_size, 1> singleton_allocator;

void thread_func() {
  std::size_t len = 10;
  data *x[10];

  for (int i = 0; i < 5; ++i) {
    x[i] = fast_allocator::allocate(len);
  }
  x[1] = fast_allocator::allocate(len);


  //fast_allocator::deallocate(x[0], len);
  std::cout << "reease memory {" << std::endl;
  bool y = singleton_allocator::release_memory();
  if(y)
    std::cout<<"hehe"<<std::endl;
  std::cout << "}" << std::endl;
}


int main() {
  std::thread *th = new std::thread[thread_num];
  for (int i=0; i<thread_num; i++)
    th[i] = std::thread(thread_func);
  for (int i=0; i<thread_num; i++)
    th[i].join();
  //singleton_allocator::purge_memory();
  std::cout << "success" << std::endl;
}
