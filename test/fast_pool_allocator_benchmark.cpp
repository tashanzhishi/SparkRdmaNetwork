#include <boost/pool/pool_alloc.hpp>
#include <list>
#include <vector>
#include <array>
#include <random>
#include <iostream>
#include <thread>

#include <unistd.h>
#include <time.h>
#include <stdlib.h>

#define MEMORY_POOL_SIZE (2*32*8)
#define N 10
#define PID (std::this_thread::get_id())
#define COST_TIME(start, end) ((end.tv_sec-start.tv_sec)*1000000+(end.tv_usec-start.tv_usec))

const int thread_num = 8;
static uint8_t UP_MEMOEY = 0;

struct my_allocator {
  typedef std::size_t size_type;
  typedef std::ptrdiff_t difference_type;
  static size_type total_size;

  static char *malloc(const size_type bytes) {
    char *ma = static_cast<char *>((std::malloc)(bytes));
    total_size += bytes;
    std::cout << PID << ": malloc " << (void*)ma << " " << bytes << std::endl;
    return ma;
  }

  static void free(char *const block) {
    std::cout << PID << ": free " << (void*)block << std::endl;
    (std::free)(block);
  }
};
std::size_t my_allocator::total_size = 0;

struct data {
  uint8_t chunk[1024*32];
};
#define pool_size 1024
typedef boost::fast_pool_allocator<data, my_allocator, boost::details::pool::default_mutex, pool_size, 0> fast_allocator;
typedef boost::singleton_pool<boost::fast_pool_allocator_tag, sizeof(data), my_allocator,boost::details::pool::default_mutex, pool_size, 0> singleton_allocator;
typedef std::pair<uint8_t *, std::size_t> ptrAndint;

void thread_func() {
  std::vector<ptrAndint> memv;
  std::thread::id pid = std::this_thread::get_id();
  struct timeval start, end;

  std::vector<int> randid = {0,1,2,3,4,5,6,7,8,9};
  std::random_device rd;
  std::mt19937 g(rd());
  std::shuffle(randid.begin(), randid.end(), g);

  std::size_t len = 32;
  data *x[N];

  gettimeofday(&start, NULL);
  data *y = (data *)malloc(len* sizeof(data));
  gettimeofday(&end, NULL);
  std::cout << PID  << "use malloc :" << COST_TIME(start, end) << "us" << std::endl;


  for (int i = 0; i < 10; ++i) {
    gettimeofday(&start, NULL);
    x[0] = fast_allocator::allocate(len);
    //x[0] = static_cast<uint8_t*>(singleton_allocator::ordered_malloc(len));
    gettimeofday(&end, NULL);
    std::cout << PID << ": allocate " <<  " :" << COST_TIME(start, end) << "us" << std::endl;
    usleep(2);

    gettimeofday(&start, NULL);
    fast_allocator::deallocate(x[0], len);
    gettimeofday(&end, NULL);
    std::cout << PID << ": deallocate " <<  " :" << COST_TIME(start, end) << "us" << std::endl;
    usleep(2);
    //std::cout << singleton_allocator::release_memory() << std::endl;
  }
  free(y);

  /*for (int j = 0; j < 10; ++j) {
    gettimeofday(&start, NULL);
    fast_allocator::deallocate(x[j], len);
    //singleton_allocator::ordered_free(x[0], len);
    gettimeofday(&end, NULL);
    std::cout << PID << ": deallocate " << (void*)x  << " :" << COST_TIME(start, end) << "us" << std::endl;
  }
  std::cout << singleton_allocator::release_memory() << std::endl;*/

  /*gettimeofday(&start, NULL);

  for (int i = 0; i < N/2; i++) {
    int id = randid[i];
    for (std::size_t j = 0; j < times[id]; j++) {
      uint8_t *x = fast_allocator::allocate(level[id]);
      while (x == NULL) {
        std::cout << "NULL" << std::endl;
        usleep(10);
        x = fast_allocator::allocate(level[id]);
      }
      memv.push_back(ptrAndint(x, level[id]));
    }
  }
  std::cout << memv.size() << std::endl;
  for (int i = 0; i < memv.size(); i++) {
    fast_allocator::deallocate(memv[i].first, memv[i].second);
  }
  memv.clear();

  //std::cout << singleton_allocator::release_memory() << std::endl;

  for (int i = N/2; i < N; i++) {
    int id = randid[i];
    for (std::size_t j = 0; j < times[id]; j++) {
      while (my_allocator::total_size > )
      uint8_t *x = fast_allocator::allocate(level[id]);
      while (x == NULL) {
        std::cout << "NULL" << std::endl;
        usleep(10);
        x = fast_allocator::allocate(level[id]);
      }
      memv.push_back(ptrAndint(x, level[id]));
    }
  }
  for (int i = 0; i < memv.size(); i++) {
    fast_allocator::deallocate(memv[i].first, memv[i].second);
  }
  gettimeofday(&end, NULL);
  std::cout << PID << " :" << COST_TIME(start, end) << "us" << std::endl;*/
}


int main() {
  struct timeval start, end;

  int init_size = pool_size;
  data *head = fast_allocator::allocate(init_size);
  fast_allocator::deallocate(head, init_size);

  std::thread *th = new std::thread[thread_num];
  gettimeofday(&start, NULL);
  for (int i=0; i<thread_num; i++)
    th[i] = std::thread(thread_func);
  for (int i=0; i<thread_num; i++)
    th[i].join();

  gettimeofday(&end, NULL);
  std::cout << PID << " total:" << COST_TIME(start, end) << "us" << std::endl;

  singleton_allocator::purge_memory();
  std::cout << "success" << std::endl;
}
