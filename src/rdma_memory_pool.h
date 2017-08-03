//
// Created by wyb on 17-5-11.
//

#ifndef SPARKRDMA_RDMA_MEMORY_POOL_H
#define SPARKRDMA_RDMA_MEMORY_POOL_H

#include <stdint.h>
#include <cstddef>

#include <boost/pool/pool_alloc.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <infiniband/verbs.h>
#include <set>
#include <utility>
#include <unordered_map>
#include <iostream>

#include "rdma_logger.h"
#include "rdma_protocol.h"
#include "rdma_thread.h"

#define RMALLOC(len) (RdmaMemoryPool::GetMemoryPool()->malloc(len))
#define RFREE(ptr, len) (RdmaMemoryPool::GetMemoryPool()->free(ptr, len))
#define GET_MR(ptr) (RdmaMemoryPool::GetMemoryPool()->get_mr_from_addr(ptr))



namespace SparkRdmaNetwork {

const std::size_t k32B = 32;
const std::size_t kInitSize32B = 64 * 1024;

const std::size_t k1KB = 1024;
const std::size_t kInitSize1KB = 64 * 1024;

const std::size_t k32KB = 32 * 1024;
const std::size_t kInitSize32KB = 1024;

const std::size_t k1MB = 1024 * 1024;
const std::size_t kInitSize1MB = 128;

const std::size_t k32MB = 32 * 1024 * 1024;
const std::size_t kInitSize32MB = 128;

// 单次allocate
const std::size_t kAllocateSize = (std::size_t)2*1024*1024*1024;
// 总内存阈值
const std::size_t kMaxMemorySize = (std::size_t)16 * 1024 * 1024 * 1024;

inline std::size_t len2num(std::size_t len, std::size_t size) {
  return (len + size - 1) / size;
}



// this is a singleton instance class
class RdmaMemoryPool {
public:
  static RdmaMemoryPool* GetMemoryPool(ibv_pd *pd) {
    if (memory_pool_ == NULL)
      memory_pool_ = new RdmaMemoryPool(pd);
    return memory_pool_;
  };
  static inline RdmaMemoryPool* GetMemoryPool() {
    return memory_pool_;
  }

  void *malloc(std::size_t len);
  void free(void *ptr, std::size_t len);

  void init();
  void try_release();
  void destory();
  ibv_mr* get_mr_from_addr(void * const addr);

  // test function
  void print_set();
  void print_map();

private:
  // malloc() register rdma memory, and free() will deregister rdma memory
  friend struct RdmaAllocator;
  struct RdmaAllocator {
    typedef std::size_t size_type;
    typedef std::ptrdiff_t difference_type;

    static char *malloc(const size_type bytes) {
      RDMA_INFO("register memory {} bytes", bytes);
      RdmaMemoryPool *pool = GetMemoryPool();
      char *addr = static_cast<char *>((std::malloc)(bytes));
      pool->HandleRegister(addr, bytes);
      return addr;
    }

    static void free(char *const block) {
      RDMA_INFO("deregister memory");
      RdmaMemoryPool *pool = GetMemoryPool();
      pool->HandleUnregister(block);
      (std::free)(block);
    }
  };

  // the level of chunk of block of memory pool
  // 32 1kb 32kb 1mb 32mb
  struct Chunk32B  { uint8_t chunk[k32B];  };
  struct Chunk1KB  { uint8_t chunk[k1KB];  };
  struct Chunk32KB { uint8_t chunk[k32KB]; };
  struct Chunk1MB  { uint8_t chunk[k1MB];  };
  struct Chunk32MB { uint8_t chunk[k32MB]; };

  typedef boost::fast_pool_allocator<Chunk32B, RdmaAllocator,
      boost::details::pool::default_mutex, kInitSize32B, 0> FastAllocator32B;
  typedef boost::singleton_pool<boost::fast_pool_allocator_tag, sizeof(Chunk32B),
      RdmaAllocator, boost::details::pool::default_mutex, kInitSize32B, 0> FreePool32B;

  typedef boost::fast_pool_allocator<Chunk1KB, RdmaAllocator,
      boost::details::pool::default_mutex, kInitSize1KB, 0> FastAllocator1KB;
  typedef boost::singleton_pool<boost::fast_pool_allocator_tag, sizeof(Chunk1KB),
      RdmaAllocator, boost::details::pool::default_mutex, kInitSize1KB, 0> FreePool1KB;

  typedef boost::fast_pool_allocator<Chunk32KB, RdmaAllocator,
      boost::details::pool::default_mutex, kInitSize32KB, 0> FastAllocator32KB;
  typedef boost::singleton_pool<boost::fast_pool_allocator_tag, sizeof(Chunk32KB),
      RdmaAllocator, boost::details::pool::default_mutex, kInitSize32KB, 0> FreePool32KB;

  typedef boost::fast_pool_allocator<Chunk1MB, RdmaAllocator,
      boost::details::pool::default_mutex, kInitSize1MB, 0> FastAllocator1MB;
  typedef boost::singleton_pool<boost::fast_pool_allocator_tag, sizeof(Chunk1MB),
      RdmaAllocator, boost::details::pool::default_mutex, kInitSize1MB, 0> FreePool1MB;

  // 每次按Chunk32MB*kInitSize32MB扩容
  typedef boost::fast_pool_allocator<Chunk32MB, RdmaAllocator,
      boost::details::pool::default_mutex, kInitSize32MB, 1> FastAllocator32MB;
  typedef boost::singleton_pool<boost::fast_pool_allocator_tag, sizeof(Chunk32MB),
      RdmaAllocator, boost::details::pool::default_mutex, kInitSize32MB, 1> FreePool32MB;


  RdmaMemoryPool(ibv_pd *pd) : pd_(pd) {
    if (pd == nullptr)
      RDMA_ERROR("pd must be not NULL");
  };
  ~RdmaMemoryPool() {
    FreePool32B::purge_memory();
    FreePool1KB::purge_memory();
    FreePool32KB::purge_memory();
    FreePool1MB::purge_memory();
    FreePool32MB::purge_memory();
  };
  void HandleRegister(void *addr, std::size_t len);
  void HandleUnregister(void *const addr);

  static RdmaMemoryPool* memory_pool_;

  ibv_pd *pd_;
  boost::shared_mutex lock_;
  // descending sort of set, can use lower_bound to get the head addr who is the last less than addr
  std::set<void*, std::greater<void*>> addr_set_;
  std::unordered_map<void*, std::pair<std::size_t, ibv_mr*>> addr2mr_;

  // no copy and =
  RdmaMemoryPool(RdmaMemoryPool &) = delete;
  RdmaMemoryPool &operator=(RdmaMemoryPool &) = delete;
};

} // namespace SparkRdmaNetwork

#endif //SPARKRDMA_RDMA_MEMORY_POOL_H
