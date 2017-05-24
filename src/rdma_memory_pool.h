//
// Created by wyb on 17-5-11.
//

#ifndef SPARKRDMA_RDMA_MEMORY_POOL_H
#define SPARKRDMA_RDMA_MEMORY_POOL_H

#include <stdint.h>
#include <cstddef>

#include <boost/pool/pool_alloc.hpp>
#include <infiniband/verbs.h>
#include <set>
#include <utility>
#include <map>
#include <iostream>

#include "rdma_logger.h"
#include "rdma_protocol.h"

#define RMALLOC(len) (RdmaMemoryPool::GetMemoryPool()->malloc(len))
#define RFREE(ptr, len) (RdmaMemoryPool::GetMemoryPool()->free(ptr, len))
#define GET_MR(ptr) (RdmaMemoryPool::GetMemoryPool()->get_mr_from_addr(ptr))

//#define ibv_pd void
//#define ibv_mr void

namespace SparkRdmaNetwork {

const std::size_t k32B = 32;
const std::size_t kInitSize32B = 32 * 1024;
//const std::size_t kInitSize32B = 32;

const std::size_t k1KB = 1024;
const std::size_t kInitSize1KB = 8 * 1024;

const std::size_t k32KB = 32 * 1024;
const std::size_t kInitSize32KB = 1024;

const std::size_t k1MB = 1024 * 1024;
const std::size_t kInitSize1MB = 32;

const std::size_t k32MB = 32 * 1024 * 1024;
const std::size_t kInitSize32MB = 2;

const std::size_t kMaxSize = 2 * 1024 * 1024 * 1024;

inline std::size_t len2num(std::size_t len, std::size_t size) {
  return (len + size - 1) / size;
}



// this is a singleton instance class
class RdmaMemoryPool {
public:
  static RdmaMemoryPool* GetMemoryPool(ibv_pd *pd);
  static RdmaMemoryPool* GetMemoryPool();

  void *malloc(std::size_t len);
  void free(void *ptr, std::size_t len);
  void destory();
  ibv_mr* get_mr_from_addr(void * const addr) const;
  void* get_head_addr(void * const addr) const;

  // test function
  void print_set();
  void print_map();

private:
  // malloc() register rdma memory, and free() will deregister rdma memory
  friend struct RdmaAllocator;
  struct RdmaAllocator {
    typedef std::size_t size_type;
    typedef std::ptrdiff_t difference_type;
    static size_type total_size;

    static char *malloc(const size_type bytes) {
      RDMA_TRACE("register memory {} bytes", bytes);
      char *addr = static_cast<char *>((std::malloc)(bytes));

      //ibv_mr *mr = (void*)(total_size++);
      ibv_mr *mr = ibv_reg_mr(memory_pool_->pd_, addr, bytes, kRdmaMemoryFlag);
      if (mr == nullptr) {
        RDMA_ERROR("ibv_reg_mr error");
        abort();
      }

      std::cout << "malloc: " << (void*)addr << std::endl;

      // should thread safe
      {
        std::lock_guard<std::mutex> lock(memory_pool_->lock_);
        memory_pool_->addr_set_.insert((void *) addr);
        memory_pool_->addr2mr_[(void *) addr] = std::pair<std::size_t, ibv_mr *>(bytes, mr);
      }

      return addr;
    }

    static void free(char *const block) {
      RDMA_TRACE("deregister memory");

      /*if (ibv_dereg_mr(memory_pool_->get_mr_from_addr(block)) != 0) {
        RDMA_ERROR("ibv_dereg_mr error: {}", strerror(errno));
        abort();
      }*/

      std::cout << "free: " << (void*)block << std::endl;

      {
        std::lock_guard<std::mutex> lock(memory_pool_->lock_);
        if (memory_pool_->addr_set_.find(block) == memory_pool_->addr_set_.end()) {
          RDMA_ERROR("free faild, because the addr not exist in addr_set");
          abort();
        }
        memory_pool_->addr_set_.erase(block);
        memory_pool_->addr2mr_.erase(block);
      }

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

  typedef boost::fast_pool_allocator<Chunk32MB, RdmaAllocator,
      boost::details::pool::default_mutex, kInitSize32MB, 0> FastAllocator32MB;
  typedef boost::singleton_pool<boost::fast_pool_allocator_tag, sizeof(Chunk32MB),
      RdmaAllocator, boost::details::pool::default_mutex, kInitSize32MB, 0> FreePool32MB;


  RdmaMemoryPool(ibv_pd *pd);
  ~RdmaMemoryPool();

  static RdmaMemoryPool *memory_pool_;

  ibv_pd *pd_;
  std::mutex lock_;
  // descending sort of set, can use lower_bound to get the head addr who is the last less than addr
  std::set<void*, std::greater<void*>> addr_set_;
  std::map<void*, std::pair<std::size_t, ibv_mr*>> addr2mr_;

  // no copy and =
  RdmaMemoryPool(RdmaMemoryPool &) = delete;
  RdmaMemoryPool &operator=(RdmaMemoryPool &) = delete;
};

} // namespace SparkRdmaNetwork

#endif //SPARKRDMA_RDMA_MEMORY_POOL_H
