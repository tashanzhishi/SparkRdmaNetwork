//
// Created by wyb on 17-5-11.
//

#ifndef SPARKRDMA_RDMA_MEMORY_POOL_H
#define SPARKRDMA_RDMA_MEMORY_POOL_H

#include <stdint.h>
#include <cstddef>

#include <boost/pool/pool_alloc.hpp>

#include "rdma_log.h"

namespace SparkRdmaNetwork {

const std::size_t k32B = 32;
const std::size_t kInitSize32B = 32 * 1024;

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
  static void *malloc(std::size_t len);

  static void free(void *ptr, std::size_t len);

  static void destory();

private:

  // malloc() register rdma memory, and free() will deregister rdma memory
  struct RdmaAllocator {
    typedef std::size_t size_type;
    typedef std::ptrdiff_t difference_type;
    static size_type total_size;

    static char *malloc(const size_type bytes) {
      // test
      RDMA_DEBUG("register memory {} bytes", bytes);
      // test

      char *ma = static_cast<char *>((std::malloc)(bytes));
      return ma;
    }

    static void free(char *const block) {
      // test
      RDMA_DEBUG("deregister memory");
      // test
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

  // no copy and =
  RdmaMemoryPool(RdmaMemoryPool &) = delete;

  RdmaMemoryPool &operator=(RdmaMemoryPool &) = delete;
};

} // namespace SparkRdmaNetwork

#endif //SPARKRDMA_RDMA_MEMORY_POOL_H
