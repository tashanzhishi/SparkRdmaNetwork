//
// Created by wyb on 17-5-11.
//

#include "rdma_memory_pool.h"

#include "rdma_log.h"

namespace SparkRdmaNetwork {

void *RdmaMemoryPool::malloc(std::size_t len) {
  RDMA_DEBUG("rdma allocate {} byte", len);

  if (len < k1KB) {
    RDMA_DEBUG("allocate pool_{}, {} chunks", "32B", len2num(len, k32B));
    return FastAllocator32B::allocate(len2num(len, k32B));
  } else if (len < k32KB) {
    RDMA_DEBUG("allocate pool_{}, {} chunks", "1KB", len2num(len, k1KB));
    return FastAllocator1KB::allocate(len2num(len, k1KB));
  } else if (len < k1MB) {
    RDMA_DEBUG("allocate pool_{}, {} chunks", "32KB", len2num(len, k32KB));
    return FastAllocator32KB::allocate(len2num(len, k32KB));
  } else if (len < k32MB) {
    RDMA_DEBUG("allocate pool_{}, {} chunks", "1MB", len2num(len, k1MB));
    return FastAllocator1MB::allocate(len2num(len, k1MB));
  } else if (len < kMaxSize) {
    RDMA_DEBUG("allocate pool_{}, {} chunks", "32MB", len2num(len, k32MB));
    return FastAllocator32MB::allocate(len2num(len, k32MB));
  } else {
    RDMA_ERROR("rdma allocate {}, is so big", len);
    abort();
  }
}

void RdmaMemoryPool::free(void *ptr, std::size_t len) {
  RDMA_DEBUG("rdma deallocate {} byte", len);

  if (len < k1KB) {
    RDMA_DEBUG("deallocate pool_{}, {} chunks", "32B", len2num(len, k32B));
    FastAllocator32B::deallocate((Chunk32B *) ptr, len2num(len, k32B));
  } else if (len < k32KB) {
    RDMA_DEBUG("deallocate pool_{}, {} chunks", "1KB", len2num(len, k1KB));
    FastAllocator1KB::deallocate((Chunk1KB *) ptr, len2num(len, k1KB));
  } else if (len < k1MB) {
    RDMA_DEBUG("deallocate pool_{}, {} chunks", "32KB", len2num(len, k32KB));
    FastAllocator32KB::deallocate((Chunk32KB *) ptr, len2num(len, k32KB));
  } else if (len < k32MB) {
    RDMA_DEBUG("deallocate pool_{}, {} chunks", "1MB", len2num(len, k1MB));
    FastAllocator1MB::deallocate((Chunk1MB *) ptr, len2num(len, k1MB));
  } else if (len < kMaxSize) {
    RDMA_DEBUG("deallocate pool_{}, {} chunks", "32MB", len2num(len, k32MB));
    FastAllocator32MB::deallocate((Chunk32MB *) ptr, len2num(len, k32MB));
  } else {
    RDMA_ERROR("rdma deallocate {}, is so big", len);
    abort();
  }
}

void RdmaMemoryPool::destory() {
  FreePool32B::purge_memory();
  FreePool1KB::purge_memory();
  FreePool32KB::purge_memory();
  FreePool1MB::purge_memory();
  FreePool32MB::purge_memory();
}

} // namespace SparkRdmaNetwork