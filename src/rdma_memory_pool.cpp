//
// Created by wyb on 17-5-11.
//

#include "rdma_memory_pool.h"

namespace SparkRdmaNetwork {

RdmaMemoryPool* RdmaMemoryPool::memory_pool_ = nullptr;
std::size_t RdmaMemoryPool::RdmaAllocator::total_size = 0;

RdmaMemoryPool::RdmaMemoryPool(ibv_pd *pd) {
  pd_ = pd;
}

RdmaMemoryPool* RdmaMemoryPool::InitMemoryPool(ibv_pd *pd) {
  if (memory_pool_ == nullptr) {
    memory_pool_ = new RdmaMemoryPool(pd);
  }
  return memory_pool_;
}

void* RdmaMemoryPool::malloc(std::size_t len) {
  if (len < k1KB) {
    return FastAllocator32B::allocate(len2num(len, k32B));
  } else if (len < k32KB) {
    return FastAllocator1KB::allocate(len2num(len, k1KB));
  } else if (len < k1MB) {
    return FastAllocator32KB::allocate(len2num(len, k32KB));
  } else if (len < k32MB) {
    return FastAllocator1MB::allocate(len2num(len, k1MB));
  } else if (len < kMaxSize) {
    return FastAllocator32MB::allocate(len2num(len, k32MB));
  } else {
    RDMA_ERROR("rdma allocate {}, is so big", len);
    abort();
  }
}

void RdmaMemoryPool::free(void *ptr, std::size_t len) {
  if (len < k1KB) {
    FastAllocator32B::deallocate((Chunk32B *) ptr, len2num(len, k32B));
  } else if (len < k32KB) {
    FastAllocator1KB::deallocate((Chunk1KB *) ptr, len2num(len, k1KB));
  } else if (len < k1MB) {
    FastAllocator32KB::deallocate((Chunk32KB *) ptr, len2num(len, k32KB));
  } else if (len < k32MB) {
    FastAllocator1MB::deallocate((Chunk1MB *) ptr, len2num(len, k1MB));
  } else if (len < kMaxSize) {
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

ibv_mr* RdmaMemoryPool::get_mr_from_addr(void *const addr) {
  ReadLock rd_lock(lock_);
  auto head_it = addr_set_.lower_bound(addr);
  if (head_it == addr_set_.end()) {
    RDMA_ERROR("get head addr of addr_set failed");
    std::cout << "addr = " << addr << std::endl;
    for (auto &kv : addr_set_) {
      std::cout << kv << std::endl;
    }
    abort();
  }
  void *head = *head_it;
  if (addr2mr_.find(head) == addr2mr_.end()) {
    RDMA_ERROR("find addr failed, because addr not exist in addr2mr");
    abort();
  }
  return addr2mr_.at(head).second;
}

void RdmaMemoryPool::print_set() {
  std::cout << "address set: \n";
  for (auto &x : addr_set_) {
    std::cout<< x << "\n";
  }
  std::cout << std::endl;
}

void RdmaMemoryPool::print_map() {
  std::cout << "address -> [len, mr]: \n";
  for (auto &kv : addr2mr_) {
    std::cout << kv.first << " -> [" << kv.second.first << ", " << kv.second.second << "]\n";
  }
  std::cout << std::endl;
}

} // namespace SparkRdmaNetwork