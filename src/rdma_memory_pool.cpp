//
// Created by wyb on 17-5-11.
//

#include "rdma_memory_pool.h"

namespace SparkRdmaNetwork {

static std::atomic_ulong kMemorySize(0);
RdmaMemoryPool* RdmaMemoryPool::memory_pool_ = nullptr;

void* RdmaMemoryPool::malloc(std::size_t len) {
  if (len < k1KB) {
    return FastAllocator32B::allocate(len2num(len, k32B));
  } else if (len < k32KB) {
    return FastAllocator1KB::allocate(len2num(len, k1KB));
  } else if (len < k1MB) {
    return FastAllocator32KB::allocate(len2num(len, k32KB));
  } else if (len < k32MB) {
    return FastAllocator1MB::allocate(len2num(len, k1MB));
  } else if (len < kAllocateSize) {
    /*while (kMemorySize + (uint64_t)len > kMaxMemorySize) {
      usleep(1000);
    }*/
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
  } else if (len < kMaxMemorySize) {
    //kMemorySize -= len;
    FastAllocator32MB::deallocate((Chunk32MB *) ptr, len2num(len, k32MB));
  } else {
    RDMA_ERROR("rdma deallocate {}, is so big", len);
    abort();
  }
}

void RdmaMemoryPool::init() {
  RDMA_INFO("init memory pool");
  uint8_t *buf;
  buf = (uint8_t*)RMALLOC(k32B);  RFREE(buf, k32B);
  buf = (uint8_t*)RMALLOC(k1KB);  RFREE(buf, k1KB);
  buf = (uint8_t*)RMALLOC(k32KB); RFREE(buf, k32KB);
  buf = (uint8_t*)RMALLOC(k1MB);  RFREE(buf, k1MB);
  buf = (uint8_t*)RMALLOC(k32MB); RFREE(buf, k32MB);
}

void RdmaMemoryPool::destory() {
  FreePool32B::purge_memory();
  FreePool1KB::purge_memory();
  FreePool32KB::purge_memory();
  FreePool1MB::purge_memory();
  FreePool32MB::purge_memory();
}

void RdmaMemoryPool::try_release() {
  FreePool32MB::release_memory();
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

void RdmaMemoryPool::HandleRegister(void *addr, std::size_t len) {
  ibv_mr *mr = ibv_reg_mr(pd_, addr, len, kRdmaMemoryFlag);
  GPR_ASSERT(mr);
  {
    WriteLock lock(lock_);
    addr_set_.insert(addr);
    addr2mr_[addr] = std::pair<std::size_t, ibv_mr*>(len, mr);
  }
  RDMA_INFO("register {} bytes success", len);
}

void RdmaMemoryPool::HandleUnregister(void *const addr) {
  if (ibv_dereg_mr(get_mr_from_addr(addr)) != 0) {
    RDMA_ERROR("ibv_dereg_mr error: {}", strerror(errno));
    abort();
  }
  WriteLock lock(lock_);
  if (addr_set_.find(addr) == addr_set_.end()) {
    RDMA_ERROR("free faild, because the addr not exist in addr_set");
    abort();
  }
  addr_set_.erase(addr);
  addr2mr_.erase(addr);
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