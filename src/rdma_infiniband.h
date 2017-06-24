//
// Created by wyb on 17-5-12.
//

#ifndef SPARKRDMA_RDMA_INFINIBAND_H
#define SPARKRDMA_RDMA_INFINIBAND_H

#include <cstring>
#include <cerrno>
#include <cstdint>
#include <infiniband/verbs.h>

#include <memory>

#include "rdma_protocol.h"
#include "rdma_logger.h"

namespace SparkRdmaNetwork {

const int kMinCqe = 1024;
const int kMaxWr = 1024;
const uint8_t kIbPortNum = 1;
const int kSmallPreReceive = 2048; // 1kb
const int kBigPreReceive = 512;    // 1kb

//
class RdmaInfiniband {
public:


  class DeviceList {
  public:
    DeviceList() : devices_(ibv_get_device_list(NULL)) {
      if (devices_ == NULL) {
        RDMA_ERROR("Could not open infiniband device list");
        abort();
      }
    }
    ~DeviceList() {
      ibv_free_device_list(devices_);
    }
    // the i is 0 usually
    ibv_device *get_device(const int i = 0) {
      return devices_[i];
    }
  private:
    ibv_device **const devices_;
    // no copy and =
    DeviceList(DeviceList &) = delete;
    DeviceList &operator=(DeviceList &) = delete;
  };


  class Device {
  public:
    Device() : ctx_(NULL) {
      DeviceList device_list;
      auto dev = device_list.get_device();
      if (dev == NULL) {
        RDMA_ERROR("failed to find infiniband device");
        abort();
      }
      ctx_ = ibv_open_device(dev);
      if (ctx_ == NULL) {
        RDMA_ERROR("failed to open infiniband device");
        abort();
      }
      // wyb test
      ibv_device_attr da;
      ibv_query_device(ctx_, &da);
      RDMA_INFO("max_qp: {}, max_qp_wr: {}, max_sge: {}, max_cq: {}, max_cqe: {}",
                da.max_qp, da.max_qp_wr, da.max_sge, da.max_cq, da.max_cqe);
    }
    ~Device() {
      if (ibv_close_device(ctx_) != 0)
        RDMA_ERROR("ibv_close_device error: {}", strerror(errno));
    }
    ibv_context *ctx_; // const after construction
  private:
    // no copy and =
    Device(Device &) = delete;
    Device &operator=(Device &) = delete;
  };


  class ProtectionDomain {
  public:
    explicit ProtectionDomain(Device& device) : pd_(ibv_alloc_pd(device.ctx_)) {
      if (pd_ == NULL) {
        RDMA_ERROR("allocate infiniband protection domain failed");
        abort();
      }
    }
    ~ProtectionDomain() {
      if (ibv_dealloc_pd(pd_) != 0) {
        RDMA_ERROR("ibv_dealloc_pd failed");
      }
    }
    ibv_pd* const pd_;

  private:
    // no copy and =
    ProtectionDomain(ProtectionDomain&) = delete;
    ProtectionDomain&operator=(ProtectionDomain&) = delete;
  };

  struct BufferDescriptor {
    BufferDescriptor(uint8_t *buffer, uint32_t bytes, ibv_mr *mr, void *channel) :
        buffer_(buffer), bytes_(bytes), mr_(mr), channel_(channel){}
    BufferDescriptor() :
        buffer_(nullptr), bytes_(0), mr_(nullptr), channel_(nullptr){}
    uint8_t *buffer_;
    uint32_t bytes_;
    ibv_mr *mr_;
    void *channel_;
  private:
    // no copy and =
    BufferDescriptor(BufferDescriptor&) = delete;
    BufferDescriptor&operator=(BufferDescriptor&) = delete;
  };

  class CompletionQueue {
  public:
    CompletionQueue(RdmaInfiniband& infiniband, int min_cqe = kMinCqe);
    ~CompletionQueue();
    inline ibv_cq* get_send_cq() { return send_cq_;}
    inline ibv_cq* get_recv_cq() { return recv_cq_;}
    inline ibv_comp_channel* get_recv_cq_channel() { return recv_cq_channel_;}
    inline ibv_comp_channel* get_send_cq_channel() { return send_cq_channel_;}

  private:
    ibv_comp_channel* send_cq_channel_;
    ibv_comp_channel* recv_cq_channel_;
    ibv_cq *send_cq_;
    ibv_cq *recv_cq_;
    // no copy and =
    CompletionQueue(CompletionQueue&) = delete;
    CompletionQueue&operator=(CompletionQueue&) = delete;
  };


  class QueuePair {
  public:
    QueuePair(RdmaInfiniband& infiniband,
              ibv_qp_type qp_type,
              ibv_cq *send_cq,
              ibv_cq *recv_cq,
              uint32_t max_send_wr,
              uint32_t max_recv_wr);
    ~QueuePair();

    inline uint32_t get_init_psn() const { return init_psn_; };
    inline uint32_t get_local_qp_num(bool is_small) const { return is_small ? small_qp_->qp_num : big_qp_->qp_num; };
    inline uint16_t get_local_lid() const { return lid_; };

    int ModifyQpToInit();
    int ModifyQpToRTS();
    int ModifyQpToRTR(RdmaConnectionInfo& info);

    void PreReceive(void *channel, int small = kSmallPreReceive, int big = kBigPreReceive);
    int PostReceiveWithNum(void *channel, bool is_small, int num);
    int PostReceiveOneWithBuffer(BufferDescriptor *buf, bool is_small);

    int PostSend(BufferDescriptor *buf, int num, bool is_small);
    int PostSendAndWait(BufferDescriptor *buf, int num, bool is_small);
    int PostWrite(BufferDescriptor *buf, int num, uint64_t addr, uint32_t rkey);
    int PostWriteAndWait(BufferDescriptor *buf, int num, uint64_t addr, uint32_t rkey);
    int PostRead(BufferDescriptor *buf, int num, uint64_t addr, uint32_t rkey);
    static const char* WcStatusToString(int status);
  private:
    RdmaInfiniband& infiniband_;
    int qp_type_; // QP type (IBV_QPT_RC, etc.)
    ibv_context* ctx_;
    ibv_pd *pd_;
    uint16_t lid_;

    ibv_qp *small_qp_;
    ibv_qp *big_qp_;

    ibv_cq *send_cq_;
    std::mutex send_lock;
    ibv_cq *recv_cq_;
    uint32_t init_psn_;
  };


  static RdmaInfiniband* GetRdmaInfiniband() {
    if (infiniband_ == nullptr) {
      std::lock_guard<std::mutex> lock(lock_);
      if (infiniband_ == nullptr)
        infiniband_ = new RdmaInfiniband();
    }
    return infiniband_;
  };

  // classs function
  CompletionQueue* CreateCompleteionQueue(int min_cqe = kMinCqe);
  QueuePair* CreateQueuePair(ibv_cq *send_cq, ibv_cq *recv_cq,
                             ibv_qp_type qp_type = IBV_QPT_RC,
                             uint32_t max_send_wr = kMaxWr, uint32_t max_recv_wr = kMaxWr);
  QueuePair* CreateQueuePair(CompletionQueue* cq,
                             ibv_qp_type qp_type = IBV_QPT_RC,
                             uint32_t max_send_wr = kMaxWr, uint32_t max_recv_wr = kMaxWr);

private:
  static RdmaInfiniband *infiniband_;
  static std::mutex lock_;

  RdmaInfiniband();
  ~RdmaInfiniband();

  Device device_;
  ProtectionDomain pd_;
};


typedef RdmaInfiniband::QueuePair QueuePair;
typedef RdmaInfiniband::CompletionQueue CompletionQueue;
typedef RdmaInfiniband::BufferDescriptor BufferDescriptor;

} // namespace SparkRdmaNetwork

#endif //SPARKRDMA_RDMA_INFINIBAND_H
