//
// Created by wyb on 17-5-12.
//

#ifndef SPARKRDMA_RDMA_INFINIBAND_H
#define SPARKRDMA_RDMA_INFINIBAND_H

#include <cstring>
#include <cerrno>
#include <infiniband/verbs.h>

#include "rdma_logger.h"

namespace SparkRdmaNetwork {

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

  class Completion


  class QueuePair {
  public:
    QueuePair(RdmaInfiniband& infiniband,
              ibv_qp_type qp_type,
              int port_num,
              ibv_cq *send_cq,
              ibv_cq *recv_cq,
              uint32_t max_send_wr,
              uint32_t max_recv_wr);
    ~QueuePair();

    uint32_t get_init_psn() const;
    uint32_t get_local_qp_num() const;
  private:
    RdmaInfiniband& infiniband_;
    int qp_type_; // QP type (IBV_QPT_RC, etc.)
    ibv_context* ctx_;
    ibv_pd *pd_;
    ibv_qp *qp_;
    int port_num_;
    ibv_cq *send_cq_;
    ibv_cq *recv_cq_;
    uint32_t init_psn_;
  };

  RdmaInfiniband();
  ~RdmaInfiniband();

  // classs function
  ibv_cq* CreateCompleteionQueue(int min_cqe, int send_or_recv);
  QueuePair* CreateQueuePair(ibv_qp_type qp_type, int port_num, ibv_cq *sxcq, ibv_cq *rxcq, uint32_t max_send_wr, uint32_t max_recv_wr);

private:
  Device device_;
  ProtectionDomain pd_;

};

} // namespace SparkRdmaNetwork

#endif //SPARKRDMA_RDMA_INFINIBAND_H
