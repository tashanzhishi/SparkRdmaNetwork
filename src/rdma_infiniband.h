//
// Created by wyb on 17-5-12.
//

#ifndef SPARKRDMA_RDMA_INFINIBAND_H
#define SPARKRDMA_RDMA_INFINIBAND_H

#include <cstring>
#include <cerrno>
#include <infiniband/verbs.h>

#include "rdma_log.h"

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
    ProtectionDomain(ProtectionDomain&) = delete;
    ProtectionDomain&operator=(ProtectionDomain&) = delete;
  };

};

} // namespace SparkRdmaNetwork

#endif //SPARKRDMA_RDMA_INFINIBAND_H
