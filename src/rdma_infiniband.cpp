//
// Created by wyb on 17-5-12.
//

#include "rdma_infiniband.h"

#include "rdma_logger.h"
#include "rdma_memory_pool.h"


namespace SparkRdmaNetwork {

RdmaInfiniband::CompletionQueue::CompletionQueue(RdmaInfiniband &infiniband, int min_cqe = kMinCqe) :
    recv_cq_channel_(ibv_create_comp_channel(infiniband.device_.ctx_)) {
  RDMA_TRACE("construct CompletionQueue");

  if (recv_cq_channel_ == nullptr) {
    RDMA_ERROR("ibv_create_comp_channel error: {}", strerror(errno));
    abort();
  }
  send_cq_ = ibv_create_cq(infiniband.device_.ctx_, min_cqe, nullptr, nullptr, 0);
  GPR_ASSERT(send_cq_);
  recv_cq_ = ibv_create_cq(infiniband.device_.ctx_, min_cqe, nullptr, recv_cq_channel_, 0);
  GPR_ASSERT(recv_cq_);
  if (ibv_req_notify_cq(recv_cq_, 0) != 0) {
    RDMA_ERROR("ibv_req_notify_cq error: {}", strerror(errno));
    abort();
  }
}

RdmaInfiniband::CompletionQueue::~CompletionQueue() {
  RDMA_TRACE("destroy CompletionQueue");

  ibv_destroy_comp_channel(recv_cq_channel_);
  ibv_destroy_cq(recv_cq_);
  ibv_destroy_cq(send_cq_);
}

RdmaInfiniband::QueuePair::QueuePair(RdmaInfiniband &infiniband, ibv_qp_type qp_type, int port_num, ibv_cq *send_cq,
                                     ibv_cq *recv_cq, uint32_t max_send_wr, uint32_t max_recv_wr) :
    infiniband_(infiniband), qp_type_(qp_type), ctx_(infiniband.device_.ctx_), pd_(infiniband.pd_.pd_), qp_(nullptr),
    send_cq_(send_cq), recv_cq_(recv_cq), init_psn_(0) {
  RDMA_TRACE("create QueuePair");

  GPR_ASSERT(qp_type == IBV_QPT_RC);
  ibv_qp_init_attr init_attr = {
      .qp_type = qp_type,
      .sq_sig_all = 1, // ??
      .send_cq = send_cq,
      .recv_cq = recv_cq,
      .cap = {
          .max_send_wr = max_send_wr,
          .max_recv_wr = max_recv_wr,
          .max_send_sge = 1,   // ?????
          .max_recv_sge = 1,  // ????
          .max_inline_data = 256,
      },
  };

  qp_ = ibv_create_qp(pd_, &init_attr);
  GPR_ASSERT(qp_);
}

RdmaInfiniband::QueuePair::~QueuePair() {
  RDMA_TRACE("destroy QueuePair");
  ibv_destroy_qp(qp_);
}

uint32_t RdmaInfiniband::QueuePair::get_init_psn() const {
  return init_psn_;
}

uint32_t RdmaInfiniband::QueuePair::get_local_qp_num() const {
  return qp_->qp_num;
}

RdmaInfiniband::RdmaInfiniband() : device_(), pd_(device_) {
  RDMA_TRACE("construct RdmaInfiniband");
  RdmaMemoryPool::GetMemoryPool(pd_.pd_);
}

RdmaInfiniband::~RdmaInfiniband() {
  RDMA_TRACE("destroy RdmaInfiniband");
}

RdmaInfiniband::CompletionQueue* RdmaInfiniband::CreateCompleteionQueue(int min_cqe) {
  return new CompletionQueue(*this);
}

RdmaInfiniband::QueuePair* RdmaInfiniband::CreateQueuePair(ibv_qp_type qp_type, int port_num, ibv_cq *send_cq,
                                                           ibv_cq *recv_cq, uint32_t max_send_wr, uint32_t max_recv_wr) {
  return new QueuePair(*this, qp_type, port_num, send_cq, recv_cq, max_send_wr, max_recv_wr);
}

} // namespace SparkRdmaNetwork