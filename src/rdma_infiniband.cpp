//
// Created by wyb on 17-5-12.
//

#include "rdma_infiniband.h"

#include "rdma_logger.h"


namespace SparkRdmaNetwork {

RdmaInfiniband::QueuePair::QueuePair(RdmaInfiniband &infiniband, ibv_qp_type qp_type, int port_num, ibv_cq *send_cq,
                                     ibv_cq *recv_cq, uint32_t max_send_wr, uint32_t max_recv_wr) :
    infiniband_(infiniband), qp_type_(qp_type), ctx_(infiniband.device_.ctx_), pd_(infiniband.pd_.pd_), qp_(nullptr),
    send_cq_(send_cq), recv_cq_(recv_cq), init_psn_(0) {
  RDMA_DEBUG("create qp");
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

uint32_t RdmaInfiniband::QueuePair::get_init_psn() const {
  return init_psn_;
}

uint32_t RdmaInfiniband::QueuePair::get_local_qp_num() const {
  return qp_->qp_num;
}

RdmaInfiniband::RdmaInfiniband() : device_(), pd_(device_) {

}

RdmaInfiniband::~RdmaInfiniband() {

}

ibv_cq* RdmaInfiniband::CreateCompleteionQueue(int min_cqe, int send_or_recv) {
  if (send_or_recv == 1) { // recv
    id_->recv_cq_channel = ibv_create_comp_channel(id_->verbs);
    GPR_ASSERT(id_->recv_cq_channel);
    id_->recv_cq = ibv_create_cq(id_->verbs, min_cqe, nullptr, id_->recv_cq_channel, 0);
    GPR_ASSERT(id_->recv_cq);
    if (ibv_req_notify_cq(id_->recv_cq, 0) != 0) {
      RDMA_ERROR("ibv_req_notify_cq error: {}", strerror(errno));
      abort();
    }
    return id_->recv_cq;
  } else if (send_or_recv == 0) { // send
    id_->send_cq = ibv_create_cq(id_->verbs, min_cqe, nullptr, nullptr, 0);
    GPR_ASSERT(id_->send_cq);
    return id_->send_cq;
  } else {
    RDMA_ERROR("CreateCompleteionQueue min_cqe must is 0 or 1");
    abort();
  }
}

RdmaInfiniband::QueuePair* RdmaInfiniband::CreateQueuePair(ibv_qp_type qp_type, int port_num, ibv_cq *sxcq,
                                                           ibv_cq *rxcq, uint32_t max_send_wr, uint32_t max_recv_wr) {
  return new QueuePair(*this, qp_type, port_num, sxcq, rxcq, max_send_wr, max_recv_wr);
}

} // namespace SparkRdmaNetwork