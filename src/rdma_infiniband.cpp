//
// Created by wyb on 17-5-12.
//

#include "rdma_infiniband.h"

#include "rdma_logger.h"


namespace SparkRdmaNetwork {

RdmaInfiniband::QueuePair::QueuePair(RdmaInfiniband &infiniband, ibv_qp_type qp_type, int port_num, ibv_cq *sxcq,
                                     ibv_cq *rxcq, uint32_t max_send_wr, uint32_t max_recv_wr) :
    infiniband_(infiniband), qp_type_(qp_type), ctx_(infiniband.device_.ctx_), pd_(infiniband.pd_.pd_), qp_(nullptr),
    sxcq_(sxcq), rxcq_(rxcq), init_psn_(0) {
  RDMA_DEBUG("create qp");
  GPR_ASSERT(qp_type == IBV_QPT_RC);

  ibv_qp_init_attr init_attr = {
      .qp_type = qp_type,
      .sq_sig_all = 1, // ??
      .send_cq = sxcq,
      .recv_cq = rxcq,
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

RdmaInfiniband::QueuePair* RdmaInfiniband::CreateQueuePair(ibv_qp_type qp_type, int port_num, ibv_cq *sxcq,
                                                           ibv_cq *rxcq, uint32_t max_send_wr, uint32_t max_recv_wr) {
  return new QueuePair(*this, qp_type, port_num, sxcq, rxcq, max_send_wr, max_recv_wr);
}

} // namespace SparkRdmaNetwork