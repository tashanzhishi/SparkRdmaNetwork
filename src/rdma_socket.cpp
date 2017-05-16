//
// Created by wyb on 17-5-14.
//

#include "rdma_socket.h"

#include <cstring>
#include <cerrno>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "rdma_logger.h"


namespace SparkRdmaNetwork {

ibv_context* RdmaSocket::ctx_ = nullptr;
ibv_pd *RdmaSocket::pd_ = nullptr;

void RdmaSocket::InitInfinaband() {
  RDMA_TRACE("initilize infiniband device ...");

  struct ibv_device **dev_list;
  struct ibv_device *ib_dev;

  dev_list = ibv_get_device_list(NULL);
  GPR_ASSERT(dev_list);

  ib_dev = dev_list[0];
  GPR_ASSERT(ib_dev);

  ctx_ = ibv_open_device(ib_dev);
  GPR_ASSERT(ctx_);

  pd_ = ibv_alloc_pd(ctx_);
  GPR_ASSERT(pd_);

  ibv_free_device_list(dev_list);
  RDMA_TRACE("initilize infiniband device success");
}

RdmaSocket::QueuePair::QueuePair(rdma_cm_id* id, ibv_pd *pd, ibv_qp_type qp_type, int port_num,
                     ibv_cq *send_cq, ibv_cq *recv_cq, uint32_t max_send_wr, uint32_t max_recv_wr) :
    id_(id), pd_(pd), qp_type_(qp_type), qp_(nullptr), port_num_(port_num), send_cq_(send_cq), recv_cq_(recv_cq), init_psn_(0) {
  RDMA_TRACE("create QueuePair");

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

  if (rdma_create_qp(id, pd, &init_attr) != 0) {
    RDMA_ERROR("rdma_create_qp error: {}", strerror(errno));
    abort();
  }
  qp_ = id->qp;
}


RdmaSocket::RdmaSocket() {
  id_->verbs = ctx_;
  id_->pd = pd_;

  id_->recv_cq_channel = ibv_create_comp_channel(id_->verbs);
  GPR_ASSERT(id_->recv_cq_channel);

  id_->send_cq = ibv_create_cq(id_->verbs, kMinCqe, nullptr, nullptr, 0);
  GPR_ASSERT(id_->send_cq);

  id_->recv_cq = ibv_create_cq(id_->verbs, kMinCqe, nullptr, id_->recv_cq_channel, 0);
  GPR_ASSERT(id_->recv_cq);

  if (ibv_req_notify_cq(id_->recv_cq, 0) != 0) {
    RDMA_ERROR("ibv_req_notify_cq error, %s", strerror(errno));
    abort();
  }
}

RdmaSocket::~RdmaSocket() {

}

void RdmaSocket::bind(struct sockaddr *addr) {
  if (rdma_bind_addr(id_, addr) != 0) {
    RDMA_ERROR("rdma_bind_addr error: {}", strerror(errno));
    abort();
  }
}

void RdmaSocket::bind(const char *ip, uint16_t port) {
  struct sockaddr_in addr;
  memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons(port);
  addr.sin_addr.s_addr = inet_addr(ip);

  bind((struct sockaddr *)&addr);
}

void RdmaSocket::bind() {
  bind("0.0.0.0", kDefaultPort);
}

void RdmaSocket::listen(int backlog = 16) {
  if (rdma_listen(id_, backlog) != 0 ) {
    RDMA_ERROR("rdma_listen error: {}", strerror(errno));
    abort();
  }
}





RdmaSocket::QueuePair* RdmaSocket::CreateQueuePair(ibv_pd *pd, ibv_qp_type qp_type, int port_num,
                                                   ibv_cq *send_cq, ibv_cq *recv_cq,
                                                   uint32_t max_send_wr, uint32_t max_recv_wr) {
  return new QueuePair(*this, pd, qp_type, port_num, send_cq, recv_cq, max_send_wr, max_recv_wr);
}

ibv_cq* RdmaSocket::CreateCompleteionQueue(int min_cqe, int send_or_recv) {
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

} // namespace SparkRdmaNetwork