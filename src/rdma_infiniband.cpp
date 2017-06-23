//
// Created by wyb on 17-5-12.
//

#include <infiniband/verbs.h>
#include <fcntl.h>
#include "rdma_infiniband.h"

#include "rdma_logger.h"
#include "rdma_memory_pool.h"


namespace SparkRdmaNetwork {

std::mutex RdmaInfiniband::lock_;
RdmaInfiniband* RdmaInfiniband::infiniband_ = nullptr;

// -------------------
// - CompletionQueue -
// -------------------
RdmaInfiniband::CompletionQueue::CompletionQueue(RdmaInfiniband &infiniband, int min_cqe) {
  RDMA_TRACE("create CompletionQueue");

  send_cq_channel_ = ibv_create_comp_channel(infiniband.device_.ctx_);
  GPR_ASSERT(send_cq_channel_);

  recv_cq_channel_ = ibv_create_comp_channel(infiniband.device_.ctx_);
  GPR_ASSERT(recv_cq_channel_);

  send_cq_ = ibv_create_cq(infiniband.device_.ctx_, min_cqe, nullptr, nullptr, 0);
  GPR_ASSERT(send_cq_);

  recv_cq_ = ibv_create_cq(infiniband.device_.ctx_, min_cqe, nullptr, recv_cq_channel_, 0);
  GPR_ASSERT(recv_cq_);

  if (ibv_req_notify_cq(send_cq_, 0) != 0) {
    RDMA_ERROR("ibv_req_notify_cq error: {}", strerror(errno));
    abort();
  }

  if (ibv_req_notify_cq(recv_cq_, 0) != 0) {
    RDMA_ERROR("ibv_req_notify_cq error: {}", strerror(errno));
    abort();
  }

  int flags = fcntl(send_cq_channel_->fd, F_GETFL);
  if (fcntl(send_cq_channel_->fd, F_SETFL, flags|O_NONBLOCK) < 0) {
    RDMA_ERROR("Failed to change file descriptor of Completion Event Channel");
    abort();
  }
  RDMA_DEBUG("send_cq_channel->fd = {}", send_cq_channel_->fd);

  flags = fcntl(recv_cq_channel_->fd, F_GETFL);
  if (fcntl(recv_cq_channel_->fd, F_SETFL, flags|O_NONBLOCK) < 0) {
    RDMA_ERROR("Failed to change file descriptor of Completion Event Channel");
    abort();
  }
  RDMA_DEBUG("recv_cq_channel->fd = {}", recv_cq_channel_->fd);

  RDMA_DEBUG("create send_cq:{}, recv_cq:{}", (void*)send_cq_, (void*)recv_cq_);
}

RdmaInfiniband::CompletionQueue::~CompletionQueue() {
  RDMA_TRACE("destroy CompletionQueue");

  ibv_destroy_comp_channel(send_cq_channel_);
  ibv_destroy_comp_channel(recv_cq_channel_);
  ibv_destroy_cq(recv_cq_);
  ibv_destroy_cq(send_cq_);
}
// -------------------
// - CompletionQueue -
// -------------------


// -------------
// - QueuePair -
// -------------
RdmaInfiniband::QueuePair::QueuePair(RdmaInfiniband &infiniband, ibv_qp_type qp_type,
                                     ibv_cq *send_cq, ibv_cq *recv_cq,
                                     uint32_t max_send_wr, uint32_t max_recv_wr) :
    infiniband_(infiniband), qp_type_(qp_type), ctx_(infiniband.device_.ctx_), pd_(infiniband.pd_.pd_), lid_(0),
    small_qp_(nullptr), big_qp_(nullptr),
    send_cq_(send_cq), recv_cq_(recv_cq), init_psn_(0) {
  RDMA_TRACE("create QueuePair");

  ibv_qp_init_attr init_attr;
  memset(&init_attr, 0, sizeof(init_attr));
  init_attr.qp_type = IBV_QPT_RC;
  init_attr.sq_sig_all = 1;
  init_attr.send_cq = send_cq;
  init_attr.recv_cq = recv_cq;
  init_attr.srq = NULL;
  init_attr.cap.max_send_wr = max_send_wr;
  init_attr.cap.max_recv_wr = max_recv_wr;
  init_attr.cap.max_send_sge = 5;
  init_attr.cap.max_recv_sge = 1;
  init_attr.cap.max_inline_data = 256;

  small_qp_ = ibv_create_qp(pd_, &init_attr);
  GPR_ASSERT(small_qp_);
  big_qp_ = ibv_create_qp(pd_, &init_attr);
  GPR_ASSERT(big_qp_);
  if (ModifyQpToInit() < 0) {
    RDMA_ERROR("ModifyQpToInit failed");
    abort();
  }

  ibv_port_attr port_attr;
  if (ibv_query_port(infiniband.device_.ctx_, kIbPortNum, &port_attr) < 0) {
    RDMA_ERROR("ibv_query_port error: {}", strerror(errno));
    abort();
  }
  lid_ = port_attr.lid;

  RDMA_DEBUG("create send_cq:{}, recv_cq:{}, small_qp:{}, big_qp:{}",
             (void*)send_cq_, (void*)recv_cq_, (void*)small_qp_, (void*)big_qp_);
}

RdmaInfiniband::QueuePair::~QueuePair() {
  RDMA_TRACE("destroy QueuePair");
  ibv_destroy_qp(small_qp_);
  ibv_destroy_qp(big_qp_);
}

int RdmaInfiniband::QueuePair::ModifyQpToInit() {
  ibv_qp_attr init_attr;
  memset(&init_attr, 0, sizeof(init_attr));
  init_attr.qp_state        = IBV_QPS_INIT;
  init_attr.pkey_index      = 0;
  init_attr.port_num        = kIbPortNum;
  init_attr.qp_access_flags = kRdmaMemoryFlag;
  int init_flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;

  if (ibv_modify_qp(small_qp_, &init_attr, init_flags) < 0 ||
      ibv_modify_qp(big_qp_, &init_attr, init_flags) < 0) {
    RDMA_ERROR("modify_qp_to_init: failed to modify QP to INIT, {}", strerror(errno));
    return -1;
  }
  return 0;
}

int RdmaInfiniband::QueuePair::ModifyQpToRTS() {
  ibv_qp_attr rts_attr;
  memset(&rts_attr, 0, sizeof(rts_attr));
  rts_attr.qp_state = IBV_QPS_RTS;
  rts_attr.timeout  = 0x14;
  rts_attr.retry_cnt = 0x07;
  rts_attr.rnr_retry = 0x07;
  rts_attr.max_rd_atomic = 1; // rc must add it
  rts_attr.sq_psn   = init_psn_;

  int rts_flags = IBV_QP_STATE | IBV_QP_SQ_PSN | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_MAX_QP_RD_ATOMIC;
  if (ibv_modify_qp(small_qp_, &rts_attr, rts_flags) < 0 ||
      ibv_modify_qp(big_qp_, &rts_attr, rts_flags) < 0) {
    RDMA_ERROR("modify QP to RTS error, {}", strerror(errno));
    return -1;
  }
  RDMA_DEBUG("ModifyQpToRTS success");
  return 0;
}

int RdmaInfiniband::QueuePair::ModifyQpToRTR(RdmaConnectionInfo &info) {
  ibv_qp_attr rtr_attr;
  memset(&rtr_attr, 0, sizeof(rtr_attr));
  rtr_attr.qp_state = IBV_QPS_RTR;
  rtr_attr.path_mtu = IBV_MTU_4096;
  rtr_attr.min_rnr_timer = 0x14;
  rtr_attr.max_dest_rd_atomic = 1; // rc must add it
  rtr_attr.dest_qp_num = info.small_qpn;
  rtr_attr.rq_psn = info.psn;
  rtr_attr.ah_attr.is_global = 0;
  rtr_attr.ah_attr.dlid = info.lid;
  rtr_attr.ah_attr.sl = 0;
  rtr_attr.ah_attr.src_path_bits = 0;
  rtr_attr.ah_attr.port_num = kIbPortNum;

  int rtr_flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
                  IBV_QP_RQ_PSN | IBV_QP_MIN_RNR_TIMER | IBV_QP_MAX_DEST_RD_ATOMIC;
  if (ibv_modify_qp(small_qp_, &rtr_attr, rtr_flags) < 0) {
    RDMA_ERROR("modify QP to RTS error, {}", strerror(errno));
    return -1;
  }
  rtr_attr.dest_qp_num = info.big_qpn;
  if (ibv_modify_qp(big_qp_, &rtr_attr, rtr_flags) < 0) {
    RDMA_ERROR("modify QP to RTS error, {}", strerror(errno));
    return -1;
  }
  RDMA_DEBUG("ModifyQpToRTR success");
  return 0;
}

int RdmaInfiniband::QueuePair::PostReceiveOneWithBuffer(BufferDescriptor *buffer, bool is_small) {
  buffer->bytes_ = k1KB;

  ibv_sge sge;
  sge.addr = (uint64_t)buffer->buffer_;
  sge.length = buffer->bytes_;
  sge.lkey = buffer->mr_->lkey;

  ibv_recv_wr recv_wr;
  recv_wr.wr_id = (uint64_t)buffer;
  recv_wr.sg_list = &sge;
  recv_wr.num_sge = 1;
  recv_wr.next = NULL;

  ibv_recv_wr *bad = NULL;
  if (ibv_post_recv(is_small ? small_qp_ : big_qp_, &recv_wr, &bad) < 0) {
    RDMA_ERROR("ibv_post_recv error, {}", strerror(errno));
    return -1;
  }
  return 0;
}

int RdmaInfiniband::QueuePair::PostReceiveWithNum(void *channel, bool is_small, int num) {
  ibv_sge *sge = new ibv_sge[num];
  ibv_recv_wr *recv_wr = new ibv_recv_wr[num];

  for (int i = 0; i < num; ++i) {
    BufferDescriptor *buffer = new BufferDescriptor();
    buffer->total_num_ = 1;
    buffer->buffer_ = (uint8_t *)RMALLOC(k1KB);
    buffer->bytes_ = k1KB;
    buffer->mr_ = GET_MR(buffer->buffer_);
    buffer->channel_ = channel;

    sge[i].addr = (uint64_t)buffer->buffer_;
    sge[i].length = buffer->bytes_;
    sge[i].lkey = buffer->mr_->lkey;

    recv_wr[i].wr_id = (uint64_t)buffer;
    recv_wr[i].sg_list = &sge[i];
    recv_wr[i].num_sge = 1;
    recv_wr[i].next = NULL;
  }

  ibv_recv_wr *bad = NULL;
  for (int i = 0; i < num; ++i) {
    if (ibv_post_recv(is_small ? small_qp_ : big_qp_, &recv_wr[i], &bad) < 0) {
      RDMA_ERROR("ibv_post_recv error, {}", strerror(errno));
      return -1;
    }
  }

  delete[] sge;
  delete[] recv_wr;

  return 0;
}

void RdmaInfiniband::QueuePair::PreReceive(void *channel, int small, int big) {
  if (PostReceiveWithNum(channel, SMALL_SIGN, small) < 0) {
    RDMA_ERROR("PreReceive small qp failed");
    abort();
  }
  if (PostReceiveWithNum(channel, BIG_SIGN, big) < 0) {
    RDMA_ERROR("PreReceive big qp failed");
    abort();
  }
  RDMA_DEBUG("PreReceive {} {}", small, big);
}

int RdmaInfiniband::QueuePair::PostSend(BufferDescriptor *buf, int num, bool is_small) {
  ibv_sge sge[num];
  for (int i = 0; i < num; ++i) {
    sge[i].addr = (uint64_t)buf[i].buffer_;
    sge[i].length = buf[i].bytes_;
    sge[i].lkey = buf[i].mr_->lkey;
  }
  ibv_send_wr send_wr;
  memset(&send_wr, 0, sizeof(send_wr));
  send_wr.wr_id = (uint64_t)buf;
  send_wr.sg_list = &sge[0];
  send_wr.num_sge = num;
  send_wr.opcode = IBV_WR_SEND;
  send_wr.send_flags = IBV_SEND_SIGNALED;

  ibv_send_wr *bad = NULL;
  if (ibv_post_send(is_small?small_qp_:big_qp_, &send_wr, &bad) < 0) {
    RDMA_ERROR("ibv_post_send error, {}", strerror(errno));
    return -1;
  }
  return 0;
}

int RdmaInfiniband::QueuePair::PostSendAndWait(BufferDescriptor *buf, int num, bool is_small) {
  ibv_sge sge[num];
  for (int i = 0; i < num; ++i) {
    sge[i].addr = (uint64_t)buf[i].buffer_;
    sge[i].length = buf[i].bytes_;
    sge[i].lkey = buf[i].mr_->lkey;
  }
  ibv_send_wr send_wr;
  memset(&send_wr, 0, sizeof(send_wr));
  send_wr.wr_id = (uint64_t)buf;
  send_wr.sg_list = &sge[0];
  send_wr.num_sge = num;
  send_wr.opcode = IBV_WR_SEND;
  send_wr.send_flags = IBV_SEND_SIGNALED;

  RDMA_TRACE("PostSendAndWait {} begin", is_small?"samll":"big");
  ibv_send_wr *bad = NULL;
  {
    std::lock_guard<std::mutex> lock(send_lock);
    if (ibv_post_send(is_small?small_qp_:big_qp_, &send_wr, &bad) < 0) {
     RDMA_ERROR("ibv_post_send error, {}", strerror(errno));
      return -1;
    }

    ibv_wc wc;
    int event_num;
    do {
      event_num = ibv_poll_cq(send_cq_, 1, &wc);
    } while (event_num == 0);
    if (event_num < 0) {
      RDMA_ERROR("ibv_poll_cq error, {}", strerror(errno));
      return -1;
    }
    if (wc.status != IBV_WC_SUCCESS) {
      RDMA_ERROR("ibv_poll_cq error: {}", WcStatusToString(wc.status));
      return -1;
    }
  }
  RDMA_TRACE("PostSendAndWait end");
  return 0;
}

int RdmaInfiniband::QueuePair::PostWrite(BufferDescriptor *buf, int num, uint64_t addr, uint32_t rkey) {
  ibv_sge sge[num];
  for (int i = 0; i < num; ++i) {
    sge[i].addr = (uint64_t)buf[i].buffer_;
    sge[i].length = buf[i].bytes_;
    sge[i].lkey = buf[i].mr_->lkey;
  }
  ibv_send_wr send_wr;
  memset(&send_wr, 0, sizeof(send_wr));
  send_wr.wr_id = 0;
  send_wr.sg_list = &sge[0];
  send_wr.num_sge = num;
  send_wr.opcode = IBV_WR_RDMA_WRITE;
  send_wr.send_flags = IBV_SEND_SIGNALED;
  send_wr.wr.rdma.remote_addr = addr;
  send_wr.wr.rdma.rkey = rkey;

  ibv_send_wr *bad;
  if (ibv_post_send(big_qp_, &send_wr, &bad) < 0) {
    RDMA_ERROR("ibv_post_send error, {}", strerror(errno));
    return -1;
  }
  return 0;
}

int RdmaInfiniband::QueuePair::PostWriteAndWait(BufferDescriptor *buf, int num, uint64_t addr, uint32_t rkey) {
  ibv_sge sge[num];
  for (int i = 0; i < num; ++i) {
    sge[i].addr = (uint64_t)buf[i].buffer_;
    sge[i].length = buf[i].bytes_;
    sge[i].lkey = buf[i].mr_->lkey;
  }
  ibv_send_wr send_wr;
  memset(&send_wr, 0, sizeof(send_wr));
  send_wr.wr_id = 0;
  send_wr.sg_list = sge;
  send_wr.num_sge = num;
  send_wr.opcode = IBV_WR_RDMA_WRITE;
  send_wr.send_flags = IBV_SEND_SIGNALED;
  send_wr.wr.rdma.remote_addr = addr;
  send_wr.wr.rdma.rkey = rkey;

  ibv_send_wr *bad;
  {
    std::lock_guard<std::mutex> lock(send_lock);
    if (ibv_post_send(big_qp_, &send_wr, &bad) < 0) {
      RDMA_ERROR("ibv_post_send error, {}", strerror(errno));
      return -1;
    }
    ibv_wc wc;
    int event_num;
    do {
      event_num = ibv_poll_cq(send_cq_, 1, &wc);
    } while (event_num == 0);
    if (event_num < 0) {
      RDMA_ERROR("ibv_poll_cq error, {}", strerror(errno));
      return -1;
    }
    if (wc.status != IBV_WC_SUCCESS) {
      RDMA_ERROR("ibv_poll_cq error: {}", WcStatusToString(wc.status));
      return -1;
    }
  }
  return 0;
}

int RdmaInfiniband::QueuePair::PostRead(BufferDescriptor *buf, int num, uint64_t addr, uint32_t rkey) {
  ibv_sge sge[num];
  for (int i = 0; i < num; ++i) {
    sge[i].addr = (uint64_t)buf[i].buffer_;
    sge[i].length = buf[i].bytes_;
    sge[i].lkey = buf[i].mr_->lkey;
  }
  ibv_send_wr send_wr;
  memset(&send_wr, 0, sizeof(send_wr));
  send_wr.wr_id = 0;
  send_wr.sg_list = &sge[0];
  send_wr.num_sge = num;
  send_wr.opcode = IBV_WR_RDMA_READ;
  send_wr.send_flags = IBV_SEND_SIGNALED;
  send_wr.wr.rdma.remote_addr = addr;
  send_wr.wr.rdma.rkey = rkey;

  ibv_send_wr *bad;
  if (ibv_post_send(big_qp_, &send_wr, &bad) < 0) {
    RDMA_ERROR("ibv_post_send error, {}", strerror(errno));
    return -1;
  }
  return 0;
}

const char* RdmaInfiniband::QueuePair::WcStatusToString(int status) {
  static const char *wc_string[] = {
      "IBV_WC_SUCCESS",
      "IBV_WC_LOC_LEN_ERR",
      "IBV_WC_LOC_QP_OP_ERR",
      "IBV_WC_LOC_EEC_OP_ERR",
      "IBV_WC_LOC_PROT_ERR",
      "IBV_WC_WR_FLUSH_ERR",
      "IBV_WC_MW_BIND_ERR",
      "IBV_WC_BAD_RESP_ERR",
      "IBV_WC_LOC_ACCESS_ERR",
      "IBV_WC_REM_INV_REQ_ERR",
      "IBV_WC_REM_ACCESS_ERR",
      "IBV_WC_REM_OP_ERR",
      "IBV_WC_RETRY_EXC_ERR",
      "IBV_WC_RNR_RETRY_EXC_ERR",
      "IBV_WC_LOC_RDD_VIOL_ERR",
      "IBV_WC_REM_INV_RD_REQ_ERR",
      "IBV_WC_REM_ABORT_ERR",
      "IBV_WC_INV_EECN_ERR",
      "IBV_WC_INV_EEC_STATE_ERR",
      "IBV_WC_FATAL_ERR",
      "IBV_WC_RESP_TIMEOUT_ERR",
      "IBV_WC_GENERAL_ERR",
  };
  if (status < IBV_WC_SUCCESS || status > IBV_WC_GENERAL_ERR)
    return "<status out of range!>";
  return wc_string[status];
}
// -------------
// - QueuePair -
// -------------


// ------------------
// - RdmaInfiniband -
// ------------------
RdmaInfiniband::RdmaInfiniband() : device_(), pd_(device_) {
  RDMA_TRACE("construct RdmaInfiniband");
  RdmaMemoryPool::InitMemoryPool(pd_.pd_);
}

RdmaInfiniband::~RdmaInfiniband() {
  RDMA_TRACE("destroy RdmaInfiniband");
}

RdmaInfiniband::CompletionQueue* RdmaInfiniband::CreateCompleteionQueue(int min_cqe) {
  return new CompletionQueue(*this, min_cqe);
}

RdmaInfiniband::QueuePair* RdmaInfiniband::CreateQueuePair(ibv_cq *send_cq, ibv_cq *recv_cq, ibv_qp_type qp_type,
                                                           uint32_t max_send_wr, uint32_t max_recv_wr) {
  return new QueuePair(*this, qp_type, send_cq, recv_cq, max_send_wr, max_recv_wr);
}
RdmaInfiniband::QueuePair * RdmaInfiniband::CreateQueuePair(CompletionQueue *cq, ibv_qp_type qp_type,
                                                            uint32_t max_send_wr, uint32_t max_recv_wr) {
  return CreateQueuePair(cq->get_send_cq(), cq->get_recv_cq(), qp_type, max_send_wr, max_recv_wr);
}
// ------------------
// - RdmaInfiniband -
// ------------------


} // namespace SparkRdmaNetwork