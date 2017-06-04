//
// Created by wyb on 17-5-15.
//

#include "rdma_channel.h"

#include <cstring>
#include <infiniband/verbs.h>

#include <string>
#include <memory>

#include "rdma_thread.h"
#include "rdma_logger.h"
#include "rdma_memory_pool.h"

namespace SparkRdmaNetwork {

const std::string RdmaChannel::get_ip_from_host(const std::string& host) {
  {
    ReadLock(Host2IpLock);
    if (Host2Ip.find(host) != Host2Ip.end())
      return Host2Ip[host];
  }

  WriteLock(Host2IpLock);
  Host2Ip[host] = RdmaSocket::GetIpFromHost(host.c_str());
  RDMA_TRACE("GetIpFromHost {} -> {}", host, Host2Ip[host]);
  return Host2Ip[host];
}

RdmaChannel* RdmaChannel::get_channel_from_ip(const std::string &ip) const {
  {
    ReadLock(Ip2ChannelLock);
    if (Ip2Channel.find(ip) != Ip2Channel.end())
      return Ip2Channel.at(ip);
  }
  RDMA_ERROR("get_channel_from_ip failed");
  return nullptr;
}

RdmaChannel::RdmaChannel(const char *host, uint16_t port) :
    ip_(""), port_(kDefaultPort), cq_(nullptr), qp_(nullptr), data_id_(0), is_ready(0),
    recv_data_running_(false), req_rpc_running_(false), ack_rpc_running_(false) {
  //Init(host, port);
}

RdmaChannel::~RdmaChannel() {
  RDMA_TRACE("destruct RdmaChannel");
  delete cq_;
  delete qp_;
}

// when client call init, this mean client will create channel
int RdmaChannel::Init(const char *c_host, uint16_t port) {
  RDMA_TRACE("init channel to {}", c_host);

  port_ = port;

  if (c_host) {
    const std::string host(c_host);
    ip_ = get_ip_from_host(host);
  }

  std::shared_ptr<RdmaSocket> socket = new RdmaSocket(ip_, port_);
  socket->Socket();
  socket->Connect();

  if (InitChannel(socket) < 0) {
    RDMA_ERROR("init rdma channel failed");
    abort();
  }
  return 0;
}

// msg: header + msg
// len = header len + msg len
int RdmaChannel::SendMsg(const char *host, uint16_t port, uint8_t *msg, uint32_t len) {
  RDMA_TRACE("send msg {}:{}", host, len);
  uint32_t data_id = atomic_fetch_add(&data_id_, 1);
  data_id += 1;

  RdmaDataHeader *header = (RdmaDataHeader*)msg;
  memset(header, 0, sizeof(RdmaDataHeader));
  header->data_type = TYPE_UNKNOW;
  header->data_id = data_id;
  header->data_len = len;


  BufferDescriptor *send_buff = new BufferDescriptor();
  send_buff->buffer_ = msg;
  send_buff->bytes_ = len;
  send_buff->channel_ = this;
  send_buff->mr_ = GET_MR(msg);

  if (IS_SMALL(len)) {
    header->data_type = TYPE_SMALL_DATA;

    qp_->PostSendAndWait(send_buff, 1, true);
    delete send_buff;
  } else {
    header->data_type = TYPE_RPC_REQ;
    RdmaDataHeader *req_data = (RdmaDataHeader *)RMALLOC(sizeof(RdmaDataHeader));
    memcpy(req_data, header, sizeof(RdmaDataHeader));
    BufferDescriptor rpc_buff;
    rpc_buff.buffer_ = (uint8_t *)req_data;
    rpc_buff.bytes_ = sizeof(RdmaDataHeader);
    rpc_buff.channel_ = this;
    rpc_buff.mr_ = GET_MR(req_data);
    {
      std::lock_guard lock(id2data_lock_);
      id2data_[data_id] = std::pair<BufferDescriptor*, int>(send_buff, 1);
    }

    qp_->PostSendAndWait(&rpc_buff, 1, false);
  }

  return 0;
}

//header : rdma_header + header
//header_len = rdma head len + head len
int RdmaChannel::SendMsgWithHeader(const char *host, uint16_t port, uint8_t *header, const uint32_t header_len,
                                   uint8_t *body, const uint32_t body_len) {
  uint32_t data_len = header_len + body_len;
  uint32_t data_id = atomic_fetch_add(&data_id_, 1);
  data_id += 1;

  RdmaDataHeader *rdma_header = (RdmaDataHeader*)header;
  memset(rdma_header, 0, sizeof(RdmaDataHeader));
  rdma_header->data_type = TYPE_UNKNOW;
  rdma_header->data_id = data_id;
  rdma_header->data_len = data_len;

  BufferDescriptor *send_buff = new BufferDescriptor[2];
  send_buff[0].buffer_ = header;
  send_buff[0].bytes_ = header_len;
  send_buff[0].channel_ = this;
  send_buff[0].mr_ = GET_MR(header);
  send_buff[1].buffer_ = body;
  send_buff[1].bytes_ = body_len;
  send_buff[1].channel_ = this;
  send_buff[1].mr_ = GET_MR(body);

  if (IS_SMALL(data_len)) {
    rdma_header->data_type = TYPE_SMALL_DATA;

    qp_->PostSendAndWait(send_buff, 2, true);
    delete[] send_buff;
  } else {
    rdma_header->data_type = TYPE_RPC_REQ;
    RdmaDataHeader *req_data = (RdmaDataHeader *)RMALLOC(sizeof(RdmaDataHeader));
    memcpy(req_data, rdma_header, sizeof(RdmaDataHeader));
    BufferDescriptor buf;
    buf.buffer_ = (uint8_t *)req_data;
    buf.bytes_ = sizeof(RdmaDataHeader);
    buf.channel_ = this;
    buf.mr_ = GET_MR(req_data);
    {
      std::lock_guard lock(id2data_lock_);
      id2data_[data_id] = std::pair<BufferDescriptor*, int>(send_buff, 2);
    }

    qp_->PostSendAndWait(&buf, 1, false);
  }

  return 0;
}

int RdmaChannel::InitChannel(std::shared_ptr<RdmaSocket> socket) {
  RDMA_TRACE("start create cq qp etc.");
  RdmaInfiniband *infiniband = RdmaInfiniband::GetRdmaInfiniband();

  {
    std::lock_guard lock(channel_lock_);
    if (cq_ == nullptr) {
      cq_ = infiniband->CreateCompleteionQueue();
      qp_ = infiniband->CreateQueuePair(cq_);
      qp_->PreReceive(this);
    }
  }

  int fd = cq_->get_recv_cq_channel()->fd;
  if (Fd2Channel.find(fd) == Fd2Channel.end()) {
    WriteLock(Fd2ChannelLock);
    if (Fd2Channel.find(fd) == Fd2Channel.end())
      Fd2Channel[fd] = this;
  }

  RdmaConnectionInfo local_info, remote_info;
  local_info.small_qpn = qp_->get_local_qp_num(1);
  local_info.big_qpn = qp_->get_local_qp_num(0);
  local_info.psn = qp_->get_init_psn();
  local_info.lid = qp_->get_local_lid();

  socket->WriteInfo(local_info);
  socket->ReadInfo(remote_info);

  {
    std::lock_guard lock(channel_lock_);
    if (is_ready == 0) {
      if (qp_->ModifyQpToRTS() < 0) {
        RDMA_ERROR("ModifyQpToRTS failed");
        return -1;
      }
      if (qp_->ModifyQpToRTR(remote_info) < 0) {
        RDMA_ERROR("ModifyQpToRTR failed");
        return -1;
      }
      is_ready = 1;
    }
  }

  return 0;
}

} // namespace SparkRdmaNetwork