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

std::map<std::string, RdmaChannel *> RdmaChannel::Ip2Channel;
boost::shared_mutex RdmaChannel::Ip2ChannelLock;

int RdmaChannel::PutChannelByIp(std::string ip, RdmaChannel *channel) {
  WriteLock wr_lock(Ip2ChannelLock);
  if (Ip2Channel.find(ip) != Ip2Channel.end()) {
    RDMA_ERROR("Ip2Channel[{}] = {} is existing", ip, (void*)Ip2Channel.at(ip));
    return 0;
  }
  Ip2Channel[ip] = channel;
  RDMA_DEBUG("insert Ip2Channel[{}] = {}", ip, (void*)channel);
  return 1;
}

RdmaChannel* RdmaChannel::GetChannelByIp(const std::string &ip) {
  {
    ReadLock rd_lock(Ip2ChannelLock);
    if (Ip2Channel.find(ip) != Ip2Channel.end()) {
      return Ip2Channel.at(ip);
    } else {
      return nullptr;
    }
  }
}

void RdmaChannel::DestroyAllChannel() {
  RDMA_INFO("destroy channel and thread pool");
  WriteLock wr_lock(Ip2ChannelLock);
  for (auto &kv : Ip2Channel) {
    RdmaChannel *channel = kv.second;
    delete channel;
  }
  RdmaEvent::thread_pool_.close();
}

// RdmaChannel must be create once between two machine
RdmaChannel::RdmaChannel(const char *host, uint16_t port) :
    ip_(""), port_(kDefaultPort), cq_(nullptr), qp_(nullptr), event_(nullptr), is_ready_(0), data_id_(0) {
  ip_ = RdmaSocket::GetIpByHost(host);
  port_ = port;
  PutChannelByIp(ip_, this);// may have bug
}

RdmaChannel::~RdmaChannel() {
  RDMA_INFO("destruct RdmaChannel");
  delete event_;
  delete cq_;
  delete qp_;
}

// when client call init, this mean client will create channel
int RdmaChannel::Init(const char *c_host, uint16_t port) {
  if (port == 0) {
    port = kDefaultPort;
  }
  RDMA_DEBUG("init channel to {}:{}", c_host, port);

  port_ = port;
  ip_ = RdmaSocket::GetIpByHost(c_host);

  std::shared_ptr<RdmaSocket> socket(new RdmaSocket(ip_, port_));
  socket->Socket();
  socket->Connect();

  if (InitChannel(socket, false) < 0) {
    RDMA_ERROR("init rdma channel failed");
    abort();
  }
  return 0;
}

// msg = data_header + spark_msg
int RdmaChannel::SendMsg(const char *host, uint16_t port, uint8_t *msg, uint32_t len) {
  uint32_t data_id = std::atomic_fetch_add(&data_id_, (uint32_t)1);
  data_id += 1;
  RDMA_DEBUG("SendMsg {}:{}:{} {}", host, port, data_id, len);

  RdmaDataHeader *data_header = (RdmaDataHeader*)msg;
  data_header->data_type = (len <= kSmallBig)? TYPE_SMALL_DATA: TYPE_BIG_DATA;
  data_header->data_len = len;
  data_header->data_id = data_id;

  BufferDescriptor *data_bd = new BufferDescriptor();
  memset(data_bd, 0, sizeof(BufferDescriptor));
  data_bd->buffer_ = msg;
  data_bd->bytes_ = len;
  data_bd->mr_ = GET_MR(msg);

  if (len <= kSmallBig) {
    if (qp_->PostSend(data_bd, 1, SMALL_SIGN) < 0) {
      RDMA_ERROR("PostSend small_data failed");
      abort();
    }
  } else {
    // wait for read
    event_->PutDataById(data_id, data_bd, 1);

    RdmaRpc *rpc = (RdmaRpc*)RMALLOC(sizeof(RdmaRpc));
    rpc->data_type = TYPE_RPC_REQ;
    rpc->data_id = data_id;
    rpc->data_len = len;
    rpc->addr = (uint64_t)msg;
    rpc->rkey = data_bd->mr_->rkey;

    BufferDescriptor *rpc_bd = new BufferDescriptor();
    rpc_bd->buffer_ = (uint8_t*)rpc;
    rpc_bd->bytes_ = sizeof(RdmaRpc);
    rpc_bd->mr_ = GET_MR(rpc);

    if (qp_->PostSend(rpc_bd, 1, BIG_SIGN) < 0) {
      RDMA_ERROR("PostSend req rpc failed");
      abort();
    }
  }

  return 0;
}

int RdmaChannel::InitChannel(std::shared_ptr<RdmaSocket> socket, bool is_accept) {
  RDMA_DEBUG("start InitChannel of {}", ip_);
  RdmaInfiniband *infiniband = RdmaInfiniband::get_infiniband();

  {
    std::lock_guard<std::mutex> lock(channel_lock_);
    if (cq_ == nullptr) {
      RDMA_DEBUG("create cq, qp, event of {}", ip_);
      cq_ = infiniband->CreateCompleteionQueue();
      qp_ = infiniband->CreateQueuePair(cq_);
      qp_->PreReceive(this);
      event_ = new RdmaEvent(ip_, cq_->get_send_cq_channel(), cq_->get_recv_cq_channel(), qp_);
    }
  }

  RdmaConnectionInfo local_info, remote_info;
  local_info.small_qpn = qp_->get_local_qp_num(SMALL_SIGN);
  local_info.big_qpn = qp_->get_local_qp_num(BIG_SIGN);
  local_info.psn = qp_->get_init_psn();
  local_info.lid = qp_->get_local_lid();

  if (is_accept) {
    socket->ReadInfo(remote_info);
    socket->WriteInfo(local_info);
  } else {
    socket->WriteInfo(local_info);
    socket->ReadInfo(remote_info);
  }

  {
    std::lock_guard<std::mutex> lock(channel_lock_);
    if (is_ready_ == 0) {
      RDMA_DEBUG("modify qp to rtr and rts of {}", ip_);
      if (qp_->ModifyQpToRTR(remote_info) < 0) {
        RDMA_ERROR("ModifyQpToRTR failed");
        abort();
      }
      if (qp_->ModifyQpToRTS() < 0) {
        RDMA_ERROR("ModifyQpToRTS failed");
        abort();
      }
      is_ready_ = 1;
    }
  }

  return 0;
}

} // namespace SparkRdmaNetwork