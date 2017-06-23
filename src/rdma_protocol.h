//
// Created by wyb on 17-5-20.
//

#ifndef SPARKRDMA_RDMA_PROTOCOL_H
#define SPARKRDMA_RDMA_PROTOCOL_H

#include <cstdint>
#include <infiniband/verbs.h>

namespace SparkRdmaNetwork {

const uint16_t kDefaultPort = 6666;

struct RdmaConnectionInfo {
  uint16_t lid;
  uint32_t small_qpn;
  uint32_t big_qpn;
  uint32_t psn;
};

// data_type: 1. send small data
//            2. rpc request
//            3. rpc ack
//            4. write big data
//            5. write success
enum RdmaDataType: uint8_t {
  TYPE_UNKNOW = 0,
  TYPE_SMALL_DATA,
  TYPE_BIG_DATA,
  TYPE_RPC_REQ,
  TYPE_RPC_ACK,
  TYPE_WRITE_SUCCESS,
};
struct RdmaDataHeader {
  RdmaDataType data_type;
  uint32_t data_len;
  uint32_t data_id;
}__attribute__((__packed__));

struct RdmaRpc {
  RdmaDataType data_type;
  uint32_t data_len;
  uint32_t data_id;
  uint32_t rkey;
  uint64_t addr;
}__attribute__((__packed__));

const int kRdmaMemoryFlag = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;

#define SMALL_SIGN true
#define BIG_SIGN false

const uint32_t kSmallBig = 1024;

inline bool IS_SMALL(uint32_t len) {
  return len + sizeof(RdmaDataHeader) <= kSmallBig;
}

} // namespace SparkRdmaNetwork

#endif //SPARKRDMA_RDMA_PROTOCOL_H
