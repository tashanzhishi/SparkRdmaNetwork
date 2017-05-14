//
// Created by wyb on 17-5-14.
//

#ifndef SPARKRDMA_RDMA_LINK_H
#define SPARKRDMA_RDMA_LINK_H

#include <rdma/rdma_cma.h>

namespace SparkRdmaNetwork {

// use rdma_cma api to establish connection
class RdmaSocket {
  RdmaSocket(): sockt_(nullptr){}
  ~RdmaSocket(){}

  void bind()

private:
  static rdma_event_channel *event_channel_;

  rdma_cm_id *sockt_;

};

} // namespace SparkRdmaNetwork

#endif //SPARKRDMA_RDMA_LINK_H
