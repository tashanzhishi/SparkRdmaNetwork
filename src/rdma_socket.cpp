//
// Created by wyb on 17-5-14.
//

#include "rdma_socket.h"

namespace SparkRdmaNetwork {

rdma_event_channel* RdmaSocket::event_channel_ = rdma_create_event_channel();

} // namespace SparkRdmaNetwork