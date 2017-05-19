//
// Created by wyb on 17-5-15.
//

#ifndef SPARKRDMA_RDMA_CHANNEL_H
#define SPARKRDMA_RDMA_CHANNEL_H

#include <cstdint>

namespace SparkRdmaNetwork {

class RdmaChannel {
public:
  void Init(const char *host, uint16_t port)
};

} // namespace SparkRdmaNetwork

#endif //SPARKRDMA_RDMA_CHANNEL_H
