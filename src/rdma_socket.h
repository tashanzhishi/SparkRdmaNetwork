//
// Created by wyb on 17-5-14.
//

#ifndef SPARKRDMA_RDMA_LINK_H
#define SPARKRDMA_RDMA_LINK_H

#include <sys/socket.h>
#include <rdma/rdma_cma.h>

#include "rdma_logger.h"

namespace SparkRdmaNetwork {

const uint16_t kDefaultPort = 6789;
const int kMinCqe = 1024;

class RdmaSocket;
RdmaSocket* const kConnectEstablished = (RdmaSocket*) 1;
RdmaSocket* const kDisconnect = (RdmaSocket*) 2;


// use rdma_cma api to establish connection
class RdmaSocket {
public:
  class QueuePair {
  public:
    QueuePair(rdma_cm_id* id, ibv_pd *pd, ibv_qp_type qp_type, int port_num,
              ibv_cq *send_cq, ibv_cq *recv_cq, uint32_t max_send_wr, uint32_t max_recv_wr);
    ~QueuePair();
    uint32_t get_init_psn() const;
    uint32_t get_local_qp_num() const;
  private:
    rdma_cm_id  *id_;
    ibv_pd      *pd_;
    int         qp_type_;   // QP type (IBV_QPT_RC, etc.)
    ibv_qp      *qp_;
    int         port_num_;
    ibv_cq      *send_cq_;
    ibv_cq      *recv_cq_;
    uint32_t    init_psn_;
  };


  RdmaSocket();
  ~RdmaSocket();
  void bind(struct sockaddr *addr);
  void bind(const char *ip, uint16_t port);
  void bind();
  void listen(int backlog);
  RdmaSocket* accept();
  void connect();
  void close();



  QueuePair* CreateQueuePair(ibv_pd *pd, ibv_qp_type qp_type, int port_num,
                             ibv_cq *send_cq, ibv_cq *recv_cq, uint32_t max_send_wr, uint32_t max_recv_wr);
  ibv_cq* CreateCompleteionQueue(int min_cqe, int send_or_recv);

  static void InitInfinaband();
private:
  static ibv_context *ctx_;
  static ibv_pd *pd_;

  // only use: (verbs, pd), recv_cq_channel, send_cq, recv_cq
  rdma_cm_id *id_;

  int visit_;
  // all qp use same cq
  QueuePair *small_qp_;
  QueuePair *big_qp_;

};

} // namespace SparkRdmaNetwork

#endif //SPARKRDMA_RDMA_LINK_H
