//
// Created by wyb on 17-5-24.
//

#include "rdma_event.h"

#include <cstddef>
#include <cstring>
#include <poll.h>
#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <infiniband/verbs.h>
#include <sys/time.h>

#include <queue>

#include "rdma_logger.h"
#include "rdma_thread.h"
#include "rdma_memory_pool.h"
#include "jni_common.h"

namespace SparkRdmaNetwork {

boost::basic_thread_pool RdmaEvent::thread_pool_(20);

RdmaEvent::RdmaEvent(std::string ip, ibv_comp_channel *send_cq_channel, ibv_comp_channel *recv_cq_channel,
                     QueuePair *qp) {
  kill_recv_fd_ = EventfdCreate();
  if (kill_recv_fd_ < 0) {
    RDMA_ERROR("create wakeup_fd failed");
    abort();
  }

  ip_ = ip;
  recv_cq_channel_ = recv_cq_channel;
  send_cq_channel_ = send_cq_channel;
  recv_fd_ = recv_cq_channel->fd;
  send_fd_ = send_cq_channel->fd;
  qp_ = qp;
  recv_runing_ = 0;
  recv_data_id_ = 1;

  PollFunction poll_send_func = std::bind(&RdmaEvent::PollSendThreadFunc, this);
  PollFunction poll_recv_func = std::bind(&RdmaEvent::PollRecvThreadFunc, this);
  thread_pool_.submit(poll_send_func);
  thread_pool_.submit(poll_recv_func);

  RDMA_INFO("create RdmaEvent for {}", ip_);
}

RdmaEvent::~RdmaEvent() {
  KillPollThread();
}

int RdmaEvent::EventfdCreate() {
  int fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
  if (fd < 0) {
    RDMA_ERROR("eventfd error, {}", strerror(errno));
    return -1;
  }
  return fd;
}

int RdmaEvent::EventfdWakeup(int fd) {
  int ret;
  do {
    ret = eventfd_write(fd, 1);
  } while (ret < 0 && errno == EINTR);
  if (ret < 0) {
    RDMA_ERROR("eventfd_write error, {}", strerror(errno));
    abort();
  }
  return 0;
}

int RdmaEvent::EventfdConsume(int fd) {
  eventfd_t value;
  int ret;
  do {
    ret = eventfd_read(fd, &value);
  } while (ret < 0 && errno == EINTR);
  if (ret < 0 && errno != EAGAIN) {
    RDMA_ERROR("eventfd_read error, {}", strerror(errno));
    abort();
  }
  return 0;
}

int RdmaEvent::KillPollThread() {
  EventfdWakeup(kill_recv_fd_);
  pthread_kill(poll_send_thread_id, SIGQUIT);
  return 0;
}

static void quit_thread(int signo) {
  RDMA_DEBUG("thread exit");
  pthread_exit(NULL);
}

void RdmaEvent::PollSendThreadFunc() {
  RDMA_INFO("poll send thread for {} start", ip_);
  poll_send_thread_id = pthread_self();
  // register quit signal
  signal(SIGQUIT, quit_thread);

  PollSendCq(500);
}

void RdmaEvent::PollRecvThreadFunc() {
  RDMA_INFO("poll recv thread for {} start", ip_);
  poll_recv_thread_id = pthread_self();
  while (1) {
    int ret = PollRecvCq(500);
    if (ret == 1) {
      RDMA_INFO("poll recv thrad end");
      return;
    } else if (ret == -1) {
      RDMA_ERROR("poll recv thread failed");
      abort();
    }
  }
}

int RdmaEvent::PollSendCq(int timeout) {
  while (1) {
    ibv_cq *ev_cq;
    void *ev_ctx;
    if (ibv_get_cq_event(send_cq_channel_, &ev_cq, &ev_ctx) < 0) {
      RDMA_ERROR("ibv_get_cq_event error, {}", strerror(errno));
      abort();
    }
    ibv_ack_cq_events(ev_cq, 1);
    if (ibv_req_notify_cq(ev_cq, 0) < 0) {
      RDMA_ERROR("ibv_req_notify_cq error, {}", strerror(errno));
      abort();
    }

    int event_num;
    do {
      ibv_wc wc;
      event_num = ibv_poll_cq(ev_cq, 1, &wc);
      RDMA_TRACE("ibv_poll_cq send {}", event_num);

      if (event_num < 0) {
        RDMA_ERROR("ibv_poll_cq poll failed");
        abort();
      } else if (event_num == 0) {
        break;
      } else {
        if (wc.status != IBV_WC_SUCCESS) {
          RDMA_ERROR("ibv_poll_cq error, {}", QueuePair::WcStatusToString(wc.status));
          abort();
        }
        if (wc.opcode != IBV_WC_SEND && wc.opcode != IBV_WC_RDMA_READ) {
          RDMA_ERROR("all poll event must be send or rdma read, {}", (int)wc.opcode);
          abort();
        }
        BufferDescriptor *bd = (BufferDescriptor*)wc.wr_id;
        RdmaDataHeader *data_header = (RdmaDataHeader*)bd->buffer_;
        if (data_header->data_type == TYPE_SMALL_DATA ||
            data_header->data_type == TYPE_RPC_REQ ||
            data_header->data_type == TYPE_RPC_ACK) {
          RFREE(bd->buffer_, bd->bytes_);
          delete bd;
        } else if (data_header->data_type == TYPE_BIG_DATA) {
          // send a ack to peer
          RdmaRpc *ack = (RdmaRpc*)RMALLOC(sizeof(RdmaRpc));
          memset(ack, 0, sizeof(RdmaRpc));
          ack->data_type = TYPE_RPC_ACK;
          ack->data_id = data_header->data_id;
          ack->data_len = data_header->data_len;

          BufferDescriptor *ack_bd = new BufferDescriptor();
          ack_bd->buffer_ = (uint8_t*)ack;
          ack_bd->bytes_ = sizeof(RdmaRpc);
          ack_bd->mr_ = GET_MR(ack);
          if (qp_->PostSend(ack_bd, 1, BIG_SIGN) < 0) {
            RDMA_ERROR("PostSend ack rpc failed");
            abort();
          }

          RDMA_DEBUG("read big data {}:{}:{}", ip_, data_header->data_id, data_header->data_len);
          {
            std::lock_guard<std::mutex> lock(recv_data_lock_);
            recv_data_[data_header->data_id] = bd;
            if (recv_runing_ == 0) {
              HandleFunction handle = std::bind(&RdmaEvent::HandleRecvDataEvent, this);
              thread_pool_.submit(handle);
              recv_runing_ = 1;
              RDMA_DEBUG("submit a handle recv data thread");
            }
          }
        }
      }
    } while (event_num);
  }
  return 0;
}

// one poll thread
int RdmaEvent::PollRecvCq(int timeout) {
  struct pollfd pfds[2];
  pfds[0].fd = kill_recv_fd_;
  pfds[0].events = POLLIN;
  pfds[0].revents = 0;
  pfds[1].fd = recv_fd_;
  pfds[1].events = POLLIN;
  pfds[1].revents = 0;

  int ret = 0;
  do {
    ret = poll(pfds, 2, -1);
    if (ret == -1) {
      RDMA_ERROR("poll error: {}", strerror(errno));
      return -1;
    }
  } while (ret == 0 || (ret == -1 && errno == EINTR));

  if (pfds[0].revents & POLLIN) {
    RDMA_INFO("kill poll recv thread {}", ip_);
    EventfdConsume(pfds[0].fd);
    close(pfds[0].fd);
    return 1;
  }

  if (pfds[1].revents & POLLIN) {
    ibv_cq *ev_cq;
    void *ev_ctx;
    if (ibv_get_cq_event(recv_cq_channel_, &ev_cq, &ev_ctx) < 0) {
      RDMA_ERROR("ibv_get_cq_event error, {}", strerror(errno));
      abort();
    }
    ibv_ack_cq_events(ev_cq, 1);
    if (ibv_req_notify_cq(ev_cq, 0) < 0) {
      RDMA_ERROR("ibv_req_notify_cq error, {}", strerror(errno));
      abort();
    }

    int event_num;
    do {
      ibv_wc wc;
      event_num = ibv_poll_cq(ev_cq, 1, &wc);
      RDMA_TRACE("ibv_poll_cq recv {}", event_num);

      if (event_num < 0) {
        RDMA_ERROR("ibv_poll_cq poll failed");
        abort();
      } else if (event_num == 0) {
        break;
      } else {
        if (wc.status != IBV_WC_SUCCESS) {
          RDMA_ERROR("ibv_poll_cq error, {}", QueuePair::WcStatusToString(wc.status));
          abort();
        }
        if (wc.opcode != IBV_WC_RECV) {
          RDMA_ERROR("all poll event must be recv");
          abort();
        }
        BufferDescriptor *bd = (BufferDescriptor*)wc.wr_id;
        bd->bytes_ = wc.byte_len;
        RdmaDataHeader *data_header = (RdmaDataHeader*)bd->buffer_;
        if (data_header->data_type == TYPE_SMALL_DATA) {
          RDMA_DEBUG("receive small data {}:{}:{}", ip_, data_header->data_id, data_header->data_len);
          {
            std::lock_guard<std::mutex> lock(recv_data_lock_);
            recv_data_[data_header->data_id] = bd;
            if (recv_runing_ == 0) {
              HandleFunction handle = std::bind(&RdmaEvent::HandleRecvDataEvent, this);
              thread_pool_.submit(handle);
              recv_runing_ = 1;
              RDMA_DEBUG("submit a handle recv data thread");
            }
          }
        } else if (data_header->data_type == TYPE_RPC_REQ) {
          HandleRecvReqRpc(bd);
        } else if (data_header->data_type == TYPE_RPC_ACK) {
          HandleRecvAckRpc(bd);
        } else {
          RDMA_ERROR("unknow recv data header, maybe send error or recv error");
          abort();
        }
      }
    } while (event_num);
  }

  return 0;
}

inline long cost_time(struct timeval& start, struct timeval& end) {
  return (end.tv_sec - start.tv_sec)*1000000 + end.tv_usec - start.tv_usec;
}

void RdmaEvent::HandleRecvDataEvent() {
  RDMA_DEBUG("start a handle recv event thread for {}", ip_);
  while (1) {
    BufferDescriptor *bd = nullptr;

    struct timeval start, end;
    gettimeofday(&start, nullptr);

    uint32_t data_id = recv_data_id_;
    {
      std::lock_guard<std::mutex> lock(recv_data_lock_);
      if (recv_data_.find(data_id) == recv_data_.end()) {
        recv_runing_ = 0;
        RDMA_DEBUG("handle recv event thread sleep");
        return;
      }
      bd = recv_data_[data_id];
      recv_data_.erase(data_id);
    }

    gettimeofday(&end, nullptr);
    long get_lock_time = cost_time(start, end);

    RdmaDataHeader *header = (RdmaDataHeader*)bd->buffer_;
    uint32_t data_len = header->data_len;
    RDMA_DEBUG("handle recv data {}:{}:{}", ip_, data_id, data_len);

    uint8_t *copy_buff = bd->buffer_ + sizeof(RdmaDataHeader);
    int copy_len = data_len - sizeof(RdmaDataHeader);

    gettimeofday(&start, nullptr);
    jbyteArray jba = jni_alloc_byte_array(copy_len);
    gettimeofday(&end, nullptr);
    long alloc_time = cost_time(start, end);
    gettimeofday(&start, nullptr);
    set_byte_array_region(jba, 0, copy_len, copy_buff);
    gettimeofday(&end, nullptr);
    long copy_jvm_time = cost_time(start, end);
    gettimeofday(&start, nullptr);
    jni_channel_callback(ip_.c_str(), jba, copy_len);
    gettimeofday(&end, nullptr);
    long channel_read_time = cost_time(start, end);
    RDMA_INFO("{} alloc copy channleRead {} {} {} {} {} {}",
              ip_, data_id, copy_len, get_lock_time, alloc_time, copy_jvm_time, channel_read_time);
    //RDMA_INFO("recv buffer {}+{}: {}", (char*)copy_buff,(char*)copy_buff+copy_len-6, copy_len);

    if (header->data_type == TYPE_SMALL_DATA) {
      bd->bytes_ = k1KB;
      if (qp_->PostReceiveOneWithBuffer(bd, SMALL_SIGN) < 0) {
        RDMA_ERROR("post recv with reuse data failed");
        abort();
      }
    } else if (header->data_type == TYPE_BIG_DATA) {
      RFREE(bd->buffer_,  bd->bytes_);
      delete bd;
    } else {
      RDMA_ERROR("handle recv data type must be small or big");
      abort();
    }

    recv_data_id_++;
    RDMA_DEBUG("success handle a data_id {} of {}", data_id, ip_);
  }
}



// recv a rpc (data addr rkey size) from peer, and should malloc registed memory
// and read remote memory
// must not reuse bd
void RdmaEvent::HandleRecvReqRpc(BufferDescriptor *bd) {
  RDMA_TRACE("receive a req");
  GPR_ASSERT(sizeof(RdmaRpc) == bd->bytes_);
  RdmaRpc *rpc = (RdmaRpc *)bd->buffer_;

  uint32_t data_len = rpc->data_len;
  uint64_t addr = rpc->addr;
  uint32_t rkey = rpc->rkey;
  uint8_t *buffer = (uint8_t*)RMALLOC(data_len);

  BufferDescriptor *read_bd = new BufferDescriptor();
  memset(read_bd, 0, sizeof(BufferDescriptor));
  read_bd->buffer_ = buffer;
  read_bd->bytes_ = data_len;
  read_bd->mr_ = GET_MR(buffer);

  if (qp_->PostRead(read_bd, 1, addr, rkey) < 0) {
    RDMA_ERROR("PostRead failed");
    abort();
  }

  if (qp_->PostReceiveOneWithBuffer(bd, BIG_SIGN) < 0) {
    RDMA_ERROR("post recv with reuse data failed");
    abort();
  }
}

void RdmaEvent::HandleRecvAckRpc(BufferDescriptor *bd) {
  RDMA_TRACE("receive a ack");
  GPR_ASSERT(sizeof(RdmaRpc) == bd->bytes_);
  RdmaRpc *ack = (RdmaRpc *)bd->buffer_;
  uint32_t data_id = ack->data_id;
  std::pair<BufferDescriptor*, int> free_data = GetDataById(data_id);
  BufferDescriptor *free_bd = free_data.first;
  int num = free_data.second;
  for (int i = 0; i < num; ++i) {
    RFREE(free_bd[i].buffer_, free_bd[i].bytes_);
  }
  delete free_bd;

  if (qp_->PostReceiveOneWithBuffer(bd, BIG_SIGN) < 0) {
    RDMA_ERROR("post recv with reuse data failed");
    abort();
  }
}

std::pair<BufferDescriptor*, int> RdmaEvent::GetDataById(uint32_t id) {
  std::lock_guard<std::mutex> lock(id2data_lock_);
  if (id2data_.find(id) == id2data_.end()) {
    RDMA_ERROR("id2data canot find data_id {}", id);
    return std::pair<BufferDescriptor*, int>(nullptr, 0);
  }
  std::pair<BufferDescriptor*, int> data = id2data_[id];
  id2data_.erase(id);
  return data;
}

void RdmaEvent::PutDataById(uint32_t id, BufferDescriptor *buf, int num) {
  std::lock_guard<std::mutex> lock(id2data_lock_);
  id2data_[id] = std::pair<BufferDescriptor*, int>(buf, num);
}

int test_print(uint8_t *out, int len, char *mark) {
  char print[600];
  memset(print, 0, sizeof(print));
  strcpy(print, mark);
  int vis = (int)strlen(print);
  for (int i = 0; i < len; ++i) {
    sprintf(print+vis, "%02x ", out[i]);
    vis += 3;
  }
  print[vis] = '\0';
  RDMA_DEBUG("{}", print);
  return 0;
}

} // namespace SparkRdmaNetwork

