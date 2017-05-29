//
// Created by wyb on 17-5-19.
//

#ifndef SPARKRDMA_RDMA_THREAD_H
#define SPARKRDMA_RDMA_THREAD_H

#include <boost/thread.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <boost/thread/thread_pool.hpp>


typedef boost::shared_lock<boost::shared_mutex> ReadLock;
typedef boost::unique_lock<boost::shared_mutex> WriteLock;

namespace SparkRdmaNetwork {

class RdmaThreadPool {

};


} // namespace SparkRdmaNetwork

#endif //SPARKRDMA_RDMA_THREAD_H
