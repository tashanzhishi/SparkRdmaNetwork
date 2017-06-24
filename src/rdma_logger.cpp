//
// Created by wyb on 17-5-11.
//

#include "rdma_logger.h"

namespace SparkRdmaNetwork {

  // initilize static class various
  LoggerSharedPtr RdmaLogger::rdma_logger_(nullptr);
  std::mutex RdmaLogger::lock_;

  RdmaLogger::RdmaLogger(spdlog::level::level_enum level) {
    spdlog::set_pattern("%Y-%m-%d %T.%e %l %t %v");

    rdma_logger_ = spdlog::stderr_logger_mt("rdma");
    rdma_logger_->set_level(level);
  }

  LoggerSharedPtr RdmaLogger::get_rdma_logger() {
    if (rdma_logger_.get() == nullptr) {
      std::lock_guard<std::mutex> lock(lock_);
      if (rdma_logger_.get() == nullptr) {
        new RdmaLogger(spdlog::level::trace);
      }
    }
    return rdma_logger_;
  }

}