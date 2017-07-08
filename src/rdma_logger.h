//
// Created by wyb on 17-5-11.
//

#ifndef SPARKRDMA_RDMA_LOG_H
#define SPARKRDMA_RDMA_LOG_H

#include <spdlog/spdlog.h>
#include <memory>
#include <mutex>
#include <cstring>
#include <cerrno>


#define RDMA_TRACE(...) do { \
    SPDLOG_TRACE(SparkRdmaNetwork::RdmaLogger::get_rdma_logger(), __VA_ARGS__); \
  } while (0)

#define RDMA_INFO(...) do { \
    SPDLOG_INFO(SparkRdmaNetwork::RdmaLogger::get_rdma_logger(), __VA_ARGS__); \
  } while (0)

#define RDMA_DEBUG(...) do { \
    SPDLOG_DEBUG(SparkRdmaNetwork::RdmaLogger::get_rdma_logger(), __VA_ARGS__); \
  } while (0)

#define RDMA_ERROR(...) do { \
    SPDLOG_ERROR(SparkRdmaNetwork::RdmaLogger::get_rdma_logger(), __VA_ARGS__); \
  } while (0)

#define GPR_ASSERT(x)                         \
  do {                                        \
    if (!(x)) {                               \
      RDMA_ERROR("assertion failed: {}", #x); \
      abort();                                \
    }                                         \
  } while(0)

typedef std::shared_ptr<spdlog::logger> LoggerSharedPtr;
typedef spdlog::level::level_enum LoggerLevel;

namespace SparkRdmaNetwork {

  // this is a singleton instance class
  class RdmaLogger {
  public:
    static LoggerSharedPtr& get_rdma_logger() {
      static RdmaLogger logger(spdlog::level::info);
      return logger.rdma_logger_;
    }

  private:
    RdmaLogger(LoggerLevel level) {
      spdlog::set_pattern("%Y-%m-%d %T.%e %l %t %v");
      rdma_logger_ = spdlog::stderr_logger_mt("rdma");
      rdma_logger_->set_level(level);
    }

    LoggerSharedPtr rdma_logger_;

    // no copy and =
    RdmaLogger(const RdmaLogger&) = delete;
    RdmaLogger& operator=(const RdmaLogger&) = delete;
  };

}

#endif //SPARKRDMA_RDMA_LOG_H
