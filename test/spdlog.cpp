#include <spdlog/spdlog.h>
#include <iostream>
#include <memory>

/*#define RDMA_INFO(...) do {\
    SPDLOG_INFO(std::shared_ptr<spdlog::logger>(RdmaLogger::get_rdma_logger()), __VA_ARGS__);\
  } while(0)*/
typedef std::shared_ptr<spdlog::logger> LoggerPtr;
#define RDMA_INFO(...) do {\
    SPDLOG_INFO(RdmaLogger::get_rdma_logger(), __VA_ARGS__);\
  } while(0)

class RdmaLogger {
public:
  static LoggerPtr get_rdma_logger();

private:
  static LoggerPtr rdma_logger_;
  RdmaLogger(spdlog::level::level_enum level);

  // no copy and =
  RdmaLogger(const RdmaLogger&) = delete;
  RdmaLogger&operator=(const RdmaLogger&) = delete;
};

LoggerPtr RdmaLogger::rdma_logger_(nullptr);

RdmaLogger::RdmaLogger(spdlog::level::level_enum level) {
  spdlog::set_pattern("%Y-%m-%d %T.%e %l %t %v");

  rdma_logger_ = spdlog::stderr_logger_mt("rdma");
  rdma_logger_->set_level(level);
  std::cout << rdma_logger_.use_count() << std::endl;
}

LoggerPtr RdmaLogger::get_rdma_logger() {
  if (rdma_logger_.get() == nullptr) {
    new RdmaLogger(spdlog::level::debug);
  }
  return rdma_logger_;
}






int main() {
  /*spdlog::set_pattern("%Y-%m-%d %T.%e %l %t %v");
  auto console = spdlog::stderr_logger_mt("console");
  console->set_level(spdlog::level::trace);
  std::cout << console.use_count() << std::endl;

  console->info("hello world");
  char s[] = "wyb";
  console->error("it is a error {} {}", 32, s);
  SPDLOG_TRACE(console, "Enabled only {} ,{}", 1, 3.23);
  SPDLOG_ERROR(console, "Enabled only {} ,{}", 1, 3.23);

  console->trace("warn {:08d}", 12);
  console->warn("critical {0:d} {0:x} {0:o} {0:b}", 42);
  console->info("info {1} {0}", "first", "second");
  console->info("{:<6}", "left aligned");

  auto my_logger = spdlog::basic_logger_mt("basic_logger", "/tmp/myspdlog.txt");
  my_logger->info("basic log");*/

  RDMA_INFO("hello {}", "world");
  int a = 25342;
  double b = 3232.90902;
  char c[] = "hehawjfoan";
  RDMA_INFO("fuck {} {} {}", a, b, c);
  return 0;
}