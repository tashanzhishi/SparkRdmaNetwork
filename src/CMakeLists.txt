cmake_minimum_required(VERSION 2.7)

message(STATUS "start running cmake...")

set(CMAKE_CXX_STANDARD 11)

set(SOURCE_FILES rdma_logger.h rdma_logger.cpp rdma_memory_pool.h rdma_memory_pool.cpp
        rdma_infiniband.cpp rdma_infiniband.h rdma_socket.cpp rdma_socket.h
        rdma_channel.cpp rdma_channel.h rdma_thread.cpp rdma_thread.h rdma_protocol.cpp
        rdma_protocol.h rdma_event.cpp rdma_event.h rdma_server.cpp rdma_server.h jni_common.cpp jni_common.h io_rdma_RdmaServer.cpp io_rdma_RdmaServer.h io_rdma_RdmaChannel.cpp io_rdma_RdmaChannel.h)

add_library(SparkRdma SHARED ${SOURCE_FILES})

# 加入REQUIRED 参数,如果找不到JNI,就报错退出
#set(JAVA_HOME "/home/wyb/opt/java/jdk1.8.0_111")
find_package(JNI REQUIRED)
# 加入jni支持
include_directories(${JAVA_INCLUDE_PATH} ${JAVA_INCLUDE_PATH2})

set(BOOST_ROOT "/usr/include/boost")
include_directories(/usr/include)
link_directories(/usr/lib/x86_64-linux-gnu)