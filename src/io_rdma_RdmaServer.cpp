//
// Created by wyb on 17-6-5.
//

#include "io_rdma_RdmaServer.h"

#include "rdma_server.h"
#include "jni_common.h"

using namespace SparkRdmaNetwork;

static JavaVM *g_jvm = nullptr;

static jclass RdmaChannelHandler = nullptr;
static jmethodID channelRead0 = nullptr;
static jobject rdmaChannelHandler = nullptr;

static RdmaServer *server = nullptr;

/*
 * Class:     io_rdma_RdmaServer
 * Method:    init
 * Signature: (Ljava/lang/String;ILio/rdma/RdmaChannelHandler;Lio/netty/buffer/PooledByteBufAllocator;)Z
 */
JNIEXPORT jboolean JNICALL Java_io_rdma_RdmaServer_init (
    JNIEnv *env, jobject obj, jstring jhost, jint jport, jobject jrch, jobject jalloc) {
  const char *host = env->GetStringUTFChars(jhost, nullptr);
  int port = jport;

  if (g_jvm == nullptr) {
    env->GetJavaVM(&g_jvm);
  }

  if (RdmaChannelHandler == nullptr) {
    RdmaChannelHandler = env->FindClass("io/rdma/RdmaChannelHandler");
    if (RdmaChannelHandler == nullptr) {
      RDMA_ERROR("find class io.rdma.RdmaChannelHandler failed");
      return 0;
    }
  }

  if (channelRead0 == nullptr) {
    channelRead0 = env->GetMethodID(RdmaChannelHandler, "channelRead0", "(Ljava/lang/String;[BI)V");
    if (channelRead0 == nullptr) {
      RDMA_ERROR("find method channelRead0 failed");
      return 0;
    }
  }

  rdmaChannelHandler = env->NewGlobalRef(jrch);

  server = new RdmaServer();
  if (server->InitServer(host, port) < 0) {
    RDMA_ERROR("InitServer failed");
    return 0;
  }
  return 1;
}

/*
 * Class:     io_rdma_RdmaServer
 * Method:    destroy
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_io_rdma_RdmaServer_destroy
    (JNIEnv *env, jobject jobj) {
  RDMA_INFO("Java_io_rdma_RdmaServer_destroy");
  server->DestroyServer();
}


void jni_channel_callback(const char *remote_host, jobject msg, int len) {
  JNIEnv *env;
  g_jvm->AttachCurrentThread((void **)&env, nullptr);
  jstring host = env->NewStringUTF(remote_host);
  env->CallVoidMethod(rdmaChannelHandler, channelRead0, host, msg, len);
  RDMA_DEBUG("call channelRead0");
  g_jvm->DetachCurrentThread();
}

jbyteArray jni_alloc_byte_array(int bytes) {
  JNIEnv *env;
  g_jvm->AttachCurrentThread((void **)&env, nullptr);
  jbyteArray jba = env->NewByteArray(bytes);
  jbyteArray jba_global = (jbyteArray)env->NewGlobalRef((jobject)jba);
  RDMA_DEBUG("jni alloc byte {}", bytes);
  g_jvm->DetachCurrentThread();
  return jba_global;
}

void set_byte_array_region(jbyteArray jba, int pos, int len, unsigned char *buf) {
  JNIEnv *env;
  g_jvm->AttachCurrentThread((void **)&env, nullptr);

  env->SetByteArrayRegion(jba, pos, len, (jbyte *)buf);

  g_jvm->DetachCurrentThread();
}