//
// Created by wyb on 17-6-5.
//

#include "io_rdma_RdmaChannel.h"

#include <string>
#include <cstring>

#include <map>
#include <boost/thread/shared_mutex.hpp>

#include "rdma_channel.h"

using namespace SparkRdmaNetwork;

//static std::map<jobject, int> channe_set;
//int idid = 0;
//static boost::shared_mutex set_lock;

/*
 * Class:     io_rdma_RdmaChannel
 * Method:    init
 * Signature: (Ljava/lang/String;I)V
 */
JNIEXPORT void JNICALL Java_io_rdma_RdmaChannel_init
    (JNIEnv *env, jobject jobj, jstring jhost, jint jport) {
  const char *host = env->GetStringUTFChars(jhost, 0);
  // test
  /*int hehe = 0;
  {
    ReadLock lock(set_lock);
    if (channe_set.find(jobj) == channe_set.end())
      hehe = 1;
  }
  if (hehe == 1) {
    WriteLock lock(set_lock);
    if (channe_set.find(jobj) == channe_set.end()) {
      channe_set[jobj] = idid++;
      RDMA_INFO("--------------- {} {}", idid, host);
    }
  }
  RDMA_INFO("init RdmaChannel {}", host);*/
  // test
  int port = jport;
  if (port == 0) {
    port = kDefaultPort;
  }
  std::string ip = RdmaSocket::GetIpByHost(host);
  RdmaChannel *channel = RdmaChannel::GetChannelByIp(ip);
  if (channel == nullptr) {
    channel = new RdmaChannel(host, port);
  }
  channel->Init(host, port);
}

/*
 * Class:     io_rdma_RdmaChannel
 * Method:    sendHeader
 * Signature: (Ljava/lang/String;ILjava/nio/ByteBuffer;ILio/rdma/RdmaSendCallback;)V
 */
JNIEXPORT void JNICALL Java_io_rdma_RdmaChannel_sendHeader
    (JNIEnv *env, jobject jobj, jstring jhost, jint jport, jobject jmsg, jint jlen, jobject jsend_cb) {
  const char *host = env->GetStringUTFChars(jhost, 0);
  int port = jport;
  if (port == 0) {
    port = kDefaultPort;
  }

  std::string ip = RdmaSocket::GetIpByHost(host);
  RdmaChannel *channel = RdmaChannel::GetChannelByIp(ip);
  GPR_ASSERT(channel != nullptr);

  static jclass ByteBuffer = env->FindClass("java/nio/ByteBuffer");
  if (ByteBuffer == nullptr) {
    RDMA_ERROR("find class java.nio.ByteBuffer failed");
    return;
  }
  // ByteBuffer.array()
  static jmethodID array = env->GetMethodID(ByteBuffer, "array", "()[B");
  if (array == nullptr) {
    RDMA_ERROR("find ByteBuffer method array failed");
    return;
  }
  // ByteBuffer.position()
  static jmethodID position = env->GetMethodID(ByteBuffer, "position", "()I");
  if (position == nullptr) {
    RDMA_ERROR("find ByteBuffer method position failed");
    return;
  }

  int msg_len = jlen;
  uint32_t data_len = msg_len + sizeof(RdmaDataHeader);
  uint8_t *rdma_data = (uint8_t*)RMALLOC(data_len);
  uint8_t *rdma_msg = rdma_data + sizeof(RdmaDataHeader);

  uint8_t *msg = (uint8_t *)env->GetDirectBufferAddress(jmsg);
  // msg is not DirectBuffer, so should copy to native
  if (msg == nullptr) {
    jbyteArray byte_array = (jbyteArray)env->CallObjectMethod(jmsg, array);
    if (byte_array == nullptr) {
      RDMA_ERROR("call ByteBuffer.array() failed");
      return;
    }
    int pos = env->CallIntMethod(jmsg, position);
    // copy data from java to rdma
    env->GetByteArrayRegion(byte_array, pos, msg_len, (jbyte*)rdma_msg);
  } else {
    // msg is DirectBuffer, but now copy it still
    RDMA_DEBUG("msg is direct buffer");
    memcpy(rdma_msg, msg, msg_len);
  }
  channel->SendMsg(host, port, rdma_data, data_len);
}

/*
 * Class:     io_rdma_RdmaChannel
 * Method:    sendHeaderWithBody
 * Signature: (Ljava/lang/String;ILjava/nio/ByteBuffer;ILjava/nio/ByteBuffer;JLio/rdma/RdmaSendCallback;)V
 */
JNIEXPORT void JNICALL Java_io_rdma_RdmaChannel_sendHeaderWithBody
    (JNIEnv *env, jobject jobj, jstring jhost, jint jport, jobject jheader, jint jhlen,
     jobject jbody, jlong jblen, jobject jsend_cb) {
  const char *host = env->GetStringUTFChars(jhost, 0);
  int port = jport;
  if (port == 0) {
    port = kDefaultPort;
  }

  std::string ip = RdmaSocket::GetIpByHost(host);
  RdmaChannel *channel = RdmaChannel::GetChannelByIp(ip);
  GPR_ASSERT(channel != nullptr);

  static jclass ByteBuffer = env->FindClass("java/nio/ByteBuffer");
  if (ByteBuffer == nullptr) {
    RDMA_ERROR("find class java.nio.ByteBuffer failed");
    return;
  }
  // ByteBuffer.array()
  static jmethodID array = env->GetMethodID(ByteBuffer, "array", "()[B");
  if (array == nullptr) {
    RDMA_ERROR("find ByteBuffer.array() failed");
    return;
  }
  // ByteBuffer.position()
  static jmethodID position = env->GetMethodID(ByteBuffer, "position", "()I");
  if (position == nullptr) {
    RDMA_ERROR("find ByteBuffer.position() failed");
    return;
  }

  int hlen = jhlen, blen = jblen;
  uint32_t data_len = hlen + blen + sizeof(RdmaDataHeader);
  uint8_t *rdma_data = (uint8_t*)RMALLOC(data_len);
  uint8_t *rdma_header = rdma_data + sizeof(RdmaDataHeader);
  uint8_t *rdma_body = rdma_data + sizeof(RdmaDataHeader) + hlen;

  uint8_t *header = (uint8_t *)env->GetDirectBufferAddress(jheader);
  if (header == nullptr) {
    jbyteArray byte_array = (jbyteArray)env->CallObjectMethod(jheader, array);
    if (byte_array == nullptr) {
      RDMA_ERROR("call ByteBuffer.array() failed");
      return;
    }
    int pos = env->CallIntMethod(jheader, position);
    // copy data from java to rdma
    env->GetByteArrayRegion(byte_array, pos, hlen, (jbyte*)rdma_header);
  } else {
    memcpy(rdma_header, header, hlen);
  }

  uint8_t *body = (uint8_t *)env->GetDirectBufferAddress(jbody);
  if (body == nullptr) {
    jbyteArray byte_array = (jbyteArray)env->CallObjectMethod(jbody, array);
    if (byte_array == nullptr) {
      RDMA_ERROR("call ByteBuffer.array() failed");
      return;
    }
    int pos = env->CallIntMethod(jbody, position);
    // copy data from java to rdma
    env->GetByteArrayRegion(byte_array, pos, blen, (jbyte*)rdma_body);
  } else {
    memcpy(rdma_body, body, blen);
  }

  channel->SendMsg(host, port, rdma_data, data_len);
}
