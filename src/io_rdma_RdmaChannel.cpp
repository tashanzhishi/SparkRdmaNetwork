//
// Created by wyb on 17-6-5.
//

#include "io_rdma_RdmaChannel.h"

#include <string>
#include <cstring>

#include "rdma_channel.h"

using namespace SparkRdmaNetwork;

/*
 * Class:     io_rdma_RdmaChannel
 * Method:    init
 * Signature: (Ljava/lang/String;I)V
 */
JNIEXPORT void JNICALL Java_io_rdma_RdmaChannel_init
    (JNIEnv *env, jobject jobj, jstring jhost, jint jport) {
  const char *host = env->GetStringUTFChars(jhost, 0);
  int port = jport;
  std::string ip = RdmaChannel::get_ip_from_host(std::string(host));
  RdmaChannel *channel = RdmaChannel::get_channel_from_ip(ip);
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
  std::string ip = RdmaChannel::get_ip_from_host(std::string(host));
  RdmaChannel *channel = RdmaChannel::get_channel_from_ip(ip);

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

  int len = jlen;
  uint8_t *rdma_msg = nullptr;

  uint8_t *msg = (uint8_t *)env->GetDirectBufferAddress(jmsg);
  // msg is not DirectBuffer, so should copy to native
  if (msg == nullptr) {
    jbyteArray byte_array = (jbyteArray)env->CallObjectMethod(jmsg, array);
    if (byte_array == nullptr) {
      RDMA_ERROR("call ByteBuffer.array() failed");
      return;
    }
    int pos = env->CallIntMethod(jmsg, position);
    RDMA_DEBUG("position = {}", pos);

    rdma_msg = (uint8_t*)RMALLOC(len);
    // copy data from java to rdma
    env->GetByteArrayRegion(byte_array, pos, len, (jbyte*)rdma_msg);
  } else {
    // msg is DirectBuffer, but now copy it still
    rdma_msg = (uint8_t*)RMALLOC(len);
    memcpy(rdma_msg, msg, len);
    free(msg);
  }
  channel->SendMsg(host, port, rdma_msg, len);
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
  std::string ip = RdmaChannel::get_ip_from_host(std::string(host));
  RdmaChannel *channel = RdmaChannel::get_channel_from_ip(ip);

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

  int hlen = jhlen;
  int blen = jblen;
  uint8_t *rdma_header = nullptr, *rdma_body = nullptr;

  uint8_t *header = (uint8_t *)env->GetDirectBufferAddress(jheader);
  if (header == nullptr) {
    jbyteArray byte_array = (jbyteArray)env->CallObjectMethod(jheader, array);
    if (byte_array == nullptr) {
      RDMA_ERROR("call ByteBuffer.array() failed");
      return;
    }
    int pos = env->CallIntMethod(jheader, position);
    RDMA_DEBUG("position = {}", pos);

    rdma_header = (uint8_t*)RMALLOC(hlen);
    // copy data from java to rdma
    env->GetByteArrayRegion(byte_array, pos, hlen, (jbyte*)rdma_header);
  } else {
    rdma_header = (uint8_t*)RMALLOC(hlen);
    memcpy(rdma_header, header, hlen);
    free(header);
  }
  uint8_t *body = (uint8_t *)env->GetDirectBufferAddress(jbody);
  if (body == nullptr) {
    jbyteArray byte_array = (jbyteArray)env->CallObjectMethod(jbody, array);
    if (byte_array == nullptr) {
      RDMA_ERROR("call ByteBuffer.array() failed");
      return;
    }
    int pos = env->CallIntMethod(jbody, position);
    RDMA_DEBUG("position = {}", pos);

    rdma_body = (uint8_t*)RMALLOC(blen);
    // copy data from java to rdma
    env->GetByteArrayRegion(byte_array, pos, blen, (jbyte*)rdma_body);
  } else {
    rdma_body = (uint8_t*)RMALLOC(blen);
    memcpy(rdma_body, header, hlen);
    free(header);
  }

  channel->SendMsgWithHeader(host, port, rdma_header, hlen, rdma_body, blen);
}
