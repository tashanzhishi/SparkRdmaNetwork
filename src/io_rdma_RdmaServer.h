/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class io_rdma_RdmaServer */

#ifndef _Included_io_rdma_RdmaServer
#define _Included_io_rdma_RdmaServer
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:     io_rdma_RdmaServer
 * Method:    init
 * Signature: (Ljava/lang/String;ILio/rdma/RdmaChannelHandler;Lio/netty/buffer/PooledByteBufAllocator;)Z
 */
JNIEXPORT jboolean JNICALL Java_io_rdma_RdmaServer_init
    (JNIEnv *, jobject, jstring, jint, jobject, jobject);

/*
 * Class:     io_rdma_RdmaServer
 * Method:    destroy
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_io_rdma_RdmaServer_destroy
    (JNIEnv *, jobject);

#ifdef __cplusplus
}
#endif
#endif