#ifndef RDMA_API_H
#define RDMA_API_H

#include "constants.h"

/* Connection */
int RDMA_connect(char *servername, PingpongContext **save_ctx);
int RDMA_disconnect(PingpongContext *ctx);

/* Send & Receive */
int send_packet(PingpongContext *ctx);
int receive_packet(PingpongContext *ctx);
int receive_packet_async(PingpongContext *ctx);

/* Read & Write */
int send_RDMA_read(PingpongContext *ctx, char* target, unsigned long size,
                   void* remote_addr, uint32_t remote_key);
int send_RDMA_write(PingpongContext *ctx, const char* value,
                    void* remote_addr, uint32_t remote_key);

/* Fin */
int receive_fin(PingpongContext * ctx);
int send_fin(PingpongContext * ctx);


#endif // RDMA_API_H
