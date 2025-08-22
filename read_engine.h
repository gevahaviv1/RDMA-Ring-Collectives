#ifndef READ_ENGINE_H
#define READ_ENGINE_H

#include <stddef.h>
#include <stdint.h>

struct ibv_qp;

typedef void (*chunk_cb)(int chunk_id, void *ptr, size_t bytes);

int rdma_read_engine(uintptr_t remote_base, uint32_t remote_rkey,
                     uintptr_t local_base, uint32_t local_lkey,
                     size_t total_bytes, size_t chunk_bytes,
                     int inflight_limit, struct ibv_qp *qp,
                     chunk_cb on_chunk_done);

#endif /* READ_ENGINE_H */
