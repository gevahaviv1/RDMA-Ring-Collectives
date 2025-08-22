#include "read_engine.h"
#include "pg_internal.h"

#include <stdio.h>
#include <string.h>

struct ibv_cq {
  int dummy;
};

struct ibv_qp {
  struct ibv_cq *send_cq;
};

struct ibv_sge {
  uintptr_t addr;
  uint32_t length;
  uint32_t lkey;
};

enum ibv_wr_opcode {
  IBV_WR_RDMA_READ = 0,
};

struct ibv_send_wr {
  uint64_t wr_id;
  struct ibv_send_wr *next;
  struct ibv_sge *sg_list;
  int num_sge;
  enum ibv_wr_opcode opcode;
  int send_flags;
  struct {
    struct {
      uintptr_t remote_addr;
      uint32_t rkey;
    } rdma;
  } wr;
};

int ibv_post_send(struct ibv_qp *qp, struct ibv_send_wr *wr,
                  struct ibv_send_wr **bad_wr);
int ibv_poll_cq(struct ibv_cq *cq, int num_entries, struct ibv_wc *wc);

int rdma_read_engine(uintptr_t remote_base, uint32_t remote_rkey,
                     uintptr_t local_base, uint32_t local_lkey,
                     size_t total_bytes, size_t chunk_bytes, int inflight_limit,
                     struct ibv_qp *qp, chunk_cb on_chunk_done) {
  if (!qp || !qp->send_cq || !on_chunk_done)
    return -1;
  if (chunk_bytes == 0 || inflight_limit <= 0)
    return -1;

  size_t num_chunks = (total_bytes + chunk_bytes - 1) / chunk_bytes;
  size_t posted = 0;
  size_t completed = 0;
  size_t inflight = 0;

  while (completed < num_chunks) {
    while (inflight < (size_t)inflight_limit && posted < num_chunks) {
      size_t off = posted * chunk_bytes;
      size_t bytes = chunk_bytes;
      if (off + bytes > total_bytes)
        bytes = total_bytes - off;

      struct ibv_sge sge = {
          .addr = local_base + off,
          .length = (uint32_t)bytes,
          .lkey = local_lkey,
      };
      struct ibv_send_wr wr = {
          .wr_id = posted,
          .next = NULL,
          .sg_list = &sge,
          .num_sge = 1,
          .opcode = IBV_WR_RDMA_READ,
          .send_flags = 0,
          .wr = {.rdma = {remote_base + off, remote_rkey}},
      };
      struct ibv_send_wr *bad = NULL;
      int rc = ibv_post_send(qp, &wr, &bad);
      if (rc) {
        fprintf(stderr, "ibv_post_send failed: %d\n", rc);
        return -1;
      }
      ++posted;
      ++inflight;
    }

    struct ibv_wc wc;
    int rc = ibv_poll_cq(qp->send_cq, 1, &wc);
    if (rc < 0) {
      fprintf(stderr, "ibv_poll_cq failed: %d\n", rc);
      return -1;
    }
    if (rc == 0)
      continue;
    if (wc.status != IBV_WC_SUCCESS) {
      PG_CHECK_WC(&wc);
      return -1;
    }
    size_t chunk_id = (size_t)wc.wr_id;
    size_t off = chunk_id * chunk_bytes;
    size_t bytes = chunk_bytes;
    if (off + bytes > total_bytes)
      bytes = total_bytes - off;
    on_chunk_done((int)chunk_id, (void *)(local_base + off), bytes);
    ++completed;
    --inflight;
  }
  return 0;
}
