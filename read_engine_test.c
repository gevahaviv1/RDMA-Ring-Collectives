#include "read_engine.h"
#include <assert.h>
#include <stdio.h>
#include <string.h>

enum ibv_wc_status { IBV_WC_SUCCESS = 0 };

struct ibv_wc {
    uint64_t wr_id;
    enum ibv_wc_status status;
};

struct ibv_cq {
    struct ibv_wc comps[16];
    int head;
    int tail;
};

struct ibv_qp {
    struct ibv_cq *send_cq;
};

struct ibv_sge {
    uintptr_t addr;
    uint32_t length;
    uint32_t lkey;
};

enum ibv_wr_opcode { IBV_WR_RDMA_READ = 0 };

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

static struct ibv_cq global_cq;

int ibv_post_send(struct ibv_qp *qp, struct ibv_send_wr *wr,
                  struct ibv_send_wr **bad_wr) {
    (void)bad_wr;
    (void)qp;
    struct ibv_sge *sge = wr->sg_list;
    memcpy((void *)sge->addr, (void *)wr->wr.rdma.remote_addr, sge->length);
    int idx = global_cq.tail++ % 16;
    global_cq.comps[idx].wr_id = wr->wr_id;
    global_cq.comps[idx].status = IBV_WC_SUCCESS;
    return 0;
}

int ibv_poll_cq(struct ibv_cq *cq, int num_entries, struct ibv_wc *wc) {
    int n = 0;
    while (n < num_entries && cq->head < cq->tail) {
        wc[n] = cq->comps[cq->head % 16];
        cq->head++;
        n++;
    }
    return n;
}

static unsigned callback_count = 0;
static char *remote_buf;
static size_t chunk_size_global;

static void on_chunk(int chunk_id, void *ptr, size_t bytes) {
    callback_count++;
    assert(bytes <= chunk_size_global);
    assert(memcmp(ptr, remote_buf + chunk_id * chunk_size_global, bytes) == 0);
}

int main(void) {
    const size_t total_bytes = 1024;
    const size_t chunk_bytes = 128;
    const int inflight_limit = 4;
    char local_buf[total_bytes];
    char remote[total_bytes];
    for (size_t i = 0; i < total_bytes; ++i)
        remote[i] = (char)i;
    memset(local_buf, 0, sizeof(local_buf));
    remote_buf = remote;
    chunk_size_global = chunk_bytes;

    global_cq.head = global_cq.tail = 0;
    struct ibv_qp qp = { .send_cq = &global_cq };

    int rc = rdma_read_engine((uintptr_t)remote, 0, (uintptr_t)local_buf, 0,
                              total_bytes, chunk_bytes, inflight_limit, &qp,
                              on_chunk);
    assert(rc == 0);
    assert(callback_count == (total_bytes + chunk_bytes - 1) / chunk_bytes);
    assert(memcmp(local_buf, remote, total_bytes) == 0);
    printf("Read engine test passed\n");
    return 0;
}

