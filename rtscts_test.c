#include "read_engine.h"
#include "rtscts.h"
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

static unsigned cb_count = 0;
static char *remote_ptr;
static size_t chunk_bytes_g;
static int base_chunk = 0;

static void on_chunk(int chunk_id, void *ptr, size_t bytes) {
    cb_count++;
    int abs_chunk = base_chunk + chunk_id;
    assert(bytes <= chunk_bytes_g);
    assert(memcmp(ptr, remote_ptr + abs_chunk * chunk_bytes_g, bytes) == 0);
}

int main(void) {
    const size_t total_bytes = 512;
    const size_t chunk_bytes = 128;
    const int num_chunks = (total_bytes + chunk_bytes - 1) / chunk_bytes;

    char right_buf[total_bytes];
    char left_buf[total_bytes];
    for (size_t i = 0; i < total_bytes; ++i)
        right_buf[i] = (char)i;
    memset(left_buf, 0, sizeof(left_buf));
    remote_ptr = right_buf;
    chunk_bytes_g = chunk_bytes;

    struct ctrl_queue to_left, to_right;
    ctrl_queue_init(&to_left);
    ctrl_queue_init(&to_right);

    global_cq.head = global_cq.tail = 0;
    struct ibv_qp qp = { .send_cq = &global_cq };

    for (int chunk = 0; chunk < num_chunks; ++chunk) {
        size_t bytes = chunk_bytes;
        if ((size_t)(chunk + 1) * chunk_bytes > total_bytes)
            bytes = total_bytes - (size_t)chunk * chunk_bytes;

        /* right sender -> left receiver */
        send_rts(&to_left, chunk, bytes);

        int r_chunk; size_t r_bytes;
        assert(recv_rts(&to_left, &r_chunk, &r_bytes));
        assert(r_chunk == chunk && r_bytes == bytes);

        size_t dst_off = (size_t)chunk * chunk_bytes;
        send_cts(&to_right, chunk, dst_off, bytes);

        int c_chunk; size_t c_off; size_t c_bytes;
        assert(recv_cts(&to_right, &c_chunk, &c_off, &c_bytes));
        assert(c_chunk == chunk && c_off == dst_off && c_bytes == bytes);

        base_chunk = chunk;
        int rc = rdma_read_engine((uintptr_t)(right_buf + (size_t)chunk * chunk_bytes), 0,
                                  (uintptr_t)(left_buf + dst_off), 0,
                                  bytes, bytes, 1, &qp, on_chunk);
        assert(rc == 0);

        send_done(&to_right, chunk);
        int d_chunk;
        assert(recv_done(&to_right, &d_chunk));
        assert(d_chunk == chunk);
    }

    assert(cb_count == (unsigned)num_chunks);
    assert(memcmp(left_buf, right_buf, total_bytes) == 0);
    printf("RTS/CTS test passed\n");
    return 0;
}

