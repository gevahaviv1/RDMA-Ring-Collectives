#include "pg.h"
#include <assert.h>
#include <stdio.h>
#include <string.h>

struct ibv_context { int dummy; };
struct ibv_pd { int dummy; };
struct ibv_cq { int dummy; };
struct ibv_qp { int dummy; };
struct ibv_mr { int dummy; };
int ibv_poll_cq(struct ibv_cq *cq, int num_entries, struct ibv_wc *wc) {
    (void)cq; (void)num_entries;
    wc[0].status = IBV_WC_SUCCESS;
    wc[0].wr_id = 0;
    return 1;
}
int ibv_destroy_qp(struct ibv_qp *qp) { (void)qp; return 0; }
int ibv_destroy_cq(struct ibv_cq *cq) { (void)cq; return 0; }
int ibv_dereg_mr(struct ibv_mr *mr) { (void)mr; return 0; }
int ibv_dealloc_pd(struct ibv_pd *pd) { (void)pd; return 0; }
int ibv_close_device(struct ibv_context *ctx) { (void)ctx; return 0; }

int main(void) {
    pg_handle handle = {0};
    handle.max_inline_data = 64;
    handle.cq = (struct ibv_cq*)0x1;
    handle.qps[1] = (struct ibv_qp*)0x1;

    int32_t sendbuf[4] = {1,2,3,4};
    int32_t recvbuf[4] = {10,20,30,40};
    size_t bytes = sizeof(sendbuf);
    size_t eager_bytes = 64;

    int rc = pg_sendrecv_inline(&handle, sendbuf, recvbuf, bytes, eager_bytes, DT_INT32, OP_SUM);
    assert(rc == 0);
    assert(recvbuf[0]==11 && recvbuf[1]==22 && recvbuf[2]==33 && recvbuf[3]==44);
    printf("pg_sendrecv_inline test passed\n");
    return 0;
}
