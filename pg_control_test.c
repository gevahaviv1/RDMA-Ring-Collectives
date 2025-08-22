#include "pg.h"
#include <assert.h>
#include <stdio.h>

struct ibv_context { int dummy; };
struct ibv_pd { int dummy; };
struct ibv_cq { int dummy; };
struct ibv_qp { int dummy; };
struct ibv_mr { int dummy; };
int ibv_poll_cq(struct ibv_cq *cq, int num_entries, struct ibv_wc *wc) {
    (void)cq; (void)num_entries; (void)wc; return 0;
}
int ibv_destroy_qp(struct ibv_qp *qp) { (void)qp; return 0; }
int ibv_destroy_cq(struct ibv_cq *cq) { (void)cq; return 0; }
int ibv_dereg_mr(struct ibv_mr *mr) { (void)mr; return 0; }
int ibv_dealloc_pd(struct ibv_pd *pd) { (void)pd; return 0; }
int ibv_close_device(struct ibv_context *ctx) { (void)ctx; return 0; }

int main(void) {
    /* verify packed sizes */
    assert(sizeof(struct pg_msg_rts) == 8);
    assert(sizeof(struct pg_msg_cts) == 16);
    assert(sizeof(struct pg_msg_done) == 4);

    pg_handle handle = {0};
    handle.max_inline_data = 64;
    pg_ctrl_init(&handle);
    /* credits initialized */
    assert(handle.rx_credits[0] == PG_CTRL_RECV_SLOTS);
    assert(handle.rx_credits[1] == PG_CTRL_RECV_SLOTS);

    struct pg_msg_rts msg = {1, 2};
    /* consume all credits */
    for (int i = 0; i < PG_CTRL_RECV_SLOTS; ++i) {
        int rc = pg_ctrl_send(&handle, 0, NULL, &msg, sizeof(msg));
        assert(rc == 0);
    }
    /* no credit left */
    assert(pg_ctrl_send(&handle, 0, NULL, &msg, sizeof(msg)) != 0);
    /* return credit and send again */
    pg_ctrl_return_credit(&handle, 0);
    assert(pg_ctrl_send(&handle, 0, NULL, &msg, sizeof(msg)) == 0);

    /* post_send_inline obeys threshold */
    assert(post_send_inline(&handle, NULL, &msg, sizeof(msg)) == 0);

    printf("Control message tests passed\n");
    return 0;
}
