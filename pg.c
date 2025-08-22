#include "pg.h"

#include <assert.h>

/* Forward declarations for RDMA structures to avoid external dependencies */
struct ibv_context;
struct ibv_pd;
struct ibv_cq;
struct ibv_qp;
struct ibv_mr;


int pg_rank(const pg_handle *handle) {
    return handle ? handle->rank : -1;
}

int pg_world_size(const pg_handle *handle) {
    return handle ? handle->world_size : -1;
}

void pg_ctrl_init(pg_handle *handle) {
    if (!handle)
        return;
    for (int i = 0; i < 2; ++i) {
        handle->ctrl_head[i] = 0;
        handle->ctrl_tail[i] = 0;
        handle->rx_credits[i] = PG_CTRL_RECV_SLOTS;
    }
}

union pg_ctrl_msg *pg_ctrl_next_recv(pg_handle *handle, int peer) {
    if (!handle || peer < 0 || peer >= 2)
        return NULL;
    int idx = handle->ctrl_head[peer];
    union pg_ctrl_msg *slot = &handle->ctrl_recv_bufs[peer][idx];
    handle->ctrl_head[peer] = (idx + 1) % PG_CTRL_RECV_SLOTS;
    return slot;
}

int post_send_inline(pg_handle *handle, struct ibv_qp *qp,
                     const void *msg, size_t len) {
    (void)qp;
    (void)msg;
    assert(handle);
    assert(len <= handle->max_inline_data);
    return 0;
}

int pg_ctrl_send(pg_handle *handle, int peer, struct ibv_qp *qp,
                 const void *msg, size_t len) {
    if (!handle || peer < 0 || peer >= 2)
        return -1;
    if (handle->rx_credits[peer] <= 0)
        return -1; /* no credits */
    int rc = post_send_inline(handle, qp, msg, len);
    if (rc == 0)
        handle->rx_credits[peer]--;
    return rc;
}

void pg_ctrl_return_credit(pg_handle *handle, int peer) {
    if (!handle || peer < 0 || peer >= 2)
        return;
    handle->rx_credits[peer]++;
}
