#include "pg.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/time.h>

/* Forward declarations for RDMA structures to avoid external dependencies */
struct ibv_context;
struct ibv_pd;
struct ibv_cq;
struct ibv_qp;
struct ibv_mr;

int ibv_poll_cq(struct ibv_cq *cq, int num_entries, struct ibv_wc *wc);


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

static const char *wc_status_str(enum ibv_wc_status status) {
    switch (status) {
    case IBV_WC_SUCCESS:
        return "IBV_WC_SUCCESS";
    case IBV_WC_LOC_LEN_ERR:
        return "IBV_WC_LOC_LEN_ERR";
    case IBV_WC_LOC_QP_OP_ERR:
        return "IBV_WC_LOC_QP_OP_ERR";
    case IBV_WC_LOC_EEC_OP_ERR:
        return "IBV_WC_LOC_EEC_OP_ERR";
    case IBV_WC_LOC_PROT_ERR:
        return "IBV_WC_LOC_PROT_ERR";
    case IBV_WC_WR_FLUSH_ERR:
        return "IBV_WC_WR_FLUSH_ERR";
    case IBV_WC_MW_BIND_ERR:
        return "IBV_WC_MW_BIND_ERR";
    case IBV_WC_BAD_RESP_ERR:
        return "IBV_WC_BAD_RESP_ERR";
    case IBV_WC_LOC_ACCESS_ERR:
        return "IBV_WC_LOC_ACCESS_ERR";
    case IBV_WC_REM_INV_REQ_ERR:
        return "IBV_WC_REM_INV_REQ_ERR";
    case IBV_WC_REM_ACCESS_ERR:
        return "IBV_WC_REM_ACCESS_ERR";
    case IBV_WC_REM_OP_ERR:
        return "IBV_WC_REM_OP_ERR";
    case IBV_WC_RETRY_EXC_ERR:
        return "IBV_WC_RETRY_EXC_ERR";
    case IBV_WC_RNR_RETRY_EXC_ERR:
        return "IBV_WC_RNR_RETRY_EXC_ERR";
    case IBV_WC_LOC_RDD_VIOL_ERR:
        return "IBV_WC_LOC_RDD_VIOL_ERR";
    case IBV_WC_REM_INV_RD_REQ_ERR:
        return "IBV_WC_REM_INV_RD_REQ_ERR";
    case IBV_WC_REM_ABORT_ERR:
        return "IBV_WC_REM_ABORT_ERR";
    case IBV_WC_INV_EECN_ERR:
        return "IBV_WC_INV_EECN_ERR";
    case IBV_WC_INV_EEC_STATE_ERR:
        return "IBV_WC_INV_EEC_STATE_ERR";
    case IBV_WC_FATAL_ERR:
        return "IBV_WC_FATAL_ERR";
    case IBV_WC_RESP_TIMEOUT_ERR:
        return "IBV_WC_RESP_TIMEOUT_ERR";
    case IBV_WC_GENERAL_ERR:
        return "IBV_WC_GENERAL_ERR";
    default:
        return "IBV_WC_UNKNOWN";
    }
}

int poll_cq_until(struct ibv_cq *cq, int min_n, int timeout_ms,
                  struct ibv_wc **wcs_out) {
    if (!cq || !wcs_out || min_n <= 0 || timeout_ms < 0)
        return -1;
    *wcs_out = NULL;

    int capacity = min_n;
    struct ibv_wc *wcs = malloc(sizeof(*wcs) * (size_t)capacity);
    if (!wcs)
        return -1;

    struct timeval start;
    gettimeofday(&start, NULL);
    int total = 0;

    while (total < min_n) {
        int rc = ibv_poll_cq(cq, capacity - total, &wcs[total]);
        if (rc < 0) {
            free(wcs);
            return -1;
        }
        for (int i = total; i < total + rc; ++i) {
            if (wcs[i].status != IBV_WC_SUCCESS) {
                fprintf(stderr, "WC error: %s (%d) wr_id=%llu\n",
                        wc_status_str(wcs[i].status),
                        (int)wcs[i].status,
                        (unsigned long long)wcs[i].wr_id);
            }
        }
        total += rc;
        if (total >= min_n)
            break;

        struct timeval now;
        gettimeofday(&now, NULL);
        long elapsed_ms = (now.tv_sec - start.tv_sec) * 1000L +
                          (now.tv_usec - start.tv_usec) / 1000L;
        if (elapsed_ms >= timeout_ms)
            break;
    }

    if (total == 0) {
        free(wcs);
    } else {
        *wcs_out = wcs;
    }
    return total;
}
