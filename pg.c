#include <stdlib.h>
#include <string.h>
#include <infiniband/verbs.h>
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
                     void *msg, size_t len) {
    (void)qp;
    (void)msg;
    assert(handle);
    assert(len <= handle->max_inline_data);
    return 0;
}

int pg_ctrl_send(pg_handle *handle, int peer, struct ibv_qp *qp,
                 void *msg, size_t len) {
    if (!handle || peer < 0 || peer >= 2)
        return -1;
    if (handle->rx_credits[peer] <= 0)
        return -1;
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

    struct ibv_wc *wcs = malloc(sizeof(*wcs) * (size_t)min_n);
    if (!wcs)
        return -1;

    struct timeval start;
    gettimeofday(&start, NULL);
    int total = 0;

    while (total < min_n) {
        int rc = ibv_poll_cq(cq, min_n - total, &wcs[total]);
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

pg_handle *pg_create(int rank, int world_size, size_t chunk_bytes,
                     int inflight_limit) {
    pg_handle *handle = calloc(1, sizeof(*handle));
    if (!handle)
        return NULL;

    handle->rank = rank;
    handle->world_size = world_size;
    handle->left_index = left_of(rank, world_size);
    handle->right_index = right_of(rank, world_size);
    handle->chunk_bytes = chunk_bytes ? chunk_bytes : PG_DEFAULT_CHUNK_BYTES;
    handle->inflight_limit = inflight_limit > 0 ? inflight_limit
                                                : PG_DEFAULT_INFLIGHT_LIMIT;

    struct ibv_device **dev_list = ibv_get_device_list(NULL);
    if (!dev_list)
        goto err_free;

    handle->ctx = ibv_open_device(dev_list[0]);
    ibv_free_device_list(dev_list);
    if (!handle->ctx)
        goto err_free;

    handle->pd = ibv_alloc_pd(handle->ctx);
    if (!handle->pd)
        goto err_ctx;

    int cq_entries = 1 + handle->inflight_limit * 2;
    handle->cq = ibv_create_cq(handle->ctx, cq_entries, NULL, NULL, 0);
    if (!handle->cq)
        goto err_pd;

    struct ibv_qp_init_attr qp_attr = {
        .send_cq = handle->cq,
        .recv_cq = handle->cq,
        .cap = {
            .max_send_wr = handle->inflight_limit + 1,
            .max_recv_wr = handle->inflight_limit + 1,
            .max_send_sge = 1,
            .max_recv_sge = 1,
            .max_inline_data = 0,
        },
        .qp_type = IBV_QPT_RC,
        .sq_sig_all = 0,
    };

    for (int i = 0; i < 2; ++i) {
        handle->qps[i] = ibv_create_qp(handle->pd, &qp_attr);
        if (!handle->qps[i]) {
            for (int j = 0; j < i; ++j)
                ibv_destroy_qp(handle->qps[j]);
            goto err_cq;
        }
    }

    struct ibv_qp_attr tmp_attr;
    struct ibv_qp_init_attr tmp_init;
    if (ibv_query_qp(handle->qps[0], &tmp_attr, IBV_QP_CAP, &tmp_init))
        goto err_qp;
    handle->max_inline_data = tmp_init.cap.max_inline_data;

    handle->data_bytes = handle->chunk_bytes * handle->inflight_limit;
    for (int i = 0; i < 2; ++i) {
        handle->data_bufs[i] = calloc(1, handle->data_bytes);
        if (!handle->data_bufs[i])
            goto err_mr;
        handle->data_mrs[i] =
            ibv_reg_mr(handle->pd, handle->data_bufs[i], handle->data_bytes,
                       IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                           IBV_ACCESS_REMOTE_READ);
        if (!handle->data_mrs[i])
            goto err_mr;
        handle->local_lkeys[i] = handle->data_mrs[i]->lkey;
        handle->local_rkeys[i] = handle->data_mrs[i]->rkey;
        handle->local_base_addrs[i] = (uintptr_t)handle->data_bufs[i];
    }

    handle->ctrl_bytes = 64;
    handle->ctrl_buf = calloc(1, handle->ctrl_bytes);
    if (!handle->ctrl_buf)
        goto err_mr;
    handle->ctrl_mr =
        ibv_reg_mr(handle->pd, handle->ctrl_buf, handle->ctrl_bytes,
                   IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                       IBV_ACCESS_REMOTE_READ);
    if (!handle->ctrl_mr)
        goto err_mr;
    handle->ctrl_lkey = handle->ctrl_mr->lkey;
    handle->ctrl_rkey = handle->ctrl_mr->rkey;
    handle->ctrl_base_addr = (uintptr_t)handle->ctrl_buf;

    return handle;

err_mr:
    for (int i = 0; i < 2; ++i) {
        if (handle->data_mrs[i])
            ibv_dereg_mr(handle->data_mrs[i]);
        if (handle->data_bufs[i])
            free(handle->data_bufs[i]);
    }
    if (handle->ctrl_mr)
        ibv_dereg_mr(handle->ctrl_mr);
    if (handle->ctrl_buf)
        free(handle->ctrl_buf);
err_qp:
    for (int i = 0; i < 2; ++i)
        if (handle->qps[i])
            ibv_destroy_qp(handle->qps[i]);
err_cq:
    ibv_destroy_cq(handle->cq);
err_pd:
    ibv_dealloc_pd(handle->pd);
err_ctx:
    ibv_close_device(handle->ctx);
err_free:
    free(handle);
    return NULL;
}

void pg_destroy(pg_handle *handle) {
    if (!handle)
        return;
    for (int i = 0; i < 2; ++i) {
        if (handle->data_mrs[i])
            ibv_dereg_mr(handle->data_mrs[i]);
        if (handle->data_bufs[i])
            free(handle->data_bufs[i]);
        if (handle->qps[i])
            ibv_destroy_qp(handle->qps[i]);
    }
    if (handle->ctrl_mr)
        ibv_dereg_mr(handle->ctrl_mr);
    if (handle->ctrl_buf)
        free(handle->ctrl_buf);
    if (handle->cq)
        ibv_destroy_cq(handle->cq);
    if (handle->pd)
        ibv_dealloc_pd(handle->pd);
    if (handle->ctx)
        ibv_close_device(handle->ctx);
    free(handle);
}

int pg_qps_to_rts(pg_handle *handle, const qp_boot boots[2]) {
    if (!handle || !boots)
        return -1;

    struct ibv_port_attr port_attr;
    if (ibv_query_port(handle->ctx, 1, &port_attr))
        return -1;
    enum ibv_mtu mtu = port_attr.active_mtu;

    for (int i = 0; i < 2; ++i) {
        struct ibv_qp *qp = handle->qps[i];
        if (!qp)
            return -1;

        struct ibv_qp_attr attr;
        memset(&attr, 0, sizeof(attr));
        attr.qp_state = IBV_QPS_INIT;
        attr.pkey_index = 0;
        attr.port_num = 1;
        attr.qp_access_flags = IBV_ACCESS_REMOTE_WRITE |
                               IBV_ACCESS_REMOTE_READ |
                               IBV_ACCESS_LOCAL_WRITE;
        int flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT |
                    IBV_QP_ACCESS_FLAGS;
        if (ibv_modify_qp(qp, &attr, flags))
            return -1;

        memset(&attr, 0, sizeof(attr));
        attr.qp_state = IBV_QPS_RTR;
        attr.path_mtu = mtu;
        attr.dest_qp_num = boots[i].qpn;
        attr.rq_psn = 0;
        attr.max_dest_rd_atomic = 1;
        attr.min_rnr_timer = 12;
        attr.ah_attr.port_num = 1;
        if (boots[i].lid) {
            attr.ah_attr.is_global = 0;
            attr.ah_attr.dlid = boots[i].lid;
        } else {
            union ibv_gid gid;
            memcpy(&gid, boots[i].gid, sizeof(gid));
            attr.ah_attr.is_global = 1;
            attr.ah_attr.grh.dgid = gid;
            attr.ah_attr.grh.sgid_index = 0;
            attr.ah_attr.grh.hop_limit = 1;
        }
        flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU |
                IBV_QP_DEST_QPN | IBV_QP_RQ_PSN |
                IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
        if (ibv_modify_qp(qp, &attr, flags))
            return -1;

        memset(&attr, 0, sizeof(attr));
        attr.qp_state = IBV_QPS_RTS;
        attr.timeout = 14;
        attr.retry_cnt = 7;
        attr.rnr_retry = 7;
        attr.sq_psn = 0;
        attr.max_rd_atomic = 1;
        flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
                IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN |
                IBV_QP_MAX_QP_RD_ATOMIC;
        if (ibv_modify_qp(qp, &attr, flags))
            return -1;
    }

    return 0;
}

int pg_set_window(pg_handle *handle, void *sendbuf, void *recvbuf,
                  size_t bytes) {
    if (!handle || !sendbuf || !recvbuf || bytes == 0)
        return -1;

    struct ibv_mr *mrs[2];
    mrs[0] = ibv_reg_mr(handle->pd, sendbuf, bytes,
                        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                            IBV_ACCESS_REMOTE_READ);
    if (!mrs[0])
        return -1;
    mrs[1] = ibv_reg_mr(handle->pd, recvbuf, bytes,
                        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                            IBV_ACCESS_REMOTE_READ);
    if (!mrs[1]) {
        ibv_dereg_mr(mrs[0]);
        return -1;
    }

    for (int i = 0; i < 2; ++i) {
        if (handle->data_mrs[i])
            ibv_dereg_mr(handle->data_mrs[i]);
        if (handle->data_bufs[i]) {
            free(handle->data_bufs[i]);
            handle->data_bufs[i] = NULL;
        }
        handle->data_mrs[i] = mrs[i];
    }
    handle->local_base_addrs[0] = (uintptr_t)sendbuf;
    handle->local_base_addrs[1] = (uintptr_t)recvbuf;
    handle->local_lkeys[0] = handle->data_mrs[0]->lkey;
    handle->local_lkeys[1] = handle->data_mrs[1]->lkey;
    handle->local_rkeys[0] = handle->data_mrs[0]->rkey;
    handle->local_rkeys[1] = handle->data_mrs[1]->rkey;
    handle->data_bytes = bytes;
    return 0;
}
