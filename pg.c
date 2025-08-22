#include <stdlib.h>
#include <string.h>
#include <infiniband/verbs.h>

#include "pg.h"
#include "ring.h"

struct pg_handle {
    struct ibv_context *ctx;          /* device context */
    struct ibv_pd *pd;                /* protection domain */
    struct ibv_cq *cq;                /* completion queue */
    struct ibv_qp *qps[2];            /* two queue pairs */
    uint32_t max_inline_data;         /* inline threshold */

    int rank;                         /* rank in communicator */
    int world_size;                   /* total participants */
    int left_index;                   /* neighbor to the left */
    int right_index;                  /* neighbor to the right */

    int bootstrap_socks[2];           /* TCP bootstrap sockets */

    struct ibv_mr *data_mrs[2];       /* exposed data windows */
    void *data_bufs[2];               /* owned placeholder buffers */
    uint32_t local_lkeys[2];          /* our lkeys */
    uint32_t local_rkeys[2];          /* our rkeys */
    uintptr_t local_base_addrs[2];    /* our base addresses */
    size_t data_bytes;                /* window size */

    struct ibv_mr *ctrl_mr;           /* small control window */
    void *ctrl_buf;                   /* owned control buffer */
    uint32_t ctrl_lkey;               /* control lkey */
    uint32_t ctrl_rkey;               /* control rkey */
    uintptr_t ctrl_base_addr;         /* control base address */
    size_t ctrl_bytes;                /* control region size */

    uint32_t neighbor_rkeys[2];       /* neighbors' rkeys */
    uintptr_t neighbor_base_addrs[2]; /* neighbors' base addresses */

    size_t chunk_bytes;               /* pipeline chunk size */
    int inflight_limit;               /* maximum inflight operations */

    int rx_credits[2];                /* receive credit counters */
};

int pg_rank(const pg_handle *handle) {
    return handle ? handle->rank : -1;
}

int pg_world_size(const pg_handle *handle) {
    return handle ? handle->world_size : -1;
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
