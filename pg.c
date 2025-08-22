#include <stdlib.h>
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
    struct ibv_mr *ctrl_mr;           /* small control window */

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

    return handle;

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
    for (int i = 0; i < 2; ++i)
        if (handle->qps[i])
            ibv_destroy_qp(handle->qps[i]);
    if (handle->cq)
        ibv_destroy_cq(handle->cq);
    if (handle->pd)
        ibv_dealloc_pd(handle->pd);
    if (handle->ctx)
        ibv_close_device(handle->ctx);
    free(handle);
}
