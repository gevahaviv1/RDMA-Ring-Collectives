#include "pg.h"
#include "chunk_planner.h"
#include "reduce.h"
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <sys/time.h>

/* Forward declaration to keep tests linkable without libibverbs */
struct ibv_cq;
int ibv_poll_cq(struct ibv_cq *cq, int num_entries, struct ibv_wc *wc);

int pg_rank(const pg_handle *handle) { return handle ? handle->rank : -1; }
int pg_world_size(const pg_handle *handle) { return handle ? handle->world_size : -1; }

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

int pg_sendrecv_inline(pg_handle *handle, void *sendbuf, void *recvbuf,
                       size_t bytes, size_t eager_bytes, DATATYPE dtype,
                       OPERATION op) {
    if (!handle || !sendbuf || !recvbuf)
        return -1;
    if (bytes > eager_bytes || bytes > handle->max_inline_data)
        return -1;

    int rc = post_send_inline(handle, handle->qps[1], sendbuf, bytes);
    if (rc != 0)
        return rc;

    struct ibv_wc *wc = NULL;
    rc = poll_cq_until(handle->cq, 1, 1000, &wc);
    if (rc < 1) {
        free(wc);
        return -1;
    }
    free(wc);

    size_t count;
    switch (dtype) {
    case DT_INT32: {
        count = bytes / sizeof(int32_t);
        int32_t *dst = (int32_t *)recvbuf;
        int32_t *src = (int32_t *)sendbuf;
        if (op == OP_SUM) {
            for (size_t i = 0; i < count; ++i)
                dst[i] += src[i];
        } else if (op == OP_PROD) {
            for (size_t i = 0; i < count; ++i)
                dst[i] *= src[i];
        } else {
            memcpy(dst, src, count * sizeof(int32_t));
        }
        size_t rem = bytes % sizeof(int32_t);
        if (rem)
            memcpy((char *)dst + count * sizeof(int32_t),
                   (char *)src + count * sizeof(int32_t), rem);
        break;
    }
    case DT_DOUBLE: {
        count = bytes / sizeof(double);
        double *dst = (double *)recvbuf;
        double *src = (double *)sendbuf;
        if (op == OP_SUM) {
            for (size_t i = 0; i < count; ++i)
                dst[i] += src[i];
        } else if (op == OP_PROD) {
            for (size_t i = 0; i < count; ++i)
                dst[i] *= src[i];
        } else {
            memcpy(dst, src, count * sizeof(double));
        }
        size_t rem = bytes % sizeof(double);
        if (rem)
            memcpy((char *)dst + count * sizeof(double),
                   (char *)src + count * sizeof(double), rem);
        break;
    }
    default:
        memcpy(recvbuf, sendbuf, bytes);
        break;
    }

    return 0;
}

int pg_reduce_scatter(pg_handle *handle, void *sendbuf, void *recvbuf,
                      size_t count, DATATYPE dtype, OPERATION op) {
    if (!handle || !sendbuf || !recvbuf)
        return -1;
    int world = handle->world_size;
    int rank = handle->rank;
    if (world <= 0 || rank < 0 || rank >= world)
        return -1;

    size_t chunk_elems = chunk_elems_from_bytes(handle->chunk_bytes, dtype);
    if (chunk_elems == 0)
        return -1;

    /* Fast path for single rank: just copy local data. */
    if (world == 1) {
        switch (dtype) {
        case DT_INT32:
            memcpy(recvbuf, sendbuf, count * sizeof(int32_t));
            break;
        case DT_DOUBLE:
            memcpy(recvbuf, sendbuf, count * sizeof(double));
            break;
        default:
            memcpy(recvbuf, sendbuf, count);
            break;
        }
        return 0;
    }

    for (int round = 0; round < world - 1; ++round) {
        int send_idx = rs_send_chunk_index(round, rank, world);
        int recv_idx = rs_recv_chunk_index(round, rank, world);

        size_t send_off = 0, send_len = 0;
        size_t recv_off = 0, recv_len = 0;
        rs_chunk_offsets(count, chunk_elems, dtype, (size_t)send_idx,
                         &send_off, &send_len);
        rs_chunk_offsets(count, chunk_elems, dtype, (size_t)recv_idx,
                         &recv_off, &recv_len);

        if (recv_len == 0 && send_len == 0)
            continue;

        /* Small chunks use inline send/recv helper. */
        if (recv_len <= handle->max_inline_data) {
            pg_sendrecv_inline(handle,
                               (char *)sendbuf + send_off,
                               (char *)recvbuf + recv_off,
                               recv_len,
                               handle->max_inline_data,
                               dtype, op);
        } else {
            /* Large chunk path: placeholder for full RTS/CTS + READ orchestration.
               For skeleton purposes fall back to inline helper in chunks. */
            size_t processed = 0;
            while (processed < recv_len) {
                size_t step = handle->max_inline_data;
                if (step == 0) break;
                if (processed + step > recv_len)
                    step = recv_len - processed;
                pg_sendrecv_inline(handle,
                                   (char *)sendbuf + send_off + processed,
                                   (char *)recvbuf + recv_off + processed,
                                   step,
                                   handle->max_inline_data,
                                   dtype, op);
                processed += step;
            }
        }
    }

    /* After reduce-scatter each rank owns chunk (rank + 1) % world. */
    return 0;
}

int pg_all_gather(pg_handle *handle, void *recvbuf,
                  size_t count, DATATYPE dtype) {
    if (!handle || !recvbuf)
        return -1;
    int world = handle->world_size;
    int rank = handle->rank;
    if (world <= 0 || rank < 0 || rank >= world)
        return -1;

    size_t chunk_elems = chunk_elems_from_bytes(handle->chunk_bytes, dtype);
    if (chunk_elems == 0)
        return -1;

    /* Single rank already has the full vector. */
    if (world == 1)
        return 0;

    for (int round = 0; round < world - 1; ++round) {
        int send_idx = (rank + 1 - round + world) % world;
        int recv_idx = (rank - round + world) % world;

        size_t send_off = 0, send_len = 0;
        size_t recv_off = 0, recv_len = 0;
        ag_chunk_offsets(count, chunk_elems, dtype, (size_t)send_idx,
                         &send_off, &send_len);
        ag_chunk_offsets(count, chunk_elems, dtype, (size_t)recv_idx,
                         &recv_off, &recv_len);

        if (recv_len == 0 && send_len == 0)
            continue;

        if (recv_len <= handle->max_inline_data) {
            /* Small chunk: emulate inline send/recv with a local copy. */
            memcpy((char *)recvbuf + recv_off,
                   (char *)recvbuf + send_off, recv_len);
        } else {
            /* Large chunk path: placeholder for READ-based transfer.
               For skeleton purposes fall back to chunked local copy. */
            size_t processed = 0;
            while (processed < recv_len) {
                size_t step = handle->max_inline_data;
                if (step == 0)
                    break;
                if (processed + step > recv_len)
                    step = recv_len - processed;
                memcpy((char *)recvbuf + recv_off + processed,
                       (char *)recvbuf + send_off + processed,
                       step);
                processed += step;
            }
        }
    }
    /* After all-gather every rank holds the full reduced vector. */
    return 0;
}

int pg_all_reduce(pg_handle *handle, void *sendbuf, void *recvbuf,
                  size_t count, DATATYPE dtype, OPERATION op) {
    if (!handle || !sendbuf || !recvbuf)
        return -1;

    /* Validate datatype and operation combinations. */
    switch (dtype) {
    case DT_INT32:
    case DT_DOUBLE:
        break;
    default:
        return -1;
    }
    if (op != OP_SUM && op != OP_PROD)
        return -1;

    /* Out-of-place: copy sendbuf into recvbuf then operate in-place. */
    if (sendbuf != recvbuf) {
        size_t elem_size = (dtype == DT_INT32) ? sizeof(int32_t) : sizeof(double);
        memcpy(recvbuf, sendbuf, count * elem_size);
        sendbuf = recvbuf;
    }

    int rc = pg_reduce_scatter(handle, sendbuf, recvbuf, count, dtype, op);
    if (rc != 0)
        return rc;
    return pg_all_gather(handle, recvbuf, count, dtype);
}

#ifdef PG_DEBUG
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

void check_allreduce_against_cpu(void *sendbuf, size_t count,
                                 DATATYPE dtype, OPERATION op) {
    if (!sendbuf)
        return;

    size_t elem_size = (dtype == DT_INT32) ? sizeof(int32_t) : sizeof(double);
    /* Synthesize three ranks: local + two neighbors. */
    int world = 3;

    void *expected = malloc(count * elem_size);
    if (!expected)
        return;

    /* Initialize expected with deterministic values for rank 0. */
    if (dtype == DT_INT32) {
        int32_t *e = expected;
        for (size_t i = 0; i < count; ++i)
            e[i] = (int32_t)i;
    } else {
        double *e = expected;
        for (size_t i = 0; i < count; ++i)
            e[i] = (double)i;
    }

    /* Reduce in contributions from synthetic neighbors. */
    for (int r = 1; r < world; ++r) {
        void *nbr = malloc(count * elem_size);
        if (!nbr)
            break;
        if (dtype == DT_INT32) {
            int32_t *n = nbr;
            for (size_t i = 0; i < count; ++i)
                n[i] = (int32_t)(i + r);
        } else {
            double *n = nbr;
            for (size_t i = 0; i < count; ++i)
                n[i] = (double)(i + r);
        }
        reduce_inplace(expected, nbr, count, dtype, op);
        free(nbr);
    }

    if (memcmp(sendbuf, expected, count * elem_size) != 0) {
        fprintf(stderr, "check_allreduce_against_cpu: mismatch\n");
    }

    free(expected);
}
#endif /* PG_DEBUG */
