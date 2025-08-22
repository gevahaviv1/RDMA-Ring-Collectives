#include "chunk_planner.h"
#include "pg_internal.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <unistd.h>

/* Forward declaration to keep tests linkable without libibverbs */
struct ibv_cq;
int ibv_poll_cq(struct ibv_cq *cq, int num_entries, struct ibv_wc *wc);
int ibv_destroy_qp(struct ibv_qp *qp);
int ibv_destroy_cq(struct ibv_cq *cq);
int ibv_dereg_mr(struct ibv_mr *mr);
int ibv_dealloc_pd(struct ibv_pd *pd);
int ibv_close_device(struct ibv_context *ctx);

int pg_rank(const pg_handle *handle) { return handle ? handle->rank : -1; }
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

int post_send_inline(pg_handle *handle, struct ibv_qp *qp, void *msg,
                     size_t len) {
  (void)qp;
  (void)msg;
  assert(handle);
  assert(len <= handle->max_inline_data);
  return 0;
}

int pg_ctrl_send(pg_handle *handle, int peer, struct ibv_qp *qp, void *msg,
                 size_t len) {
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
    for (int i = total; i < total + rc; ++i)
      PG_CHECK_WC(&wcs[i]);
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

void pg_close(pg_handle *handle) {
  if (!handle)
    return;

  for (int i = 0; i < 2; ++i) {
    if (handle->qps[i]) {
      ibv_destroy_qp(handle->qps[i]);
      handle->qps[i] = NULL;
    }
  }

  if (handle->cq) {
    ibv_destroy_cq(handle->cq);
    handle->cq = NULL;
  }

  for (int i = 0; i < 2; ++i) {
    if (handle->data_mrs[i]) {
      ibv_dereg_mr(handle->data_mrs[i]);
      handle->data_mrs[i] = NULL;
    }
  }
  if (handle->ctrl_mr) {
    ibv_dereg_mr(handle->ctrl_mr);
    handle->ctrl_mr = NULL;
  }

  if (handle->pd) {
    ibv_dealloc_pd(handle->pd);
    handle->pd = NULL;
  }

  if (handle->ctx) {
    ibv_close_device(handle->ctx);
    handle->ctx = NULL;
  }

  for (int i = 0; i < 2; ++i) {
    if (handle->bootstrap_socks[i] >= 0) {
      close(handle->bootstrap_socks[i]);
      handle->bootstrap_socks[i] = -1;
    }
  }

  free(handle);
}

void pg_destroy(pg_handle *handle) { pg_close(handle); }
