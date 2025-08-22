#include "pg_internal.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

struct ibv_context {
  int dummy;
};
struct ibv_pd {
  int dummy;
};
struct ibv_cq {
  int dummy;
};
struct ibv_qp {
  int dummy;
};
struct ibv_mr {
  int dummy;
};

static int destroy_qp_calls = 0;
static int destroy_cq_calls = 0;
static int dereg_mr_calls = 0;
static int dealloc_pd_calls = 0;
static int close_dev_calls = 0;

int ibv_poll_cq(struct ibv_cq *cq, int num_entries, struct ibv_wc *wc) {
  (void)cq;
  (void)num_entries;
  (void)wc;
  return 0;
}
int ibv_destroy_qp(struct ibv_qp *qp) {
  (void)qp;
  destroy_qp_calls++;
  return 0;
}
int ibv_destroy_cq(struct ibv_cq *cq) {
  (void)cq;
  destroy_cq_calls++;
  return 0;
}
int ibv_dereg_mr(struct ibv_mr *mr) {
  (void)mr;
  dereg_mr_calls++;
  return 0;
}
int ibv_dealloc_pd(struct ibv_pd *pd) {
  (void)pd;
  dealloc_pd_calls++;
  return 0;
}
int ibv_close_device(struct ibv_context *ctx) {
  (void)ctx;
  close_dev_calls++;
  return 0;
}

int main(void) {
  pg_handle *h = calloc(1, sizeof(pg_handle));
  assert(h);

  struct ibv_qp qp;
  struct ibv_cq cq;
  struct ibv_mr mr1;
  struct ibv_mr mr_ctrl;
  struct ibv_pd pd;
  struct ibv_context ctx;

  h->qps[0] = &qp; /* only one QP to destroy */
  h->cq = &cq;
  h->data_mrs[0] = &mr1;
  h->ctrl_mr = &mr_ctrl;
  h->pd = &pd;
  h->ctx = &ctx;
  h->bootstrap_socks[0] = -1;
  h->bootstrap_socks[1] = -1;

  pg_close(h);

  assert(destroy_qp_calls == 1);
  assert(destroy_cq_calls == 1);
  assert(dereg_mr_calls == 2); /* data + ctrl */
  assert(dealloc_pd_calls == 1);
  assert(close_dev_calls == 1);

  /* closing NULL handle should be a no-op */
  pg_close(NULL);
  assert(destroy_qp_calls == 1);

  /* zero-initialized handle should also be safe */
  pg_handle *h2 = calloc(1, sizeof(pg_handle));
  pg_close(h2);
  assert(destroy_qp_calls == 1);

  printf("pg_close tests passed\n");
  return 0;
}
