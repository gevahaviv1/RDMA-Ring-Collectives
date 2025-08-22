#include "pg_internal.h"
#include <assert.h>
#include <stdio.h>
#include <string.h>

struct ibv_cq {
  int dummy;
};
int ibv_poll_cq(struct ibv_cq *cq, int num_entries, struct ibv_wc *wc) {
  (void)cq;
  (void)num_entries;
  wc[0].status = IBV_WC_SUCCESS;
  wc[0].wr_id = 0;
  return 1;
}

int main(void) {
  pg_handle handle = {0};
  handle.rank = 0;
  handle.world_size = 1;
  handle.max_inline_data = 64;
  handle.chunk_bytes = 16;
  handle.cq = (struct ibv_cq *)0x1;
  handle.qps[1] = (struct ibv_qp *)0x1;

  int32_t sendbuf[4] = {1, 2, 3, 4};
  int32_t recvbuf[4] = {0};

  int rc = pg_all_reduce(&handle, sendbuf, recvbuf, 4, DT_INT32, OP_SUM);
  assert(rc == 0);
  assert(memcmp(sendbuf, recvbuf, sizeof(sendbuf)) == 0);

  int32_t inplace_buf[4] = {5, 6, 7, 8};
  rc = pg_all_reduce(&handle, inplace_buf, inplace_buf, 4, DT_INT32, OP_SUM);
  assert(rc == 0);
  assert(inplace_buf[0] == 5 && inplace_buf[3] == 8);

  printf("pg_all_reduce single-rank test passed\n");
  return 0;
}
