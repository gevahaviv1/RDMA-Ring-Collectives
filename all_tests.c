#include "bootstrap.h"
#include "chunk_planner.h"
#include "pg_internal.h"
#include "read_engine.h"
#include "reduce.h"
#include "ring.h"
#include "rtscts.h"
#include "serverlist.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/wait.h>
#include <unistd.h>
struct ibv_cq {

  struct ibv_wc comps[16];
  int head;
  int tail;
};

struct ibv_qp {
  struct ibv_cq *send_cq;
};

struct ibv_sge {
  uintptr_t addr;
  uint32_t length;
  uint32_t lkey;
};

enum ibv_wr_opcode { IBV_WR_RDMA_READ = 0 };

struct ibv_send_wr {
  uint64_t wr_id;
  struct ibv_send_wr *next;
  struct ibv_sge *sg_list;
  int num_sge;
  enum ibv_wr_opcode opcode;
  int send_flags;
  struct {
    struct {
      uintptr_t remote_addr;
      uint32_t rkey;
    } rdma;
  } wr;
};

struct ibv_context {
  int dummy;
};
struct ibv_pd {
  int dummy;
};
struct ibv_mr {
  int dummy;
};

static struct ibv_cq global_cq;

int ibv_post_send(struct ibv_qp *qp, struct ibv_send_wr *wr,
                  struct ibv_send_wr **bad_wr) {
  (void)bad_wr;
  (void)qp;
  if (wr && wr->sg_list) {
    struct ibv_sge *sge = wr->sg_list;
    memcpy((void *)sge->addr, (void *)wr->wr.rdma.remote_addr, sge->length);
  }
  int idx = global_cq.tail++ % 16;
  global_cq.comps[idx].wr_id = wr ? wr->wr_id : 0;
  global_cq.comps[idx].status = IBV_WC_SUCCESS;
  return 0;
}

static int (*ibv_poll_cq_impl)(struct ibv_cq *, int, struct ibv_wc *);

int ibv_poll_cq(struct ibv_cq *cq, int num_entries, struct ibv_wc *wc) {
  if (!ibv_poll_cq_impl)
    return 0;
  return ibv_poll_cq_impl(cq, num_entries, wc);
}

static int ibv_poll_cq_queue(struct ibv_cq *cq, int num_entries,
                             struct ibv_wc *wc) {
  int n = 0;
  while (n < num_entries && cq->head < cq->tail) {
    wc[n] = cq->comps[cq->head % 16];
    cq->head++;
    n++;
  }
  return n;
}

static int destroy_qp_calls = 0;
static int destroy_cq_calls = 0;
static int dereg_mr_calls = 0;
static int dealloc_pd_calls = 0;
static int close_dev_calls = 0;

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

/* Bootstrap test */
static void test_bootstrap(void) {
  const char *hosts[2] = {"127.0.0.1", "127.0.0.1"};
  const int base_port = 40000;
  int fd_left = -1, fd_right = -1;
  pid_t pid = fork();
  if (pid < 0)
    return;

  if (pid == 0) {
    if (bootstrap_ring(hosts, 2, 1, base_port, 5000, &fd_left, &fd_right) != 0)
      _exit(1);
    char ch;
    if (read(fd_left, &ch, 1) != 1)
      _exit(1);
    assert(ch == 'a');
    if (write(fd_right, "b", 1) != 1)
      _exit(1);
    struct qp_boot my = {.qpn = 11,
                         .psn = 21,
                         .lid = 31,
                         .gid = {0},
                         .base_addr = 41,
                         .rkey = 51,
                         .bytes = 61};
    memset(my.gid, 1, sizeof(my.gid));
    struct qp_boot left, right;
    if (exchange_qp_boot(fd_left, fd_right, &my, &left, &right, 5000) != 0)
      _exit(1);
    assert(left.qpn == 10 && right.qpn == 10);
    close(fd_left);
    close(fd_right);
    _exit(0);
  } else {
    if (bootstrap_ring(hosts, 2, 0, base_port, 5000, &fd_left, &fd_right) != 0)
      return;
    if (write(fd_right, "a", 1) != 1)
      return;
    char ch;
    if (read(fd_left, &ch, 1) != 1)
      return;
    assert(ch == 'b');
    struct qp_boot my = {.qpn = 10,
                         .psn = 20,
                         .lid = 30,
                         .gid = {0},
                         .base_addr = 40,
                         .rkey = 50,
                         .bytes = 60};
    memset(my.gid, 0, sizeof(my.gid));
    struct qp_boot left, right;
    if (exchange_qp_boot(fd_left, fd_right, &my, &left, &right, 5000) != 0)
      return;
    assert(left.qpn == 11 && right.qpn == 11);
    close(fd_left);
    close(fd_right);
    int status = 0;
    waitpid(pid, &status, 0);
    assert(WIFEXITED(status) && WEXITSTATUS(status) == 0);
    printf("Bootstrap test passed\n");
  }
}

/* Chunk planner tests */
static void test_chunk_elems(void) {
  assert(chunk_elems_from_bytes(32, DT_INT32) == 8);
  assert(chunk_elems_from_bytes(40, DT_DOUBLE) == 5);
}

static void test_chunk_count(void) {
  size_t elems = chunk_elems_from_bytes(16, DT_INT32);
  assert(chunk_count(10, elems) == 3);
  assert(chunk_count(8, elems) == 2);
}

static void test_rs_indices(void) {
  int N = 4;
  assert(rs_send_chunk_index(0, 0, N) == 0);
  assert(rs_recv_chunk_index(0, 0, N) == 3);
  assert(rs_send_chunk_index(1, 0, N) == 3);
  assert(rs_recv_chunk_index(1, 0, N) == 2);
  assert(rs_send_chunk_index(2, 0, N) == 2);
  assert(rs_recv_chunk_index(2, 0, N) == 1);
  assert(rs_send_chunk_index(0, 2, N) == 2);
  assert(rs_recv_chunk_index(0, 2, N) == 1);
  assert(rs_send_chunk_index(1, 2, N) == 1);
  assert(rs_recv_chunk_index(1, 2, N) == 0);
}

static void check_offsets(size_t count, size_t last_bytes) {
  size_t elems = chunk_elems_from_bytes(16, DT_INT32);
  size_t off, len;
  rs_chunk_offsets(count, elems, DT_INT32, 0, &off, &len);
  assert(off == 0 && len == 16);
  rs_chunk_offsets(count, elems, DT_INT32, 1, &off, &len);
  assert(off == 16 && len == 16);
  rs_chunk_offsets(count, elems, DT_INT32, 2, &off, &len);
  assert(off == 32 && len == last_bytes);
}

static void test_chunk_planner(void) {
  test_chunk_elems();
  test_chunk_count();
  test_rs_indices();
  check_offsets(10, 8);
  check_offsets(9, 4);
  printf("Chunk planner tests passed\n");
}

/* pg_all_gather */
static void test_pg_all_gather(void) {
  pg_handle handle = {0};
  handle.rank = 0;
  handle.world_size = 1;
  handle.max_inline_data = 64;
  handle.chunk_bytes = 16;
  handle.cq = (struct ibv_cq *)0x1;
  handle.qps[1] = (struct ibv_qp *)0x1;
  int32_t sendbuf[4] = {1, 2, 3, 4};
  int32_t recvbuf[4] = {0};
  int rc = pg_reduce_scatter(&handle, sendbuf, recvbuf, 4, DT_INT32, OP_SUM);
  assert(rc == 0);
  rc = pg_all_gather(&handle, recvbuf, 4, DT_INT32);
  assert(rc == 0);
  assert(memcmp(sendbuf, recvbuf, sizeof(sendbuf)) == 0);
  printf("pg_all_gather single-rank test passed\n");
}

/* pg_all_reduce */
static void test_pg_all_reduce(void) {
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
}

/* pg_close */
static void test_pg_close(void) {
  destroy_qp_calls = destroy_cq_calls = dereg_mr_calls = dealloc_pd_calls =
      close_dev_calls = 0;
  pg_handle *h = calloc(1, sizeof(pg_handle));
  assert(h);
  struct ibv_qp qp;
  struct ibv_cq cq;
  struct ibv_mr mr1;
  struct ibv_mr mr_ctrl;
  struct ibv_pd pd;
  struct ibv_context ctx;
  h->qps[0] = &qp;
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
  assert(dereg_mr_calls == 2);
  assert(dealloc_pd_calls == 1);
  assert(close_dev_calls == 1);
  pg_close(NULL);
  assert(destroy_qp_calls == 1);
  pg_handle *h2 = calloc(1, sizeof(pg_handle));
  pg_close(h2);
  assert(destroy_qp_calls == 1);
  printf("pg_close tests passed\n");
}

/* pg_control */
static void test_pg_control(void) {
  pg_handle handle = {0};
  handle.max_inline_data = 64;
  pg_ctrl_init(&handle);
  assert(handle.rx_credits[0] == PG_CTRL_RECV_SLOTS);
  assert(handle.rx_credits[1] == PG_CTRL_RECV_SLOTS);
  struct pg_msg_rts msg = {1, 2};
  for (int i = 0; i < PG_CTRL_RECV_SLOTS; ++i) {
    int rc = pg_ctrl_send(&handle, 0, NULL, &msg, sizeof(msg));
    assert(rc == 0);
  }
  assert(pg_ctrl_send(&handle, 0, NULL, &msg, sizeof(msg)) != 0);
  pg_ctrl_return_credit(&handle, 0);
  assert(pg_ctrl_send(&handle, 0, NULL, &msg, sizeof(msg)) == 0);
  assert(post_send_inline(&handle, NULL, &msg, sizeof(msg)) == 0);
  printf("Control message tests passed\n");
}

/* pg_cq_poll */
static int poll_called = 0;
static int cq_poll_stub(struct ibv_cq *cq, int num_entries, struct ibv_wc *wc) {
  (void)cq;
  if (poll_called++) {
    return 0;
  }
  if (num_entries >= 2) {
    wc[0].wr_id = 1;
    wc[0].status = IBV_WC_SUCCESS;
    wc[1].wr_id = 2;
    wc[1].status = IBV_WC_LOC_LEN_ERR;
    return 2;
  }
  return 0;
}

static void test_pg_cq_poll(void) {
  struct ibv_cq cq;
  struct ibv_wc *wcs = NULL;
  poll_called = 0;
  ibv_poll_cq_impl = cq_poll_stub;
  int n = poll_cq_until(&cq, 2, 100, &wcs);
  assert(n == 2);
  assert(wcs);
  assert(wcs[0].wr_id == 1);
  assert(wcs[0].status == IBV_WC_SUCCESS);
  assert(wcs[1].status == IBV_WC_LOC_LEN_ERR);
  free(wcs);
  ibv_poll_cq_impl = NULL;
  printf("CQ poll tests passed\n");
}

/* pg_reduce_scatter */
static void test_pg_reduce_scatter(void) {
  ibv_poll_cq_impl = ibv_poll_cq_queue;
  pg_handle handle = {0};
  handle.rank = 0;
  handle.world_size = 1;
  handle.max_inline_data = 64;
  handle.chunk_bytes = 16;
  handle.cq = (struct ibv_cq *)0x1;
  handle.qps[1] = (struct ibv_qp *)0x1;
  int32_t sendbuf[4] = {1, 2, 3, 4};
  int32_t recvbuf[4] = {0};
  int rc = pg_reduce_scatter(&handle, sendbuf, recvbuf, 4, DT_INT32, OP_SUM);
  assert(rc == 0);
  assert(memcmp(sendbuf, recvbuf, sizeof(sendbuf)) == 0);
  printf("pg_reduce_scatter single-rank test passed\n");
  ibv_poll_cq_impl = NULL;
}

/* pg_sendrecv_inline */
static void test_pg_sendrecv_inline(void) {
  ibv_poll_cq_impl = ibv_poll_cq_queue;
  global_cq.head = 0;
  global_cq.tail = 1;
  global_cq.comps[0].wr_id = 1;
  global_cq.comps[0].status = IBV_WC_SUCCESS;
  pg_handle handle = {0};
  handle.max_inline_data = 64;
  handle.cq = &global_cq;
  handle.qps[1] = (struct ibv_qp *)0x1;
  int32_t sendbuf[4] = {1, 2, 3, 4};
  int32_t recvbuf[4] = {10, 20, 30, 40};
  size_t bytes = sizeof(sendbuf);
  size_t eager_bytes = 64;
  int rc = pg_sendrecv_inline(&handle, sendbuf, recvbuf, bytes, eager_bytes,
                              DT_INT32, OP_SUM);
  assert(rc == 0);
  assert(recvbuf[0] == 11 && recvbuf[1] == 22 && recvbuf[2] == 33 &&
         recvbuf[3] == 44);
  printf("pg_sendrecv_inline test passed\n");
  ibv_poll_cq_impl = NULL;
}

/* read_engine */
static unsigned re_cb_count;
static char *re_remote_buf;
static size_t re_chunk_size;
static void re_on_chunk(int chunk_id, void *ptr, size_t bytes) {
  re_cb_count++;
  assert(bytes <= re_chunk_size);
  assert(memcmp(ptr, re_remote_buf + (size_t)chunk_id * re_chunk_size, bytes) ==
         0);
}

static void test_read_engine(void) {
  ibv_poll_cq_impl = ibv_poll_cq_queue;
  const size_t total_bytes = 1024;
  const size_t chunk_bytes = 128;
  const int inflight_limit = 4;
  char local_buf[total_bytes];
  char remote[total_bytes];
  for (size_t i = 0; i < total_bytes; ++i)
    remote[i] = (char)i;
  memset(local_buf, 0, sizeof(local_buf));
  re_remote_buf = remote;
  re_chunk_size = chunk_bytes;
  re_cb_count = 0;
  global_cq.head = global_cq.tail = 0;
  struct ibv_qp qp = {.send_cq = &global_cq};
  int rc = rdma_read_engine((uintptr_t)remote, 0, (uintptr_t)local_buf, 0,
                            total_bytes, chunk_bytes, inflight_limit, &qp,
                            re_on_chunk);
  assert(rc == 0);
  assert(re_cb_count == (total_bytes + chunk_bytes - 1) / chunk_bytes);
  assert(memcmp(local_buf, remote, total_bytes) == 0);
  printf("Read engine test passed\n");
  ibv_poll_cq_impl = NULL;
}

/* reduce_inplace */
static void ri_test_int32_sum(void) {
  int32_t dst[3] = {1, 2, 3};
  int32_t src[3] = {4, 5, 6};
  reduce_inplace(dst, src, 3, DT_INT32, OP_SUM);
  assert(dst[0] == 5 && dst[1] == 7 && dst[2] == 9);
}
static void ri_test_int32_prod(void) {
  int32_t dst[3] = {1, 2, 3};
  int32_t src[3] = {4, 5, 6};
  reduce_inplace(dst, src, 3, DT_INT32, OP_PROD);
  assert(dst[0] == 4 && dst[1] == 10 && dst[2] == 18);
}
static void ri_test_double_sum(void) {
  double dst[3] = {1.0, 2.0, 3.0};
  double src[3] = {4.0, 5.0, 6.0};
  reduce_inplace(dst, src, 3, DT_DOUBLE, OP_SUM);
  assert(dst[0] == 5.0 && dst[1] == 7.0 && dst[2] == 9.0);
}
static void ri_test_double_prod(void) {
  double dst[3] = {1.0, 2.0, 3.0};
  double src[3] = {4.0, 5.0, 6.0};
  reduce_inplace(dst, src, 3, DT_DOUBLE, OP_PROD);
  assert(dst[0] == 4.0 && dst[1] == 10.0 && dst[2] == 18.0);
}
static void test_reduce(void) {
  ri_test_int32_sum();
  ri_test_int32_prod();
  ri_test_double_sum();
  ri_test_double_prod();
  printf("reduce_inplace tests passed\n");
}

/* ring */
static void test_neighbors(void) {
  assert(left_of(0, 4) == 3);
  assert(right_of(0, 4) == 1);
  assert(left_of(2, 4) == 1);
  assert(right_of(3, 4) == 0);
  assert(left_of(4, 4) == 3);
  assert(right_of(-1, 4) == 0);
  assert(left_of(0, 0) == -1);
  assert(right_of(0, -5) == -1);
}
static void test_ring(void) {
  test_neighbors();
  printf("Ring neighbor tests passed\n");
}

/* rtscts */
static unsigned rtscts_cb_count;
static char *rtscts_remote_ptr;
static size_t rtscts_chunk_bytes;
static int rtscts_base_chunk;
static void rtscts_on_chunk(int chunk_id, void *ptr, size_t bytes) {
  rtscts_cb_count++;
  int abs_chunk = rtscts_base_chunk + chunk_id;
  assert(bytes <= rtscts_chunk_bytes);
  assert(memcmp(ptr, rtscts_remote_ptr + (size_t)abs_chunk * rtscts_chunk_bytes,
                bytes) == 0);
}
static void test_rtscts(void) {
  ibv_poll_cq_impl = ibv_poll_cq_queue;
  const size_t total_bytes = 512;
  const size_t chunk_bytes = 128;
  const int num_chunks = (total_bytes + chunk_bytes - 1) / chunk_bytes;
  char right_buf[total_bytes];
  char left_buf[total_bytes];
  for (size_t i = 0; i < total_bytes; ++i)
    right_buf[i] = (char)i;
  memset(left_buf, 0, sizeof(left_buf));
  rtscts_remote_ptr = right_buf;
  rtscts_chunk_bytes = chunk_bytes;
  rtscts_cb_count = 0;
  struct ctrl_queue to_left, to_right;
  ctrl_queue_init(&to_left);
  ctrl_queue_init(&to_right);
  global_cq.head = global_cq.tail = 0;
  struct ibv_qp qp = {.send_cq = &global_cq};
  for (int chunk = 0; chunk < num_chunks; ++chunk) {
    size_t bytes = chunk_bytes;
    if ((size_t)(chunk + 1) * chunk_bytes > total_bytes)
      bytes = total_bytes - (size_t)chunk * chunk_bytes;
    send_rts(&to_left, chunk, bytes);
    int r_chunk;
    size_t r_bytes;
    assert(recv_rts(&to_left, &r_chunk, &r_bytes));
    assert(r_chunk == chunk && r_bytes == bytes);
    size_t dst_off = (size_t)chunk * chunk_bytes;
    send_cts(&to_right, chunk, dst_off, bytes);
    int c_chunk;
    size_t c_off;
    size_t c_bytes;
    assert(recv_cts(&to_right, &c_chunk, &c_off, &c_bytes));
    assert(c_chunk == chunk && c_off == dst_off && c_bytes == bytes);
    rtscts_base_chunk = chunk;
    int rc =
        rdma_read_engine((uintptr_t)(right_buf + (size_t)chunk * chunk_bytes),
                         0, (uintptr_t)(left_buf + dst_off), 0, bytes, bytes, 1,
                         &qp, rtscts_on_chunk);
    assert(rc == 0);
    send_done(&to_right, chunk);
    int d_chunk;
    assert(recv_done(&to_right, &d_chunk));
    assert(d_chunk == chunk);
  }
  assert(rtscts_cb_count == (unsigned)num_chunks);
  assert(memcmp(left_buf, right_buf, total_bytes) == 0);
  printf("RTS/CTS test passed\n");
  ibv_poll_cq_impl = NULL;
}

/* serverlist */
static void test_single_host(void) {
  const char *servers = "host1";
  char **hosts = NULL;
  size_t count = 0, my_index = 0;
  int rc = parse_server_list(servers, "host1", &hosts, &count, &my_index);
  assert(rc == 0);
  assert(count == 1);
  assert(my_index == 0);
  free_host_list(hosts, count);
}
static void test_four_hosts(void) {
  const char *servers = "host1 host2\n   host3\thost4";
  char **hosts = NULL;
  size_t count = 0, my_index = 0;
  int rc = parse_server_list(servers, "host3.example.com", &hosts, &count,
                             &my_index);
  assert(rc == 0);
  assert(count == 4);
  assert(my_index == 2);
  free_host_list(hosts, count);
}
static void test_host_not_found(void) {
  const char *servers = "host1 host2";
  char **hosts = NULL;
  size_t count = 0, my_index = 0;
  int rc = parse_server_list(servers, "host3", &hosts, &count, &my_index);
  assert(rc == -1);
  assert(hosts == NULL);
  assert(count == 0);
  assert(my_index == 0);
}
static void test_serverlist(void) {
  test_single_host();
  test_four_hosts();
  test_host_not_found();
  printf("All tests passed\n");
}

int main(void) {
  test_bootstrap();
  test_chunk_planner();
  test_pg_all_gather();
  test_pg_all_reduce();
  test_pg_close();
  test_pg_control();
  test_pg_cq_poll();
  test_pg_reduce_scatter();
  test_pg_sendrecv_inline();
  test_read_engine();
  test_reduce();
  test_ring();
  test_rtscts();
  test_serverlist();
  printf("All tests passed\n");
  return 0;
}
