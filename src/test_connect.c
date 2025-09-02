#include <infiniband/verbs.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "pg.h"  // your header with connect_process_group etc.

// Simple helper: post one RECV
static int post_one_recv(struct ibv_qp *qp, struct ibv_mr *mr, int *buf) {
  struct ibv_sge sge = {
      .addr = (uintptr_t)buf, .length = sizeof(int), .lkey = mr->lkey};
  struct ibv_recv_wr wr = {.wr_id = 1, .sg_list = &sge, .num_sge = 1};
  struct ibv_recv_wr *bad;
  return ibv_post_recv(qp, &wr, &bad);
}

static int post_one_send(struct ibv_qp *qp, struct ibv_mr *mr, int *buf,
                         uint32_t send_flags) {
  struct ibv_sge sge = {
      .addr = (uintptr_t)buf, .length = sizeof(int), .lkey = mr->lkey};
  struct ibv_send_wr wr = {.wr_id = 2,
                           .sg_list = &sge,
                           .num_sge = 1,
                           .opcode = IBV_WR_SEND,
                           .send_flags = send_flags};
  struct ibv_send_wr *bad;
  return ibv_post_send(qp, &wr, &bad);
}

int main(int argc, char *argv[]) {
  if (argc < 4) {
    fprintf(stderr, "Usage: %s -myindex <rank> -list <host1 host2 ...>\n",
            argv[0]);
    return EXIT_FAILURE;
  }

  int myindex = -1;
  char hostlist[1024] = {0};

  // --- parse args ---
  for (int i = 1; i < argc; i++) {
    if (strcmp(argv[i], "-myindex") == 0 && i + 1 < argc) {
      myindex = atoi(argv[++i]);
    } else if (strcmp(argv[i], "-list") == 0) {
      for (int j = i + 1; j < argc; j++) {
        strcat(hostlist, argv[j]);
        if (j < argc - 1) strcat(hostlist, " ");
      }
      break;
    }
  }

  if (myindex <= 0 || strlen(hostlist) == 0) {
    fprintf(stderr, "Invalid arguments.\n");
    return EXIT_FAILURE;
  }

  char hostname[256];
  gethostname(hostname, sizeof(hostname));

  printf("Rank %d on host %s starting, hostlist='%s'\n", myindex, hostname,
         hostlist);

  // --- call your existing connect_process_group ---
  struct pg *handle;
  if (connect_process_group(hostlist, (void **)&handle)) {
    fprintf(stderr, "connect_process_group failed!\n");
    return EXIT_FAILURE;
  }

  // === Ring Ping Test ===
  // Assume pg_handle exposes left_qp, right_qp, pd, cq
  int send_val = myindex * 100;  // unique per rank
  int recv_val = -1;

  // Register memory for send/recv
  struct ibv_mr *send_mr =
      ibv_reg_mr(handle->pd, &send_val, sizeof(int), IBV_ACCESS_LOCAL_WRITE);
  struct ibv_mr *recv_mr =
      ibv_reg_mr(handle->pd, &recv_val, sizeof(int), IBV_ACCESS_LOCAL_WRITE);

  if (!send_mr || !recv_mr) {
    perror("ibv_reg_mr");
    return EXIT_FAILURE;
  }

  // Post RECV from left neighbor
  if (post_one_recv(handle->qp_left, recv_mr, &recv_val)) {
    fprintf(stderr, "Failed to post RECV\n");
    return EXIT_FAILURE;
  }

  // Post SEND to right neighbor
  uint32_t flags = IBV_SEND_SIGNALED;
  if (handle->max_inline_data >= sizeof(int)) flags |= IBV_SEND_INLINE;
  if (post_one_send(handle->qp_right, send_mr, &send_val, flags)) {
    fprintf(stderr, "Failed to post SEND\n");
    return EXIT_FAILURE;
  }

  // Poll CQ until both complete
  int completions = 0;
  while (completions < 2) {
    struct ibv_wc wc;
    int n = ibv_poll_cq(handle->cq, 1, &wc);
    if (n < 0) {
      fprintf(stderr, "poll_cq error\n");
      break;
    } else if (n > 0) {
      if (wc.status != IBV_WC_SUCCESS) {
        fprintf(stderr, "Work completion error: %s\n",
                ibv_wc_status_str(wc.status));
        break;
      }
      completions++;
    }
  }

  printf("Rank %d on host %s received %d from left neighbor\n", myindex,
         hostname, recv_val);

  // Cleanup
  ibv_dereg_mr(send_mr);
  ibv_dereg_mr(recv_mr);
  pg_close(handle);

  return EXIT_SUCCESS;
}
