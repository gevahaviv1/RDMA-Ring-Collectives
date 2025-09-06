#include <arpa/inet.h>
#include <infiniband/verbs.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include "pg.h"  // your header with connect_process_group etc.

// Simple TCP-based ring barrier for test synchronization
static int tcp_barrier(struct pg *pg) {
    if (pg->world_size <= 1) {
        return 0;
    }

    // Simple ring barrier using the bootstrap TCP sockets
    char token = 'S'; // Sync token
    int rank = pg->rank;

    fprintf(stderr, "Rank %d entering TCP barrier.\n", rank);

    if (rank == 0) {
        fprintf(stderr, "Rank 0 sending token to right...\n");
        if (write(pg->right_fd, &token, sizeof(token)) != sizeof(token)) {
            perror("tcp_barrier: rank 0 write to right failed");
            return -1;
        }
        fprintf(stderr, "Rank 0 waiting for token from left...\n");
        if (read(pg->left_fd, &token, sizeof(token)) != sizeof(token)) {
            perror("tcp_barrier: rank 0 read from left failed");
            return -1;
        }
    } else {
        fprintf(stderr, "Rank %d waiting for token from left...\n", rank);
        if (read(pg->left_fd, &token, sizeof(token)) != sizeof(token)) {
            fprintf(stderr, "tcp_barrier: rank %d read from left failed\n", rank);
            perror("read");
            return -1;
        }
        fprintf(stderr, "Rank %d received token, sending to right...\n", rank);
        if (write(pg->right_fd, &token, sizeof(token)) != sizeof(token)) {
            fprintf(stderr, "tcp_barrier: rank %d write to right failed\n", rank);
            perror("write");
            return -1;
        }
    }

    fprintf(stderr, "Rank %d passed TCP barrier.\n", rank);
    return 0;
}

// Simple helper: post one RECV
static int post_one_recv(struct ibv_qp *qp, struct ibv_mr *mr, int *buf) {
    struct ibv_sge sge = {
        .addr = (uintptr_t)buf, .length = sizeof(int), .lkey = mr->lkey};
    struct ibv_recv_wr wr = {.wr_id = 1, .sg_list = &sge, .num_sge = 1};
    struct ibv_recv_wr *bad;
    fprintf(stderr, "Posting receive for buf at %p...\n", buf);
    int ret = ibv_post_recv(qp, &wr, &bad);
    if (ret) {
        fprintf(stderr, "Failed to post RECV: %s\n", strerror(ret));
    }
    return ret;
}

// Helper to stringify QP state for clearer debug logs
static const char *qp_state_str(enum ibv_qp_state s) {
    switch (s) {
        case IBV_QPS_RESET: return "RESET";
        case IBV_QPS_INIT:  return "INIT";
        case IBV_QPS_RTR:   return "RTR";
        case IBV_QPS_RTS:   return "RTS";
        case IBV_QPS_SQD:   return "SQD";
        case IBV_QPS_SQE:   return "SQE";
        case IBV_QPS_ERR:   return "ERR";
        default: return "?";
    }
}

static void debug_dump_qp(struct ibv_qp *qp, const char *name) {
    struct ibv_qp_attr attr;
    struct ibv_qp_init_attr init;
    memset(&attr, 0, sizeof(attr));
    memset(&init, 0, sizeof(init));
    int mask = IBV_QP_STATE | IBV_QP_CAP;
    if (ibv_query_qp(qp, &attr, mask, &init) == 0) {
        fprintf(stderr, "QP %s: qpn=%u state=%s max_inline=%u max_send_wr=%u max_recv_wr=%u\n",
                name, qp->qp_num, qp_state_str(attr.qp_state),
                init.cap.max_inline_data, init.cap.max_send_wr, init.cap.max_recv_wr);
    } else {
        perror("ibv_query_qp");
    }
}

static int post_one_send(struct ibv_qp *qp, struct ibv_mr *mr, int *buf,
                         uint32_t send_flags) {
    // Sanity-check the buffer is within the MR range
    uintptr_t a = (uintptr_t)buf;
    uintptr_t start = (uintptr_t)mr->addr;
    uintptr_t end = start + mr->length;
    if (a < start || (a + sizeof(int)) > end) {
        fprintf(stderr, "SEND buffer %p out of MR range [%p..%p)\n",
                (void*)a, (void*)start, (void*)end);
        return EINVAL;
    }
    struct ibv_sge sge = {
        .addr = (uintptr_t)buf, .length = sizeof(int), .lkey = mr->lkey};
    struct ibv_send_wr wr = {.wr_id = 2,
                             .sg_list = &sge,
                             .num_sge = 1,
                             .opcode = IBV_WR_SEND,
                             .send_flags = send_flags};
    struct ibv_send_wr *bad;
    // Optional: allow disabling inline via env for troubleshooting
    const char *no_inline = getenv("PG_NO_INLINE");
    if (no_inline && no_inline[0] == '1') {
        send_flags &= ~IBV_SEND_INLINE;
    }

    // Ensure WR reflects any env-based flag changes
    wr.send_flags = send_flags;

    fprintf(stderr, "Posting send for buf at %p (len=%zu) lkey=0x%x flags=0x%x...\n",
            buf, sizeof(int), mr->lkey, send_flags);
    debug_dump_qp(qp, "send_qp");
    fflush(stderr);
    int ret = ibv_post_send(qp, &wr, &bad);
    fprintf(stderr, "ibv_post_send returned %d%s\n", ret, ret ? " (error)" : "");
    if (ret) {
        fprintf(stderr, "Failed to post SEND: %s\n", strerror(ret));
    }
    return ret;
}

int main(int argc, char *argv[]) {
  if (argc < 4) {
    fprintf(stderr, "Usage: %s -myindex <rank> -list <host1 host2 ...>\n",
            argv[0]);
    return EXIT_FAILURE;
  }

  int myindex = -1;
  char hostlist[1024] = {0};
  int chain_mode = 0; // optional serialized hop-by-hop mode

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
    } else if (strcmp(argv[i], "--chain") == 0 || strcmp(argv[i], "-chain") == 0) {
      chain_mode = 1;
    }
  }

  if (myindex <= 0 || strlen(hostlist) == 0) {
    fprintf(stderr, "Invalid arguments.\n");
    return EXIT_FAILURE;
  }

  char hostname[256];
  gethostname(hostname, sizeof(hostname));

  printf("Rank %d on host %s starting, hostlist='%s'%s\n", myindex, hostname,
         hostlist, chain_mode ? " [CHAIN]" : "");

  // --- call your existing connect_process_group ---
  struct pg *handle;
  if (connect_process_group(hostlist, (void **)&handle)) {
    fprintf(stderr, "connect_process_group failed!\n");
    return EXIT_FAILURE;
  }

  // === Ring Ping Test ===
  // Use the library's pre-registered buffer for communication
  // This avoids registering separate, small buffers and ensures we use the MR
  // that the library's post_send/recv helpers expect.
  int *send_buf = (int *)handle->buf; // Use the start of the buffer for sending
  int *recv_buf = (int *)((char*)handle->buf + sizeof(int)); // Use an offset for receiving

  *send_buf = myindex; // Send our rank
  *recv_buf = -1;      // Initialize with an invalid value

  // Post RECV from left neighbor using the library's MR
  fprintf(stderr, "Rank %d posting receive buffer...\n", myindex);
  if (post_one_recv(handle->qp_left, handle->mr, recv_buf)) {
    fprintf(stderr, "Failed to post RECV\n");
    return EXIT_FAILURE;
  }

  // Synchronize all ranks to ensure RECVs are posted before any SEND
  if (tcp_barrier(handle) != 0) {
      fprintf(stderr, "TCP barrier failed\n");
      return EXIT_FAILURE;
  }

  // In CHAIN mode, only rank 0 sends initially. Others wait until their RECV completes.
  // In default mode, everyone posts SEND immediately after the barrier.
  uint32_t flags = IBV_SEND_SIGNALED;
  if (handle->max_inline_data >= sizeof(int)) flags |= IBV_SEND_INLINE;
  int posted_send = 0;
  if (!chain_mode || handle->rank == 0) {
    fprintf(stderr, "Rank %d posting send to right%s...\n", myindex, chain_mode ? " [CHAIN start]" : "");
    if (post_one_send(handle->qp_right, handle->mr, send_buf, flags)) {
      fprintf(stderr, "Failed to post SEND\n");
      return EXIT_FAILURE;
    }
    posted_send = 1;
  } else {
    fprintf(stderr, "Rank %d defers SEND until RECV completes [CHAIN]...\n", myindex);
  }

  // Poll CQ until both complete
  int completions = 0;
  int spins = 0;
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
        return EXIT_FAILURE;
      }
      fprintf(stderr, "CQE: wr_id=%llu opcode=%u\n", (unsigned long long)wc.wr_id, wc.opcode);
      // In CHAIN mode, once our RECV completes, we trigger our SEND if not yet posted.
      if (chain_mode && wc.wr_id == 1 && !posted_send) {
        fprintf(stderr, "Rank %d posting send to right [CHAIN after RECV]...\n", myindex);
        if (post_one_send(handle->qp_right, handle->mr, send_buf, flags)) {
          fprintf(stderr, "Failed to post SEND\n");
          return EXIT_FAILURE;
        }
        posted_send = 1;
      }
      completions++;
      spins = 0;
    } else {
      // Periodic debug heartbeat to show we are still polling
      if ((++spins % 1000000) == 0) { // print occasionally
        fprintf(stderr, "Still waiting for completions (have=%d)...\n", completions);
        debug_dump_qp(handle->qp_left,  "left_qp");
        debug_dump_qp(handle->qp_right, "right_qp");
        fflush(stderr);
      }
    }
  }

  fprintf(stderr, "Rank %d on host %s received %d from left neighbor\n", myindex,
          hostname, *recv_buf);

  // The library's MR will be deregistered by pg_close()

  pg_close(handle);
  return EXIT_SUCCESS;
}
