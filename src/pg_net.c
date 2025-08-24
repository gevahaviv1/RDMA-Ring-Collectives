#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include "pg_internal.h"
#include "RDMA_api.h"

/*
 * This file implements the TCP-based bootstrap networking for the process group.
 * It handles socket creation, ring-based connections, and the exchange of RDMA
 * bootstrap information required to bring Queue Pairs (QPs) to a connected state.
 */

//==============================================================================
// Socket I/O Helpers
//==============================================================================

/**
 * @brief Writes exactly 'len' bytes from 'buf' to socket 'fd'.
 * Handles short writes and EINTR signals to ensure all data is sent.
 * @return 0 on success, -1 on error.
 */
static int pgnet_write_full(int fd, const void *buf, size_t len) {
    const char *p = (const char *)buf;
    while (len > 0) {
        ssize_t n = send(fd, p, len, 0);
        if (n <= 0) {
            if (n < 0 && errno == EINTR) continue;
            fprintf(stderr, "pgnet_write_full failed: %s\n", strerror(errno));
            return -1;
        }
        p += n;
        len -= (size_t)n;
    }
    return 0;
}

/**
 * @brief Reads exactly 'len' bytes into 'buf' from socket 'fd'.
 * Handles short reads and EINTR signals to ensure all data is received.
 * @return 0 on success, -1 on error or EOF.
 */
static int pgnet_read_full(int fd, void *buf, size_t len) {
    char *p = (char *)buf;
    while (len > 0) {
        ssize_t n = recv(fd, p, len, 0);
        if (n <= 0) {
            if (n < 0 && errno == EINTR) continue;
            fprintf(stderr, "pgnet_read_full failed: %s\n",
                    (n == 0) ? "peer closed connection" : strerror(errno));
            return -1;
        }
        p += n;
        len -= (size_t)n;
    }
    return 0;
}

//==============================================================================
// TCP Connection Setup
//==============================================================================

/**
 * @brief Creates a non-blocking listening socket bound to 'portstr'.
 * @return 0 on success, -1 on failure.
 */
static int pgnet_setup_listen_socket(const char *portstr, int *listen_fd_out) {
    struct addrinfo hints, *ai = NULL;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    if (getaddrinfo(NULL, portstr, &hints, &ai) != 0) {
        fprintf(stderr, "getaddrinfo failed for port %s\n", portstr);
        return -1;
    }

    int fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
    if (fd < 0) {
        fprintf(stderr, "socket() failed: %s\n", strerror(errno));
        freeaddrinfo(ai);
        return -1;
    }

    fcntl(fd, F_SETFL, O_NONBLOCK);
    int reuse = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    if (bind(fd, ai->ai_addr, ai->ai_addrlen) != 0) {
        fprintf(stderr, "bind() to port %s failed: %s\n", portstr, strerror(errno));
        freeaddrinfo(ai);
        close(fd);
        return -1;
    }

    if (listen(fd, 128) != 0) { // Increased backlog
        fprintf(stderr, "listen() failed: %s\n", strerror(errno));
        freeaddrinfo(ai);
        close(fd);
        return -1;
    }

    freeaddrinfo(ai);
    *listen_fd_out = fd;
    return 0;
}

/**
 * @brief Initiates a non-blocking connect to 'host:portstr'.
 * @return 0 on success (or in-progress), -1 on immediate failure.
 */
static int pgnet_connect_right_nonblocking(const char *host, const char *portstr, int *conn_fd_out) {
    struct addrinfo hints, *ai = NULL;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    if (getaddrinfo(host, portstr, &hints, &ai) != 0) {
        fprintf(stderr, "getaddrinfo failed for host %s\n", host);
        return -1;
    }

    int fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
    if (fd < 0) {
        fprintf(stderr, "socket() failed: %s\n", strerror(errno));
        freeaddrinfo(ai);
        return -1;
    }

    fcntl(fd, F_SETFL, O_NONBLOCK);
    int rc = connect(fd, ai->ai_addr, ai->ai_addrlen);
    freeaddrinfo(ai);

    if (rc != 0 && errno != EINPROGRESS) {
        fprintf(stderr, "connect() to %s:%s failed: %s\n", host, portstr, strerror(errno));
        close(fd);
        return -1;
    }

    *conn_fd_out = fd;
    return 0;
}

/**
 * @brief Polls until the inbound accept (from left) and outbound connect (to right) complete.
 * @return 0 on success, -1 on timeout or error.
 */
static int pgnet_poll_until_ready(int listen_fd, int conn_fd, int *left_fd_out, int *right_fd_out) {
    struct pollfd pfds[2];
    pfds[0].fd = listen_fd;
    pfds[0].events = POLLIN;
    pfds[1].fd = conn_fd;
    pfds[1].events = POLLOUT;
    int timeout_ms = 30000; // Increased timeout

    int left_fd = -1, right_fd = -1;
    while (left_fd < 0 || right_fd < 0) {
        int rc = poll(pfds, 2, timeout_ms);
        if (rc < 0) {
            fprintf(stderr, "poll() failed: %s\n", strerror(errno));
            break;
        }
        if (rc == 0) {
            fprintf(stderr, "poll() timed out waiting for connections\n");
            break;
        }

        if (left_fd < 0 && (pfds[0].revents & POLLIN)) {
            left_fd = accept(listen_fd, NULL, NULL);
            if (left_fd >= 0) {
                fcntl(left_fd, F_SETFL, fcntl(left_fd, F_GETFL, 0) & ~O_NONBLOCK);
            }
        }

        if (right_fd < 0 && (pfds[1].revents & (POLLOUT | POLLERR | POLLHUP))) {
            int err = 0;
            socklen_t errlen = sizeof(err);
            if (getsockopt(conn_fd, SOL_SOCKET, SO_ERROR, &err, &errlen) < 0 || err) {
                fprintf(stderr, "connect to right neighbor failed: %s\n", strerror(err));
                break;
            }
            right_fd = conn_fd;
            fcntl(right_fd, F_SETFL, fcntl(right_fd, F_GETFL, 0) & ~O_NONBLOCK);
        }
    }

    if (left_fd >= 0 && right_fd >= 0) {
        *left_fd_out = left_fd;
        *right_fd_out = right_fd;
        return 0;
    }

    if (left_fd >= 0) close(left_fd);
    // conn_fd is closed by the caller's fail path
    return -1;
}

//==============================================================================
// Bootstrap Exchange
//==============================================================================

/**
 * @brief Exchanges bootstrap QP info with a peer over a connected socket.
 * @param send_first If true, send local info before receiving remote info.
 * @return 0 on success, -1 on failure.
 */
static int pgnet_exchange_qp(int fd, const struct qp_boot *local, struct qp_boot *remote, int send_first) {
    if (send_first) {
        if (pgnet_write_full(fd, local, sizeof(*local)) != 0) return -1;
        if (pgnet_read_full(fd, remote, sizeof(*remote)) != 0) return -1;
    } else {
        if (pgnet_read_full(fd, remote, sizeof(*remote)) != 0) return -1;
        if (pgnet_write_full(fd, local, sizeof(*local)) != 0) return -1;
    }
    return 0;
}

/**
 * @brief Main bootstrap logic: gathers local RDMA info, exchanges it with neighbors,
 * and transitions QPs to a connected state.
 * @return 0 on success, -1 on failure.
 */
static int pgnet_exchange_bootstrap(int left_fd, int right_fd, struct pg *pg) {
    // 1. Query local GID for RoCE addressing.
    union ibv_gid local_gid;
    if (rdma_query_gid(pg->ctx, pg->ib_port, pg->gid_index, &local_gid) != 0) {
        return -1;
    }

    // 2. Populate local bootstrap info.
    struct qp_boot myinfo;
    memset(&myinfo, 0, sizeof(myinfo));
    myinfo.qpn = pg->qp_left->qp_num; // Use one QP for both directions' identity
    myinfo.psn = lrand48() & 0xFFFFFF;
    memcpy(myinfo.gid, &local_gid, 16);
    myinfo.addr = (uintptr_t)pg->mr->addr;
    myinfo.rkey = pg->mr->rkey;
    myinfo.bytes = pg->mr->length;

    // 3. Exchange bootstrap info with left and right neighbors.
    if (pgnet_exchange_qp(left_fd, &myinfo, &pg->left_qp, 0) != 0) {
        fprintf(stderr, "Bootstrap exchange with left neighbor failed\n");
        return -1;
    }
    if (pgnet_exchange_qp(right_fd, &myinfo, &pg->right_qp, 1) != 0) {
        fprintf(stderr, "Bootstrap exchange with right neighbor failed\n");
        return -1;
    }

    // 4. Transition local QPs to RTR and RTS using received remote info.
    enum ibv_mtu mtu = IBV_MTU_1024;
    if (rdma_qp_to_rtr(pg->qp_left, &pg->left_qp, pg->ib_port, pg->gid_index, mtu) != 0) return -1;
    if (rdma_qp_to_rtr(pg->qp_right, &pg->right_qp, pg->ib_port, pg->gid_index, mtu) != 0) return -1;
    if (rdma_qp_to_rts(pg->qp_left, myinfo.psn) != 0) return -1;
    if (rdma_qp_to_rts(pg->qp_right, myinfo.psn) != 0) return -1;

    // 5. Query and store the actual max inline data size supported.
    if (rdma_qp_query_inline(pg->qp_left, &pg->max_inline_data) != 0) {
        pg->max_inline_data = 0; // Fallback if query fails
    }

    return 0;
}

/**
 * @brief Orchestrates the entire ring connection and bootstrap process.
 */
int pgnet_ring_connect(struct pg *pg) {
    int right = (pg->rank + 1) % pg->world;
    char portstr[16];
    snprintf(portstr, sizeof(portstr), "%d", pg->port);

    int listen_fd = -1, conn_fd = -1, left_fd = -1, right_fd = -1;

    if (pgnet_setup_listen_socket(portstr, &listen_fd) != 0) goto fail;

    // Ranks connect in a ring. To avoid deadlock, rank 0 connects first, then
    // others wait briefly before connecting to allow listeners to be ready.
    if (pg->rank != 0) usleep(100 * 1000);

    if (pgnet_connect_right_nonblocking(pg->hosts[right], portstr, &conn_fd) != 0) {
        goto fail;
    }

    if (pgnet_poll_until_ready(listen_fd, conn_fd, &left_fd, &right_fd) != 0) {
        goto fail;
    }

    close(listen_fd);
    listen_fd = -1; // Mark as closed

    if (pgnet_exchange_bootstrap(left_fd, right_fd, pg) != 0) {
        goto fail;
    }

    close(left_fd);
    close(right_fd);
    return 0;

fail:
    if (listen_fd >= 0) close(listen_fd);
    if (conn_fd >= 0 && conn_fd != right_fd) close(conn_fd);
    if (left_fd >= 0) close(left_fd);
    if (right_fd >= 0) close(right_fd);
    return -1;
}
