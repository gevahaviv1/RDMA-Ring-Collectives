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
#include <time.h>

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
            if (n < 0 && (errno == EINTR || errno == EAGAIN)) {
                continue;
            }
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
            if (n < 0 && (errno == EINTR || errno == EAGAIN)) {
                continue;
            }
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
// Time and Env Helpers
//==============================================================================

static inline int64_t now_ms(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static int getenv_int(const char *name, int fallback) {
    const char *s = getenv(name);
    if (!s || !*s) return fallback;
    char *end = NULL;
    long v = strtol(s, &end, 10);
    if (end == s || v <= 0 || v > 1<<30) return fallback;
    return (int)v;
}

//==============================================================================
// TCP Connection Setup
//==============================================================================

/**
 * @brief Creates a non-blocking listening socket bound to INADDR_ANY:port.
 * @return 0 on success, -1 on failure.
 */
static int pgnet_setup_listen_socket(int port, int *listen_fd_out) {
    struct addrinfo hints, *ai = NULL;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;

    char portstr[16];
    snprintf(portstr, sizeof(portstr), "%d", port);

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
#ifdef SO_REUSEPORT
    setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &reuse, sizeof(reuse));
#endif

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
 * @brief Initiates a non-blocking connect to host:port.
 * @return 0 on success (or in-progress), -1 on immediate failure.
 */
static int pgnet_connect_nonblocking(const char *host, int port, int *conn_fd_out) {
    struct addrinfo hints, *ai = NULL;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    char portstr[16];
    snprintf(portstr, sizeof(portstr), "%d", port);

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
        close(fd);
        return -1;
    }

    *conn_fd_out = fd;
    return 0;
}

/**
 * @brief Polls until the inbound accept (from left) and outbound connect (to right) complete.
 * Retries connect on transient failures until deadline.
 * @return 0 on success, -1 on timeout or error.
 */
static int pgnet_poll_until_ready(int listen_fd, const char *right_host, int right_port,
                                  int timeout_ms, int backoff_ms,
                                  int *left_fd_out, int *right_fd_out) {
    int64_t start = now_ms();
    int64_t deadline = start + timeout_ms;
    int left_fd = -1;
    int right_fd = -1;
    int conn_fd = -1;
    int64_t next_attempt = start; // immediate first attempt

    struct pollfd pfds[2];
    memset(pfds, 0, sizeof(pfds));
    pfds[0].fd = listen_fd;
    pfds[0].events = POLLIN;
    pfds[1].fd = -1;
    pfds[1].events = POLLOUT;

    while ((left_fd < 0 || right_fd < 0) && now_ms() < deadline) {
        int64_t now = now_ms();

        // Initiate or re-initiate non-blocking connect as needed
        if (right_fd < 0 && conn_fd < 0 && now >= next_attempt) {
            if (pgnet_connect_nonblocking(right_host, right_port, &conn_fd) == 0) {
                pfds[1].fd = conn_fd;
#ifdef PG_DEBUG
                fprintf(stderr, "[net] retrying connect... (%lld ms elapsed)\n",
                        (long long)(now - start));
#endif
            } else {
                // immediate failure, schedule next attempt
                next_attempt = now + backoff_ms;
            }
        }

        int poll_ms;
        if (conn_fd < 0) {
            // No outgoing connect fd yet: wait until next_attempt or deadline
            int64_t until_next = next_attempt - now;
            if (until_next < 0) until_next = 0;
            int64_t until_deadline = deadline - now;
            poll_ms = (int)((until_next < until_deadline) ? until_next : until_deadline);
        } else {
            // Have an outstanding connect; poll both fds with a small timeout
            int64_t until_deadline = deadline - now;
            poll_ms = (int)((until_deadline > backoff_ms) ? backoff_ms : until_deadline);
        }
        if (poll_ms < 0) poll_ms = 0;

        nfds_t nfds = (conn_fd >= 0) ? 2 : 1;
        int rc = poll(pfds, nfds, poll_ms);
        if (rc < 0) {
            if (errno == EINTR) continue;
            fprintf(stderr, "poll() failed: %s\n", strerror(errno));
            break;
        }

        // Accept from left (non-blocking)
        if (left_fd < 0 && (pfds[0].revents & POLLIN)) {
            int fd = accept(listen_fd, NULL, NULL);
            if (fd >= 0) {
                // Keep sockets non-blocking to ensure robust I/O; our read/write handle EAGAIN
                left_fd = fd;
#ifdef PG_DEBUG
                fprintf(stderr, "[net] accept left OK\n");
#endif
            } else if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
                fprintf(stderr, "accept() failed: %s\n", strerror(errno));
                break;
            }
        }

        // Check outgoing connect progress
        if (right_fd < 0 && conn_fd >= 0 && (pfds[1].revents & (POLLOUT | POLLERR | POLLHUP))) {
            int err = 0;
            socklen_t errlen = sizeof(err);
            if (getsockopt(conn_fd, SOL_SOCKET, SO_ERROR, &err, &errlen) < 0) err = errno;
            if (err == 0) {
                right_fd = conn_fd;
                conn_fd = -1;
                pfds[1].fd = -1;
#ifdef PG_DEBUG
                fprintf(stderr, "[net] connect right OK\n");
#endif
            } else {
                // transient failure: retry until deadline
                if (err == ECONNREFUSED || err == ETIMEDOUT || err == EHOSTUNREACH) {
                    close(conn_fd);
                    conn_fd = -1;
                    pfds[1].fd = -1;
                    next_attempt = now_ms() + backoff_ms;
                } else {
                    fprintf(stderr, "connect() failed: %s\n", strerror(err));
                    close(conn_fd);
                    conn_fd = -1;
                    break;
                }
            }
        }
    }

    if (conn_fd >= 0) { close(conn_fd); }
    if (left_fd >= 0 && right_fd >= 0) {
        *left_fd_out = left_fd;
        *right_fd_out = right_fd;
        return 0;
    }
    if (left_fd >= 0) close(left_fd);
    if (right_fd >= 0) close(right_fd);
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

    // Base settings with env overrides
    int base_port = pg->port ? pg->port : PG_DEFAULT_PORT;
    base_port = getenv_int("PG_PORT", base_port);
    int timeout_ms = getenv_int("PG_CONNECT_TIMEOUT_MS", PG_DEFAULT_CONNECT_TIMEOUT_MS);
    int backoff_ms = getenv_int("PG_BACKOFF_MS", PG_DEFAULT_BACKOFF_MS);

    // Per-rank ports
    int listen_port = base_port + (pg->rank % 10000);
    int right_port  = base_port + ((pg->rank + 1) % pg->world);
    int left_rank   = (pg->rank - 1 + pg->world) % pg->world;
    int left_port   = base_port + left_rank; (void)left_port; // for clarity/logs if needed
    const char *right_host = pg->hosts[right];

    fprintf(stderr, "[net] rank=%d listening on port %d, connecting right=%s:%d\n",
            pg->rank, listen_port, right_host, right_port);

#ifdef PG_DEBUG
    fprintf(stderr, "[net] rank=%d listen=%s:%d right=%s:%d timeout=%dms\n",
            pg->rank, "*", listen_port, pg->hosts[right], right_port, timeout_ms);
#endif

    int listen_fd = -1, left_fd = -1, right_fd = -1;
    if (pgnet_setup_listen_socket(listen_port, &listen_fd) != 0) goto fail;

    // Single loop handles both accept and connect with retry/backoff until timeout
    if (pgnet_poll_until_ready(listen_fd, pg->hosts[right], right_port,
                               timeout_ms, backoff_ms, &left_fd, &right_fd) != 0) {
        goto fail;
    }

    close(listen_fd);
    listen_fd = -1;

    if (pgnet_exchange_bootstrap(left_fd, right_fd, pg) != 0) {
        goto fail;
    }

    close(left_fd);
    close(right_fd);
    return 0;

fail:
    if (listen_fd >= 0) close(listen_fd);
    if (left_fd >= 0) close(left_fd);
    if (right_fd >= 0) close(right_fd);
    return -1;
}
