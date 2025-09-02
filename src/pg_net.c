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
#include <arpa/inet.h>
#include <netinet/in.h>
#include <signal.h>

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
    // Create IPv4 non-blocking listen socket, bind to INADDR_ANY
    int fd_listen = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (fd_listen < 0) {
        perror("[net] socket");
        return -1;
    }

    int one = 1;
    setsockopt(fd_listen, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
#ifdef SO_REUSEPORT
    setsockopt(fd_listen, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one));
#endif

    struct sockaddr_in sa;
    memset(&sa, 0, sizeof(sa));
    sa.sin_family = AF_INET;
    sa.sin_addr.s_addr = htonl(INADDR_ANY); // not 127.0.0.1
    sa.sin_port = htons((uint16_t)port);

    if (bind(fd_listen, (struct sockaddr*)&sa, sizeof(sa)) < 0) {
        perror("[net] bind");
        close(fd_listen);
        return -1;
    }
    if (listen(fd_listen, 128) < 0) { // backlog
        perror("[net] listen");
        close(fd_listen);
        return -1;
    }

    // Debug: report actual bound address/port
    {
        struct sockaddr_storage got; socklen_t glen = sizeof(got);
        if (getsockname(fd_listen, (struct sockaddr*)&got, &glen) == 0) {
            char ip[INET6_ADDRSTRLEN] = {0};
            uint16_t p = 0;
            if (got.ss_family == AF_INET) {
                struct sockaddr_in *a = (struct sockaddr_in*)&got;
                inet_ntop(AF_INET, &a->sin_addr, ip, sizeof ip);
                p = ntohs(a->sin_port);
            } else if (got.ss_family == AF_INET6) {
                struct sockaddr_in6 *a6 = (struct sockaddr_in6*)&got;
                inet_ntop(AF_INET6, &a6->sin6_addr, ip, sizeof ip);
                p = ntohs(a6->sin6_port);
            }
            fprintf(stderr, "[net] actually listening on %s:%u\n", ip, p);
        } else {
            perror("[net] getsockname");
        }
    }

    *listen_fd_out = fd_listen;
    return 0;
}

/**
 * @brief Initiates a non-blocking connect to host:port.
 * @return 0 on success (or in-progress), -1 on immediate failure.
 */
static int pgnet_connect_nonblocking(const char *host, int port, int *conn_fd_out) {
    struct addrinfo hints, *ai = NULL, *rp = NULL;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;      // force IPv4 to match INADDR_ANY listener
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;

    char portstr[16];
    snprintf(portstr, sizeof(portstr), "%d", port);

    int rc_gai = getaddrinfo(host, portstr, &hints, &ai);
    if (rc_gai != 0) {
        fprintf(stderr, "getaddrinfo(%s:%s) failed: %s\n", host, portstr, gai_strerror(rc_gai));
        return -1;
    }

    int fd = -1;
    int rc = -1;
    for (rp = ai; rp; rp = rp->ai_next) {
        fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (fd < 0) {
            continue;
        }
        // Non-blocking
        fcntl(fd, F_SETFL, O_NONBLOCK);
        rc = connect(fd, rp->ai_addr, rp->ai_addrlen);
        if (rc == 0 || (rc != 0 && errno == EINPROGRESS)) {
            // Success (immediate or in-progress)
            *conn_fd_out = fd;
            freeaddrinfo(ai);
            return 0;
        }
        // Immediate failure for this addrinfo; try next
        close(fd);
        fd = -1;
    }

    freeaddrinfo(ai);
    return -1;
}

/**
 * @brief Polls until the inbound accept (from left) and outbound connect (to right) complete.
 * Retries connect on transient failures until deadline.
 * @return 0 on success, -1 on timeout or error.
 */
static int pgnet_poll_until_ready(int listen_fd, const char *right_host, int right_port,
                                  int timeout_ms, int backoff_ms, int rank,
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
                fprintf(stderr, "[net] rank=%d accept left OK\n", rank);
                // Optional: report peer endpoint
                struct sockaddr_in peer; socklen_t pl = sizeof peer;
                if (getpeername(left_fd, (struct sockaddr*)&peer, &pl) == 0) {
                    char ip[INET_ADDRSTRLEN] = {0};
                    inet_ntop(AF_INET, &peer.sin_addr, ip, sizeof ip);
                    fprintf(stderr, "[net] rank=%d accepted from left %s:%u\n", rank, ip, ntohs(peer.sin_port));
                }
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
                fprintf(stderr, "[net] rank=%d connect right OK\n", rank);
                // Optional: report peer endpoint
                struct sockaddr_in peer; socklen_t prl = sizeof peer;
                if (getpeername(right_fd, (struct sockaddr*)&peer, &prl) == 0) {
                    char ip[INET_ADDRSTRLEN] = {0};
                    inet_ntop(AF_INET, &peer.sin_addr, ip, sizeof ip);
                    fprintf(stderr, "[net] rank=%d connected to right %s:%u\n", rank, ip, ntohs(peer.sin_port));
                }
            } else {
                // transient failure: retry until deadline
                if (err == ECONNREFUSED || err == ETIMEDOUT || err == EHOSTUNREACH ||
                    err == ENETUNREACH || err == ENETDOWN || err == EADDRNOTAVAIL) {
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

        // If we have an in-progress connect but poll didn't flag it, periodically probe SO_ERROR.
        if (right_fd < 0 && conn_fd >= 0 && pfds[1].revents == 0) {
            int err = 0; socklen_t errlen = sizeof(err);
            if (getsockopt(conn_fd, SOL_SOCKET, SO_ERROR, &err, &errlen) == 0 && err == 0) {
                right_fd = conn_fd;
                conn_fd = -1;
                pfds[1].fd = -1;
                fprintf(stderr, "[net] rank=%d connect right OK\n", rank);
                // Optional: report peer endpoint
                struct sockaddr_in peer; socklen_t prl = sizeof peer;
                if (getpeername(right_fd, (struct sockaddr*)&peer, &prl) == 0) {
                    char ip[INET_ADDRSTRLEN] = {0};
                    inet_ntop(AF_INET, &peer.sin_addr, ip, sizeof ip);
                    fprintf(stderr, "[net] rank=%d connected to right %s:%u\n", rank, ip, ntohs(peer.sin_port));
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

    // 2. Populate local bootstrap info for each neighbor/direction.
    struct qp_boot my_left;  // info for our QP that talks to LEFT neighbor
    struct qp_boot my_right; // info for our QP that talks to RIGHT neighbor
    memset(&my_left, 0, sizeof(my_left));
    memset(&my_right, 0, sizeof(my_right));

    // Common fields
    memcpy(my_left.gid, &local_gid, 16);
    memcpy(my_right.gid, &local_gid, 16);
    my_left.addr = my_right.addr = (uintptr_t)pg->mr->addr;
    my_left.rkey = my_right.rkey = pg->mr->rkey;
    my_left.bytes = my_right.bytes = pg->mr->length;

    // Direction-specific fields
    my_left.qpn  = pg->qp_left->qp_num;
    my_right.qpn = pg->qp_right->qp_num;

    // Generate high-quality, non-zero PSNs using getrandom()
    uint32_t psns[2] = {0};
    if (getrandom(psns, sizeof(psns), 0) != sizeof(psns)) {
        // Fallback for environments without getrandom (less ideal)
        struct timespec ts; clock_gettime(CLOCK_REALTIME, &ts);
        long seed = (long)ts.tv_nsec ^ (long)ts.tv_sec ^ (long)getpid() ^ (long)pg->rank;
        srand48(seed);
        psns[0] = (uint32_t)lrand48();
        psns[1] = (uint32_t)lrand48();
    }
    my_left.psn  = (psns[0] & 0xFFFFFF) ?: 1; // ensure not 0
    my_right.psn = (psns[1] & 0xFFFFFF) ?: 1; // ensure not 0

    // 3. Exchange bootstrap info with neighbors.
    // Classic ring-safe ordering:
    //   - rank 0: send-first to right, then recv-first from left
    //   - others: recv-first from left, then send-first to right
    if (pg->rank == 0) {
        fprintf(stderr, "[boot] (rank0) exch with right (send-first): send qp_right=%u psn=%u, then recv peer_left\n", my_right.qpn, my_right.psn);
        if (pgnet_exchange_qp(right_fd, &my_right, &pg->right_qp, 1) != 0) {
            fprintf(stderr, "Bootstrap exchange with right neighbor failed\n");
            return -1;
        }
        fprintf(stderr, "[boot] (rank0) exch with left (recv-first): recv peer_right, then send qp_left=%u psn=%u\n", my_left.qpn, my_left.psn);
        if (pgnet_exchange_qp(left_fd, &my_left, &pg->left_qp, 0) != 0) {
            fprintf(stderr, "Bootstrap exchange with left neighbor failed\n");
            return -1;
        }
    } else {
        fprintf(stderr, "[boot] exch with left (recv-first): recv peer_right, then send qp_left=%u psn=%u\n", my_left.qpn, my_left.psn);
        if (pgnet_exchange_qp(left_fd, &my_left, &pg->left_qp, 0) != 0) {
            fprintf(stderr, "Bootstrap exchange with left neighbor failed\n");
            return -1;
        }
        fprintf(stderr, "[boot] exch with right (send-first): send qp_right=%u psn=%u, then recv peer_left\n", my_right.qpn, my_right.psn);
        if (pgnet_exchange_qp(right_fd, &my_right, &pg->right_qp, 1) != 0) {
            fprintf(stderr, "Bootstrap exchange with right neighbor failed\n");
            return -1;
        }
    }

    // 4. Dump received remote info for diagnostics, then transition to RTR/RTS.
    fprintf(stderr, "[boot] rx left:  qpn=%u psn=%u rkey=%u bytes=%u gid0=%02x\n",
            pg->left_qp.qpn, pg->left_qp.psn, pg->left_qp.rkey, pg->left_qp.bytes, pg->left_qp.gid[0]);
    fprintf(stderr, "[boot] rx right: qpn=%u psn=%u rkey=%u bytes=%u gid0=%02x\n",
            pg->right_qp.qpn, pg->right_qp.psn, pg->right_qp.rkey, pg->right_qp.bytes, pg->right_qp.gid[0]);
    // 4. Transition local QPs to RTR and RTS using received remote info.
    enum ibv_mtu mtu = IBV_MTU_1024;
    if (rdma_qp_to_rtr(pg->qp_left, &pg->left_qp, pg->ib_port, pg->gid_index, mtu) != 0) return -1;
    if (rdma_qp_to_rtr(pg->qp_right, &pg->right_qp, pg->ib_port, pg->gid_index, mtu) != 0) return -1;
    if (rdma_qp_to_rts(pg->qp_left, my_left.psn) != 0) return -1;
    if (rdma_qp_to_rts(pg->qp_right, my_right.psn) != 0) return -1;

    // 5. Query and store the actual max inline data size supported.
    if (rdma_qp_query_inline(pg->qp_left, &pg->max_inline_data) != 0) {
        pg->max_inline_data = 0; // Fallback if query fails
    }

    // 6. Post-RTS ready handshake on TCP to prevent early RDMA sends before peers are ready.
    //    This keeps public API unchanged while adding robustness.
    uint8_t tok = 0xA5, peer = 0;
    ssize_t n;
    if (pg->rank == 0) {
        // rank0: notify right first, then wait for left
        n = send(right_fd, &tok, 1, 0);
        if (n != 1) { perror("send ready->right"); return -1; }
        n = recv(left_fd, &peer, 1, MSG_WAITALL);
        if (n != 1) { perror("recv ready<-left"); return -1; }
        fprintf(stderr, "[boot] (rank0) ready handshake OK (right then left)\n");
    } else {
        // others: wait from left first, then notify right
        n = recv(left_fd, &peer, 1, MSG_WAITALL);
        if (n != 1) { perror("recv ready<-left"); return -1; }
        n = send(right_fd, &tok, 1, 0);
        if (n != 1) { perror("send ready->right"); return -1; }
        fprintf(stderr, "[boot] ready handshake OK (left then right)\n");
    }

    return 0;
}

/**
 * @brief Orchestrates the entire ring connection and bootstrap process.
 */
int pgnet_ring_connect(struct pg *pg) {
    int right = (pg->rank + 1) % pg->world_size;

    // Base settings with env overrides
    int base_port = pg->port ? pg->port : PG_DEFAULT_PORT;
    base_port = getenv_int("PG_PORT", base_port);
    int timeout_ms = getenv_int("PG_CONNECT_TIMEOUT_MS", PG_DEFAULT_CONNECT_TIMEOUT_MS);
    int backoff_ms = getenv_int("PG_BACKOFF_MS", PG_DEFAULT_BACKOFF_MS);

    // Per-rank ports (consistent modulo scheme for all ranks)
    int listen_port = base_port + (pg->rank % 10000);
    int right_rank  = (pg->rank + 1) % pg->world_size;
    int right_port  = base_port + (right_rank % 10000);
    int left_rank   = (pg->rank - 1 + pg->world_size) % pg->world_size;
    int left_port   = base_port + (left_rank % 10000); (void)left_port; // for clarity/logs if needed
    const char *right_host = pg->hosts[right];

    fprintf(stderr, "[net] rank=%d listening on port %d, connecting right=%s:%d\n",
            pg->rank, listen_port, right_host, right_port);

#ifdef PG_DEBUG
    fprintf(stderr, "[net] rank=%d listen=%s:%d right=%s:%d timeout=%dms\n",
            pg->rank, "*", listen_port, pg->hosts[right], right_port, timeout_ms);
#endif

    // Avoid SIGPIPE crashes on send() after peer closes
    signal(SIGPIPE, SIG_IGN);

    int listen_fd = -1, left_fd = -1, right_fd = -1;
    if (pgnet_setup_listen_socket(listen_port, &listen_fd) != 0) goto fail;

    // Single loop handles both accept and connect with retry/backoff until timeout
    if (pgnet_poll_until_ready(listen_fd, pg->hosts[right], right_port,
                               timeout_ms, backoff_ms, pg->rank, &left_fd, &right_fd) != 0) {
        goto fail;
    }

    close(listen_fd);
    listen_fd = -1;

    // Store FDs in handle; they will be closed by pg_close()
    pg->left_fd = left_fd;
    pg->right_fd = right_fd;

    // Switch established sockets to blocking for robust full read/write
    int fl;
    if ((fl = fcntl(pg->left_fd, F_GETFL, 0)) >= 0) fcntl(pg->left_fd, F_SETFL, fl & ~O_NONBLOCK);
    if ((fl = fcntl(pg->right_fd, F_GETFL, 0)) >= 0) fcntl(pg->right_fd, F_SETFL, fl & ~O_NONBLOCK);

    if (pgnet_exchange_bootstrap(pg->left_fd, pg->right_fd, pg) != 0) {
        goto fail;
    }

    // Sockets are intentionally left open for potential use by test harnesses (e.g., barriers)
    // pg_close() is responsible for closing them.
    return 0;

fail:
    if (listen_fd >= 0) close(listen_fd);
    if (left_fd >= 0) close(left_fd);
    if (right_fd >= 0) close(right_fd);
    return -1;
}
