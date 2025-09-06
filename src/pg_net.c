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
#include <inttypes.h>
#include <endian.h>

#include "pg_internal.h"
#include "RDMA_api.h"

/*
 * This file implements the TCP-based bootstrap networking for the process group.
 * It handles socket creation, ring-based connections, and the exchange of RDMA
 * bootstrap information required to bring Queue Pairs (QPs) to a connected state.
 */

//==============================================================================
// Bootstrap Pack Helpers
//==============================================================================

// Fixed, packed wire format (network byte order) for qp_boot exchange
struct wire_boot {
    uint32_t qpn;
    uint32_t psn;
    uint32_t lid;
    uint32_t rkey;
    uint64_t bytes;
    uint8_t  gid[16];
} __attribute__((packed));

static void boot_to_wire(const struct qp_boot *in, struct wire_boot *out) {
    out->qpn   = htonl(in->qpn);
    out->psn   = htonl(in->psn);
    out->lid   = htonl(in->lid);
    out->rkey  = htonl(in->rkey);
    out->bytes = htobe64(in->bytes);
    memcpy(out->gid, in->gid, 16);
}

static void wire_to_boot(const struct wire_boot *in, struct qp_boot *out) {
    out->qpn   = ntohl(in->qpn);
    out->psn   = ntohl(in->psn);
    out->lid   = ntohl(in->lid);
    out->rkey  = ntohl(in->rkey);
    out->bytes = be64toh(in->bytes);
    memcpy(out->gid, in->gid, 16);
}

// Pack the blob we send to our RIGHT neighbor: it must contain the QP the
// RIGHT neighbor will target (our local QP that talks to RIGHT neighbor).
static void pack_boot_for_right(struct pg *pg, const union ibv_gid *gid,
                                uint16_t lid, uint32_t psn, struct qp_boot *b) {
    memset(b, 0, sizeof(*b));
    // Export our LEFT-facing QP so RIGHT neighbor can program its qp_left
    b->qpn   = pg->qp_left->qp_num;
    b->psn   = psn;
    b->lid   = lid;
    memcpy(b->gid, gid, 16);
    b->addr  = (uintptr_t)pg->mr->addr;
    b->rkey  = pg->mr->rkey;
    b->bytes = pg->mr->length;
}

// Pack the blob we send to our LEFT neighbor: it must contain the QP the
// LEFT neighbor will target (our local QP that talks to LEFT neighbor).
static void pack_boot_for_left(struct pg *pg, const union ibv_gid *gid,
                               uint16_t lid, uint32_t psn, struct qp_boot *b) {
    memset(b, 0, sizeof(*b));
    // Export our RIGHT-facing QP so LEFT neighbor can program its qp_right
    b->qpn   = pg->qp_right->qp_num;
    b->psn   = psn;
    b->lid   = lid;
    memcpy(b->gid, gid, 16);
    b->addr  = (uintptr_t)pg->mr->addr;
    b->rkey  = pg->mr->rkey;
    b->bytes = pg->mr->length;
}

//==============================================================================
// Socket I/O Helpers
//==============================================================================

// Robust, full-length write/read helpers using POSIX write/read.
static int writen(int fd, const void *buf, size_t n) {
    const uint8_t *p = (const uint8_t *)buf;
    size_t off = 0;
    while (off < n) {
        ssize_t r = write(fd, p + off, n - off);
        if (r > 0) {
            off += (size_t)r;
        } else if (r < 0 && (errno == EINTR || errno == EAGAIN)) {
            continue;
        } else {
            return -1;
        }
    }
    return 0;
}

static int readn(int fd, void *buf, size_t n) {
    uint8_t *p = (uint8_t *)buf;
    size_t off = 0;
    while (off < n) {
        ssize_t r = read(fd, p + off, n - off);
        if (r > 0) {
            off += (size_t)r;
        } else if (r == 0) {
            return -1; // EOF
        } else if (errno == EINTR || errno == EAGAIN) {
            continue;
        } else {
            return -1;
        }
    }
    return 0;
}

//==============================================================================
// GID Debug Helpers
//==============================================================================

static void print_gid_hex(const uint8_t gid[16]) {
    fprintf(stderr,
            "%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x:%02x",
            gid[0], gid[1], gid[2], gid[3], gid[4], gid[5], gid[6], gid[7],
            gid[8], gid[9], gid[10], gid[11], gid[12], gid[13], gid[14], gid[15]);
}

static void dump_port_and_gids(struct ibv_context *ctx, uint8_t port, int limit) {
    struct ibv_port_attr pa; memset(&pa, 0, sizeof(pa));
    if (ibv_query_port(ctx, port, &pa) == 0) {
        fprintf(stderr, "[rdma] port %u link_layer=%s active_mtu=%u lid=%u\n",
                (unsigned)port,
                (pa.link_layer == IBV_LINK_LAYER_ETHERNET ? "ETHERNET" : "INFINIBAND"),
                (unsigned)pa.active_mtu, (unsigned)pa.lid);
    }
    union ibv_gid g; memset(&g, 0, sizeof(g));
    for (int i = 0; i < limit; ++i) {
        if (ibv_query_gid(ctx, port, (uint8_t)i, &g) != 0) break;
        fprintf(stderr, "[rdma] gid[%d]=", i);
        print_gid_hex(g.raw);
        fprintf(stderr, "\n");
    }
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
enum exch_mode { SEND_FIRST = 0, RECV_FIRST = 1 };

static int exch_boot_with_ack(int fd, enum exch_mode mode,
                              const struct qp_boot *my,
                              struct qp_boot *peer) {
    struct wire_boot wb_my, wb_peer;
    uint32_t ack; // host order

    if (mode == SEND_FIRST) {
        boot_to_wire(my, &wb_my);
        if (writen(fd, &wb_my, sizeof(wb_my)) != 0) return -1;
        if (readn(fd, &wb_peer, sizeof(wb_peer)) != 0) return -1;
        wire_to_boot(&wb_peer, peer);

        // ACK: tell peer we received its struct by echoing peer->qpn
        ack = htonl(peer->qpn);
        if (writen(fd, &ack, sizeof(ack)) != 0) return -1;
        // Expect peer to echo our qpn back
        if (readn(fd, &ack, sizeof(ack)) != 0) return -1;
        ack = ntohl(ack);
        if (ack != my->qpn) {
            fprintf(stderr, "[boot] ACK mismatch (send-first): my_sent=%u peer_echo=%u\n",
                    my->qpn, ack);
            return -1;
        }
        // ack ok for send-first handled by unified print at end
    } else { // RECV_FIRST
        if (readn(fd, &wb_peer, sizeof(wb_peer)) != 0) return -1;
        wire_to_boot(&wb_peer, peer);
        boot_to_wire(my, &wb_my);
        if (writen(fd, &wb_my, sizeof(wb_my)) != 0) return -1;

        // Expect peer to send us our qpn as ACK
        if (readn(fd, &ack, sizeof(ack)) != 0) return -1;
        ack = ntohl(ack);
        if (ack != my->qpn) {
            fprintf(stderr, "[boot] PEER ACK mismatch (recv-first): want=%u got=%u\n", my->qpn, ack);
            return -1;
        }
        // Echo back that we received peer by sending peer->qpn
        ack = htonl(peer->qpn);
        if (writen(fd, &ack, sizeof(ack)) != 0) return -1;
        // ack ok for recv-first handled by unified print at end
    }
    // Unified success print after either mode completes successfully
    fprintf(stderr, "[boot] ack ok: my=%u peer=%u (%s)\n",
            my->qpn, peer->qpn, mode == SEND_FIRST ? "send-first" : "recv-first");
    return 0;
}

static int pgnet_exchange_qp(int fd, const struct qp_boot *local, struct qp_boot *remote, int send_first) {
    enum exch_mode mode = send_first ? SEND_FIRST : RECV_FIRST;
    return exch_boot_with_ack(fd, mode, local, remote);
}

/**
 * @brief Main bootstrap logic: gathers local RDMA info, exchanges it with neighbors,
 * and transitions QPs to a connected state.
 * @return 0 on success, -1 on failure.
 */
static int pgnet_exchange_bootstrap(int left_fd, int right_fd, struct pg *pg) {
    // 1. If on RoCE and no explicit PG_GID_INDEX, auto-pick a suitable GID index.
    {
        struct ibv_port_attr pa; memset(&pa, 0, sizeof(pa));
        if (ibv_query_port(pg->ctx, pg->ib_port, &pa) == 0 && pa.link_layer == IBV_LINK_LAYER_ETHERNET) {
            const char *env_gidx = getenv("PG_GID_INDEX");
            if (!env_gidx || !*env_gidx) {
                struct sockaddr_in local_ip; socklen_t llen = sizeof local_ip;
                memset(&local_ip, 0, sizeof local_ip);
                if (getsockname(right_fd, (struct sockaddr*)&local_ip, &llen) == 0 && local_ip.sin_family == AF_INET) {
                    uint32_t my_ip_be = local_ip.sin_addr.s_addr; // network byte order
                    union ibv_gid g; memset(&g, 0, sizeof g);
                    int picked = -1, fallback = -1;
                    int limit = pa.gid_tbl_len > 128 ? 128 : pa.gid_tbl_len;
                    for (int i = 0; i < limit; ++i) {
                        if (ibv_query_gid(pg->ctx, pg->ib_port, (uint8_t)i, &g) != 0) continue;
                        int nonzero = 0; for (int b = 0; b < 16; ++b) { if (g.raw[b]) { nonzero = 1; break; } }
                        if (!nonzero) continue;
                        if (fallback < 0) fallback = i;
                        uint32_t gid_ipv4_be = 0; memcpy(&gid_ipv4_be, &g.raw[12], 4);
                        if (gid_ipv4_be == my_ip_be) { picked = i; break; }
                    }
                    if (picked >= 0) {
                        fprintf(stderr, "[boot] auto-picked gid_index=%d matching local ip %s\n",
                                picked, inet_ntoa(local_ip.sin_addr));
                        pg->gid_index = (uint8_t)picked;
                    } else if (fallback >= 0) {
                        fprintf(stderr, "[boot] auto-picked first non-zero gid_index=%d (no IPv4 match)\n",
                                fallback);
                        pg->gid_index = (uint8_t)fallback;
                    } else {
                        fprintf(stderr, "[boot] warning: no non-zero GIDs found on port %u; keeping gid_index=%u\n",
                                (unsigned)pg->ib_port, (unsigned)pg->gid_index);
                    }
                }
            }
        }
    }

    // 1b. Query local addressing: GID for RoCE and LID for InfiniBand
    union ibv_gid local_gid;
    if (rdma_query_gid(pg->ctx, pg->ib_port, pg->gid_index, &local_gid) != 0) {
        return -1;
    }
    fprintf(stderr, "[boot] using gid_index=%u local_gid=", (unsigned)pg->gid_index);
    print_gid_hex(local_gid.raw);
    fprintf(stderr, "\n");
    struct ibv_port_attr port_attr;
    memset(&port_attr, 0, sizeof(port_attr));
    if (ibv_query_port(pg->ctx, pg->ib_port, &port_attr) != 0) {
        fprintf(stderr, "Failed to query port attributes for port %u\n", pg->ib_port);
        return -1;
    }

    // 2. Populate local bootstrap info for each neighbor/direction using helpers.
    struct qp_boot my_left;   // blob to send to LEFT neighbor (targets our qp_left)
    struct qp_boot my_right;  // blob to send to RIGHT neighbor (targets our qp_right)

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
    uint32_t psn_left  = (psns[0] & 0xFFFFFF) ?: 1; // ensure not 0
    uint32_t psn_right = (psns[1] & 0xFFFFFF) ?: 1; // ensure not 0

    // Ensure PSN in each blob matches the exported local QP
    pack_boot_for_left(pg,  &local_gid, port_attr.lid, psn_right, &my_left);  // exports qp_right
    pack_boot_for_right(pg, &local_gid, port_attr.lid, psn_left,  &my_right); // exports qp_left

    // 3. Exchange bootstrap info with neighbors.
    // Classic ring-safe ordering:
    //   - rank 0: send-first to right, then recv-first from left
    //   - others: recv-first from left, then send-first to right
    if (pg->rank == 0) {
        fprintf(stderr, "[boot] (rank0) exch with right (send-first): send my L=%u psn=%u, then recv peer_left\n", my_right.qpn, my_right.psn);
        fprintf(stderr, "[boot] (rank%d) exch with right: sending my L=%u, expecting peer_left\n",
                pg->rank, pg->qp_left->qp_num);
        if (pgnet_exchange_qp(right_fd, &my_right, &pg->right_qp, 1) != 0) {
            fprintf(stderr, "Bootstrap exchange with right neighbor failed\n");
            return -1;
        }
        fprintf(stderr, "[boot] (rank0) exch with left (recv-first): recv peer_right, then send my R=%u psn=%u\n", my_left.qpn, my_left.psn);
        fprintf(stderr, "[boot] (rank%d) exch with left:  sending my R=%u, expecting peer_right\n",
                pg->rank, pg->qp_right->qp_num);
        if (pgnet_exchange_qp(left_fd, &my_left, &pg->left_qp, 0) != 0) {
            fprintf(stderr, "Bootstrap exchange with left neighbor failed\n");
            return -1;
        }
    } else {
        fprintf(stderr, "[boot] exch with left (recv-first): recv peer_right, then send my R=%u psn=%u\n", my_left.qpn, my_left.psn);
        fprintf(stderr, "[boot] (rank%d) exch with left:  sending my R=%u, expecting peer_right\n",
                pg->rank, pg->qp_right->qp_num);
        if (pgnet_exchange_qp(left_fd, &my_left, &pg->left_qp, 0) != 0) {
            fprintf(stderr, "Bootstrap exchange with left neighbor failed\n");
            return -1;
        }
        fprintf(stderr, "[boot] exch with right (send-first): send my L=%u psn=%u, then recv peer_left\n", my_right.qpn, my_right.psn);
        fprintf(stderr, "[boot] (rank%d) exch with right: sending my L=%u, expecting peer_left\n",
                pg->rank, pg->qp_left->qp_num);
        if (pgnet_exchange_qp(right_fd, &my_right, &pg->right_qp, 1) != 0) {
            fprintf(stderr, "Bootstrap exchange with right neighbor failed\n");
            return -1;
        }
    }

    // Neighbor blob debug after TCP exchange
    fprintf(stderr,
            "[nbr] rank=%d left_blob{qpn=%u lid=%u psn=%u} right_blob{qpn=%u lid=%u psn=%u}\n",
            pg->rank,
            pg->left_qp.qpn, (unsigned)pg->left_qp.lid, pg->left_qp.psn,
            pg->right_qp.qpn, (unsigned)pg->right_qp.lid, pg->right_qp.psn);

    // 4. Dump received remote info for diagnostics, then transition to RTR/RTS.
    fprintf(stderr, "[boot] rx left:  qpn=%u psn=%u rkey=%u bytes=%u gid=",
            pg->left_qp.qpn, pg->left_qp.psn, pg->left_qp.rkey, pg->left_qp.bytes);
    print_gid_hex(pg->left_qp.gid);
    fprintf(stderr, "\n");
    fprintf(stderr, "[boot] rx right: qpn=%u psn=%u rkey=%u bytes=%u gid=",
            pg->right_qp.qpn, pg->right_qp.psn, pg->right_qp.rkey, pg->right_qp.bytes);
    print_gid_hex(pg->right_qp.gid);
    fprintf(stderr, "\n");
    // 4. Transition local QPs to RTR and RTS using received remote info.
    enum ibv_mtu mtu = IBV_MTU_1024;
    int use_gid = (port_attr.link_layer == IBV_LINK_LAYER_ETHERNET);
    // Show LID/GID path used for each direction (helps diagnose fabric issues)
    fprintf(stderr, "[rtr] rank=%d using %s to remote qpn=%u lid=%u (LEFT dir)\n",
            pg->rank, use_gid ? "GRH" : "LID", (unsigned)pg->left_qp.qpn, (unsigned)pg->left_qp.lid);
    if (rdma_qp_to_rtr(pg->qp_left, &pg->left_qp, pg->ib_port, pg->gid_index, mtu) != 0) return -1;
    fprintf(stderr, "[rtr] rank=%d using %s to remote qpn=%u lid=%u (RIGHT dir)\n",
            pg->rank, use_gid ? "GRH" : "LID", (unsigned)pg->right_qp.qpn, (unsigned)pg->right_qp.lid);
    if (rdma_qp_to_rtr(pg->qp_right, &pg->right_qp, pg->ib_port, pg->gid_index, mtu) != 0) return -1;
    if (rdma_qp_to_rts(pg->qp_left, my_left.psn) != 0) return -1;
    if (rdma_qp_to_rts(pg->qp_right, my_right.psn) != 0) return -1;

    // Mapping debug after QPs reach RTS
    fprintf(stderr,
            "[map] rank=%d local_qps{L=%u R=%u} remote_used{for_left: qpn=%u lid=%u psn=%u, for_right: qpn=%u lid=%u psn=%u}\n",
            pg->rank,
            pg->qp_left->qp_num, pg->qp_right->qp_num,
            (unsigned)pg->left_qp.qpn, (unsigned)pg->left_qp.lid, (unsigned)pg->left_qp.psn,
            (unsigned)pg->right_qp.qpn, (unsigned)pg->right_qp.lid, (unsigned)pg->right_qp.psn);

    // Cross-rank equality summary (eyeball across ranks):
    fprintf(stderr, "[eq] expect: right_blob.qpn == next.rank.qp_left; left_blob.qpn == prev.rank.qp_right\n");

#ifdef PG_DEBUG
    // Cross-rank mapping invariants (compact):
    // - Our left QP pairs with LEFT neighbor's RIGHT QP (stored in left_blob)
    // - Our right QP pairs with RIGHT neighbor's LEFT QP (stored in right_blob)
    fprintf(stderr,
            "[map] rank=%d local{L=%u R=%u} expect{L<-left.right: qpn=%u, R<-right.left: qpn=%u}\n",
            pg->rank,
            pg->qp_left->qp_num, pg->qp_right->qp_num,
            (unsigned)pg->left_qp.qpn,   // left neighbor's RIGHT qpn
            (unsigned)pg->right_qp.qpn); // right neighbor's LEFT qpn

    // Sanity orientation at RTR time (no swap):
    //   setup_rtr(qp_left,  left_blob)
    //   setup_rtr(qp_right, right_blob)
    fprintf(stderr,
            "[map] rank=%d RTR mapping: qp_left<=left_blob.qpn=%u, qp_right<=right_blob.qpn=%u\n",
            pg->rank, (unsigned)pg->left_qp.qpn, (unsigned)pg->right_qp.qpn);

    // These comments describe the ring invariants you can eyeball across hosts:
    // rank k: right_blob.qpn must equal rank (k+1): qp_left->qp_num
    // rank k: left_blob.qpn  must equal rank (k-1): qp_right->qp_num
#endif

    // 5. Query and store the actual max inline data size supported.
    if (rdma_qp_query_inline(pg->qp_left, &pg->max_inline_data) != 0) {
        pg->max_inline_data = 0; // Fallback if query fails
    }

    // 6. Post-RTS ready handshake on TCP to prevent early RDMA sends before peers are ready.
    //    This keeps public API unchanged while adding robustness.
    uint8_t tok = 0xA5, peer = 0;
    if (pg->rank == 0) {
        // rank0: notify right first, then wait for left
        if (writen(right_fd, &tok, 1) != 0) { perror("send ready->right"); return -1; }
        if (readn(left_fd, &peer, 1) != 0) { perror("recv ready<-left"); return -1; }
        fprintf(stderr, "[boot] (rank0) ready handshake OK (right then left)\n");
    } else {
        // others: wait from left first, then notify right
        if (readn(left_fd, &peer, 1) != 0) { perror("recv ready<-left"); return -1; }
        if (writen(right_fd, &tok, 1) != 0) { perror("send ready->right"); return -1; }
        fprintf(stderr, "[boot] ready handshake OK (left then right)\n");
    }

    // Optional debug: dump several GIDs to help troubleshoot addressing issues
    dump_port_and_gids(pg->ctx, pg->ib_port, 8);

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
