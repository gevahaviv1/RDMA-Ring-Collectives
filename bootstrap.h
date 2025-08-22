#ifndef BOOTSTRAP_H
#define BOOTSTRAP_H

#include <stddef.h>
#include <stdint.h>

/*
 * Establish a pair of TCP connections forming a ring.
 *
 * hosts: array of hostnames of size count.
 * count: total number of hosts.
 * my_index: index of the current host within the hosts array.
 * port_base: base port; each host listens on port_base + its index.
 * timeout_ms: maximum time to wait for connections.
 * fd_from_left: on success, connected socket from left neighbor.
 * fd_to_right:  on success, connected socket to right neighbor.
 *
 * Returns 0 on success, -1 on failure (with resources cleaned up).
 */
int bootstrap_ring(const char **hosts, size_t count, size_t my_index,
                   int port_base, int timeout_ms,
                   int *fd_from_left, int *fd_to_right);

/* Packed exchange structure for QP bootstrap */
struct qp_boot {
    uint32_t qpn;
    uint32_t psn;
    uint16_t lid;
    uint8_t gid[16];
    uint64_t base_addr;
    uint32_t rkey;
    uint64_t bytes;
} __attribute__((packed));

/*
 * Exchange qp_boot blobs with both neighbors.
 *
 * fd_from_left: socket connected to left neighbor.
 * fd_to_right:  socket connected to right neighbor.
 * mine:         information to send.
 * left:         on success, info received from left neighbor.
 * right:        on success, info received from right neighbor.
 * timeout_ms:   poll timeout for each send/recv attempt.
 *
 * Returns 0 on success, -1 on failure.
 */
int exchange_qp_boot(int fd_from_left, int fd_to_right,
                     const struct qp_boot *mine,
                     struct qp_boot *left, struct qp_boot *right,
                     int timeout_ms);

#endif /* BOOTSTRAP_H */
