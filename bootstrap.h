#ifndef BOOTSTRAP_H
#define BOOTSTRAP_H

#include <stddef.h>

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

#endif /* BOOTSTRAP_H */
