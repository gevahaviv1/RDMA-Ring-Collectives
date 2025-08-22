#define _POSIX_C_SOURCE 200112L
#include "bootstrap.h"
#include "ring.h"

#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

static int set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return -1;
    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) return -1;
    return 0;
}

static int set_blocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return -1;
    if (fcntl(fd, F_SETFL, flags & ~O_NONBLOCK) < 0) return -1;
    return 0;
}

int bootstrap_ring(const char **hosts, size_t count, size_t my_index,
                   int port_base, int timeout_ms,
                   int *fd_from_left, int *fd_to_right) {
    if (!hosts || count == 0 || my_index >= count || !fd_from_left || !fd_to_right)
        return -1;

    int listen_fd = -1;
    int connect_fd = -1;
    int left_fd = -1;
    int right_fd = -1;

    int rc = -1; /* default to failure */

    int right_index = right_of((int)my_index, (int)count);

    char portstr[16];
    struct addrinfo hints, *ai = NULL, *p;

    /* setup listening socket */
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;
    snprintf(portstr, sizeof(portstr), "%d", port_base + (int)my_index);
    if (getaddrinfo(NULL, portstr, &hints, &ai) != 0)
        goto out;
    for (p = ai; p; p = p->ai_next) {
        listen_fd = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if (listen_fd < 0)
            continue;
        int yes = 1;
        setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
        if (set_nonblocking(listen_fd) < 0) {
            close(listen_fd);
            listen_fd = -1;
            continue;
        }
        if (bind(listen_fd, p->ai_addr, p->ai_addrlen) == 0 &&
            listen(listen_fd, 1) == 0)
            break;
        close(listen_fd);
        listen_fd = -1;
    }
    freeaddrinfo(ai);
    ai = NULL;
    if (listen_fd < 0)
        goto out;

    /* setup outgoing connection */
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    snprintf(portstr, sizeof(portstr), "%d", port_base + right_index);
    if (getaddrinfo(hosts[right_index], portstr, &hints, &ai) != 0)
        goto out;
    for (p = ai; p; p = p->ai_next) {
        connect_fd = socket(p->ai_family, p->ai_socktype, p->ai_protocol);
        if (connect_fd < 0)
            continue;
        if (set_nonblocking(connect_fd) < 0) {
            close(connect_fd);
            connect_fd = -1;
            continue;
        }
        if (connect(connect_fd, p->ai_addr, p->ai_addrlen) == 0) {
            right_fd = connect_fd;
        } else if (errno == EINPROGRESS) {
            right_fd = -1; /* will complete later */
        } else {
            close(connect_fd);
            connect_fd = -1;
            continue;
        }
        break;
    }
    freeaddrinfo(ai);
    ai = NULL;
    if (connect_fd < 0)
        goto out;
    if (right_fd >= 0) {
        /* connection completed immediately */
        connect_fd = right_fd;
    }

    /* poll loop */
    struct pollfd pfds[2];
    struct timespec start, now;
    clock_gettime(CLOCK_MONOTONIC, &start);
    int remaining = timeout_ms;

    while ((left_fd < 0 || right_fd < 0) && remaining > 0) {
        int idx = 0;
        if (left_fd < 0) {
            pfds[idx].fd = listen_fd;
            pfds[idx].events = POLLIN;
            pfds[idx].revents = 0;
            idx++;
        }
        if (right_fd < 0) {
            pfds[idx].fd = connect_fd;
            pfds[idx].events = POLLOUT;
            pfds[idx].revents = 0;
            idx++;
        }
        int prc = poll(pfds, idx, remaining);
        if (prc < 0)
            goto out;
        if (prc == 0)
            break; /* timeout */

        int offset = 0;
        if (left_fd < 0 && pfds[offset].fd == listen_fd) {
            if (pfds[offset].revents & POLLIN) {
                left_fd = accept(listen_fd, NULL, NULL);
                if (left_fd >= 0) {
                    set_blocking(left_fd);
                    close(listen_fd);
                } else if (errno != EAGAIN && errno != EWOULDBLOCK) {
                    goto out;
                }
            }
            offset++;
        }
        if (right_fd < 0 && pfds[offset].fd == connect_fd) {
            if (pfds[offset].revents & (POLLOUT | POLLERR | POLLHUP)) {
                int err = 0;
                socklen_t len = sizeof(err);
                if (getsockopt(connect_fd, SOL_SOCKET, SO_ERROR, &err, &len) < 0 || err != 0) {
                    goto out;
                }
                right_fd = connect_fd;
                set_blocking(right_fd);
            }
        }

        clock_gettime(CLOCK_MONOTONIC, &now);
        long elapsed = (now.tv_sec - start.tv_sec) * 1000L +
                       (now.tv_nsec - start.tv_nsec) / 1000000L;
        remaining = timeout_ms - (int)elapsed;
    }

    if (left_fd < 0 || right_fd < 0)
        goto out;

    *fd_from_left = left_fd;
    *fd_to_right = right_fd;
    rc = 0;

out:
    if (rc != 0) {
        if (left_fd >= 0) close(left_fd);
        if (right_fd >= 0) close(right_fd);
    }
    if (listen_fd >= 0) close(listen_fd);
    if (connect_fd >= 0 && connect_fd != right_fd) close(connect_fd);
    return rc;
}

