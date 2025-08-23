#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include "pg_internal.h"

struct qp_boot;
struct pg;

// Write exactly 'len' bytes from 'buf' to socket 'fd', handling short writes
// and EINTR. Returns 0 on success, -1 on error.
static int write_full(int fd, const void *buf, size_t len) {
  const char *p = (const char *)buf;
  while (len) {
    ssize_t n = send(fd, p, len, 0);
    if (n <= 0) {
      if (n < 0 && errno == EINTR) continue;
      return -1;
    }
    p += n;
    len -= (size_t)n;
  }
  return 0;
}

// Read exactly 'len' bytes into 'buf' from socket 'fd', handling short reads
// and EINTR. Returns 0 on success, -1 on error/EOF.
static int read_full(int fd, void *buf, size_t len) {
  char *p = (char *)buf;
  while (len) {
    ssize_t n = recv(fd, p, len, 0);
    if (n <= 0) {
      if (n < 0 && errno == EINTR) continue;
      return -1;
    }
    p += n;
    len -= (size_t)n;
  }
  return 0;
}

// Exchange bootstrap queue-pair info with a peer over socket 'fd'. If
// 'send_first' is non-zero, send then receive; otherwise receive then send.
// Returns 0 on success, -1 on error.
static int exchange_qp(int fd, const struct qp_boot *local,
                       struct qp_boot *remote, int send_first) {
  if (send_first) {
    if (write_full(fd, local, sizeof(*local)) != 0) return -1;
    if (read_full(fd, remote, sizeof(*remote)) != 0) return -1;
  } else {
    if (read_full(fd, remote, sizeof(*remote)) != 0) return -1;
    if (write_full(fd, local, sizeof(*local)) != 0) return -1;
  }
  return 0;
}

// Create a non-blocking listening socket bound to 'portstr'.
static int setup_listen_socket(const char *portstr, int *listen_fd_out) {
  struct addrinfo hints, *ai = NULL;
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = AI_PASSIVE;
  if (getaddrinfo(NULL, portstr, &hints, &ai) != 0) return -1;

  int fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
  if (fd < 0) {
    freeaddrinfo(ai);
    return -1;
  }

  fcntl(fd, F_SETFL, O_NONBLOCK);
  if (bind(fd, ai->ai_addr, ai->ai_addrlen) != 0) {
    freeaddrinfo(ai);
    close(fd);
    return -1;
  }
  int ok = (listen(fd, 1) == 0);
  freeaddrinfo(ai);
  if (!ok) {
    close(fd);
    return -1;
  }
  *listen_fd_out = fd;
  return 0;
}

// Start a non-blocking connect to 'host:portstr'.
static int connect_right_nonblocking(const char *host, const char *portstr,
                                     int *conn_fd_out) {
  struct addrinfo hints, *ai = NULL;
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  if (getaddrinfo(host, portstr, &hints, &ai) != 0) return -1;

  int fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
  if (fd < 0) {
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

// Poll until an inbound accept and the outbound connect complete.
static int poll_until_ready(int listen_fd, int conn_fd, int *left_fd_out,
                            int *right_fd_out) {
  struct pollfd pfds[2];
  pfds[0].fd = listen_fd;
  pfds[0].events = POLLIN;
  pfds[1].fd = conn_fd;
  pfds[1].events = POLLOUT;
  int timeout_ms = 10000;

  int left_fd = -1, right_fd = -1;
  for (;;) {
    int rc = poll(pfds, 2, timeout_ms);
    if (rc <= 0) break;

    if (left_fd < 0 && (pfds[0].revents & POLLIN)) {
      left_fd = accept(listen_fd, NULL, NULL);
      if (left_fd >= 0) {
        int fl = fcntl(left_fd, F_GETFL, 0);
        fcntl(left_fd, F_SETFL, fl & ~O_NONBLOCK);
      }
    }

    if (right_fd < 0 && (pfds[1].revents & POLLOUT)) {
      int err = 0;
      socklen_t errlen = sizeof(err);
      if (getsockopt(conn_fd, SOL_SOCKET, SO_ERROR, &err, &errlen) < 0 || err)
        break;
      right_fd = conn_fd;
      int fl = fcntl(right_fd, F_GETFL, 0);
      fcntl(right_fd, F_SETFL, fl & ~O_NONBLOCK);
    }

    if (left_fd >= 0 && right_fd >= 0) {
      *left_fd_out = left_fd;
      *right_fd_out = right_fd;
      return 0;
    }
  }

  if (left_fd >= 0) close(left_fd);
  if (right_fd >= 0 && right_fd != conn_fd) close(right_fd);
  return -1;
}

// Exchange bootstrap info with left/right neighbors using exchange_qp().
static int exchange_bootstrap(int left_fd, int right_fd, struct pg *pg) {
  struct qp_boot myinfo;
  memset(&myinfo, 0, sizeof(myinfo));
  if (exchange_qp(left_fd, &myinfo, &pg->left_qp, 0) != 0) return -1;
  if (exchange_qp(right_fd, &myinfo, &pg->right_qp, 1) != 0) return -1;
  pg->max_inline_data = 0;
  return 0;
}

int ring_connect(struct pg *pg) {
  int right = (pg->rank + 1) % pg->world;

  char portstr[16];
  snprintf(portstr, sizeof(portstr), "%d", pg->port);

  int listen_fd = -1, conn_fd = -1, left_fd = -1, right_fd = -1;

  if (setup_listen_socket(portstr, &listen_fd) != 0) goto fail;

  if (connect_right_nonblocking(pg->hosts[right], portstr, &conn_fd) != 0)
    goto fail;

  if (poll_until_ready(listen_fd, conn_fd, &left_fd, &right_fd) != 0) goto fail;

  close(listen_fd);
  listen_fd = -1;

  if (exchange_bootstrap(left_fd, right_fd, pg) != 0) goto fail;

  close(left_fd);
  close(right_fd);
  left_fd = right_fd = -1;
  return 0;

fail:
  if (listen_fd >= 0) close(listen_fd);
  if (conn_fd >= 0 && conn_fd != right_fd) close(conn_fd);
  if (left_fd >= 0) close(left_fd);
  if (right_fd >= 0) close(right_fd);
  return -1;
}
