#include "pg.h"

#include <ctype.h>
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

#define PG_DEFAULT_EAGER_MAX 4096
#define PG_DEFAULT_CHUNK_BYTES 4096
#define PG_DEFAULT_INFLIGHT 4

struct qp_boot {
  uint32_t qpn;
  uint32_t psn;
  uint8_t gid[16];
  uint64_t addr;
  uint32_t rkey;
  uint32_t bytes;
};

struct pg {
  int rank;
  int world;
  size_t chunk_bytes;
  size_t eager_max;
  int inflight;

  char **hosts;
  int port;
  struct qp_boot left_qp;
  struct qp_boot right_qp;
  uint32_t max_inline_data;
};

// Initialize pg fields from environment variables (PG_EAGER_MAX,
// PG_CHUNK_BYTES, PG_INFLIGHT) if they are zero-initialized; otherwise keep
// existing values.
static void pg_init_env(struct pg *pg) {
  const char *s;

  if (!pg->eager_max) {
    s = getenv("PG_EAGER_MAX");
    pg->eager_max = s ? strtoul(s, NULL, 0) : PG_DEFAULT_EAGER_MAX;
  }

  if (!pg->chunk_bytes) {
    s = getenv("PG_CHUNK_BYTES");
    pg->chunk_bytes = s ? strtoul(s, NULL, 0) : PG_DEFAULT_CHUNK_BYTES;
  }

  if (!pg->inflight) {
    s = getenv("PG_INFLIGHT");
    pg->inflight = s ? atoi(s) : PG_DEFAULT_INFLIGHT;
  }
}

// Return the size in bytes of one element of the given DATATYPE; 0 if
// unsupported.
static size_t elem_size(DATATYPE dt) {
  switch (dt) {
    case DT_INT32:
      return sizeof(int32_t);
    case DT_DOUBLE:
      return sizeof(double);
    default:
      return 0;
  }
}

// Given a chunk size in bytes and datatype, compute how many whole elements
// fit. Returns at least 1 when dt is supported; returns 0 if dt is invalid.
static size_t chunk_elems_from_bytes(size_t chunk_bytes, DATATYPE dt) {
  size_t es = elem_size(dt);

  if (!es) return 0;
  size_t elems = chunk_bytes / es;

  return elems ? elems : 1;
}

// For a total of 'count' elements split into chunks of 'chunk_elems', compute
// the byte offset (*off) and byte length (*len) for the chunk with index 'idx'.
static void chunk_offsets(size_t count, size_t chunk_elems, DATATYPE dt,
                          size_t idx, size_t *off, size_t *len) {
  size_t es = elem_size(dt);
  size_t start = idx * chunk_elems;

  if (start >= count) {
    *off = *len = 0;
    return;
  }

  size_t end = start + chunk_elems;
  if (end > count) end = count;
  *off = start * es;
  *len = (end - start) * es;
}

// Compute which chunk index this rank should SEND in round 'round' (0..world-2)
// in a ring-style reduce-scatter schedule.
static int rs_send_chunk_index(int round, int rank, int world) {
  int r = round % world;
  int p = rank % world;
  if (p < 0) p += world;
  return (p - r + world) % world;
}

// Compute which chunk index this rank should RECEIVE in round 'round'
// in a ring-style reduce-scatter schedule.
static int rs_recv_chunk_index(int round, int rank, int world) {
  int r = round % world;
  int p = rank % world;
  if (p < 0) p += world;
  return (p - r - 1 + world) % world;
}

// Apply the reduction operation 'op' elementwise from 'src' into 'dst' for 'n'
// elements of type 'dt'. The result is stored in-place in 'dst'.
static void reduce_inplace(void *dst, const void *src, size_t n, DATATYPE dt,
                           OPERATION op) {
  if (dt == DT_INT32) {
    int32_t *d = dst;
    const int32_t *s = src;

    if (op == OP_SUM) {
      for (size_t i = 0; i < n; ++i) d[i] += s[i];
    } else {
      for (size_t i = 0; i < n; ++i) d[i] *= s[i];
    }

  } else if (dt == DT_DOUBLE) {
    double *d = dst;
    const double *s = src;

    if (op == OP_SUM) {
      for (size_t i = 0; i < n; ++i) d[i] += s[i];
    } else {
      for (size_t i = 0; i < n; ++i) d[i] *= s[i];
    }
  }
}

// Simulate a send/recv+reduce: reduce as many whole elements as fit in 'bytes'
// from sendbuf into recvbuf using 'op' and copy any remaining tail bytes.
static int pg_sendrecv_inline(struct pg *pg, void *sendbuf, void *recvbuf,
                              size_t bytes, DATATYPE dt, OPERATION op) {
  (void)pg;
  size_t es = elem_size(dt);
  size_t n = es ? bytes / es : 0;

  if (n) reduce_inplace(recvbuf, sendbuf, n, dt, op);
  size_t rem = es ? bytes % es : bytes;

  if (rem) memcpy((char *)recvbuf + n * es, (char *)sendbuf + n * es, rem);
  return 0;
}

// Parse a whitespace-separated list of hostnames in 'list'. Determine total
// number of entries (*n_out) and the index (*idx_out) matching 'me'. Returns 0
// on success, -1 if 'me' is not found.
static int parse_server_list(const char *list, const char *me, size_t *n_out,
                             size_t *idx_out, char ***hosts_out) {
  size_t n = 0, idx = (size_t)-1;
  const char *p = list;

  // first pass: count hosts and find our index
  while (*p) {
    while (isspace((unsigned char)*p)) p++;
    if (!*p) break;

    const char *start = p;
    while (*p && !isspace((unsigned char)*p)) p++;

    size_t len = (size_t)(p - start);
    size_t me_len = strlen(me);
    if (idx == (size_t)-1 &&
        ((me_len >= len && strncmp(me, start, len) == 0) ||
         (len >= me_len && strncmp(start, me, me_len) == 0)))
      idx = n;
    n++;
  }

  if (idx == (size_t)-1) return -1;

  char **hosts = calloc(n, sizeof(char *));
  if (!hosts) return -1;

  // second pass: copy hostnames
  p = list;
  size_t i = 0;
  while (*p && i < n) {
    while (isspace((unsigned char)*p)) p++;
    if (!*p) break;

    const char *start = p;
    while (*p && !isspace((unsigned char)*p)) p++;
    size_t len = (size_t)(p - start);
    hosts[i] = strndup(start, len);
    if (!hosts[i]) {
      for (size_t j = 0; j < i; ++j) free(hosts[j]);
      free(hosts);
      return -1;
    }
    i++;
  }

  *n_out = n;
  *idx_out = idx;
  *hosts_out = hosts;
  return 0;
}

// Write exactly 'len' bytes from 'buf' to socket 'fd', handling short writes
// and EINTR. Returns 0 on success, -1 on error.
static int write_full(int fd, const void *buf, size_t len) {
  const char *p = buf;
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
  char *p = buf;
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

// Establish ring connections to left/right peers and exchange bootstrap info.
// Uses pg->hosts, pg->rank, pg->world, pg->port. Fills pg->left_qp,
// pg->right_qp and pg->max_inline_data. Returns 0 on success, -1 on failure.
static int ring_connect(struct pg *pg) {
  int right = (pg->rank + 1) % pg->world;

  struct addrinfo hints, *ai = NULL;
  char portstr[16];
  snprintf(portstr, sizeof(portstr), "%d", pg->port);

  int listen_fd = -1, conn_fd = -1, left_fd = -1, right_fd = -1;

  // Setup passive listen socket
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = AI_PASSIVE;
  if (getaddrinfo(NULL, portstr, &hints, &ai) != 0) goto fail;

  listen_fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
  if (listen_fd < 0) goto fail;

  fcntl(listen_fd, F_SETFL, O_NONBLOCK);
  if (bind(listen_fd, ai->ai_addr, ai->ai_addrlen) != 0) goto fail;
  if (listen(listen_fd, 1) != 0) goto fail;
  freeaddrinfo(ai);
  ai = NULL;

  // Initiate non-blocking connect to right neighbor
  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  if (getaddrinfo(pg->hosts[right], portstr, &hints, &ai) != 0) goto fail;

  conn_fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
  if (conn_fd < 0) goto fail;

  fcntl(conn_fd, F_SETFL, O_NONBLOCK);
  int rc = connect(conn_fd, ai->ai_addr, ai->ai_addrlen);
  if (rc != 0 && errno != EINPROGRESS) goto fail;
  freeaddrinfo(ai);
  ai = NULL;

  // Poll until both incoming (left) and outgoing (right) are ready
  struct pollfd pfds[2];
  pfds[0].fd = listen_fd;
  pfds[0].events = POLLIN;
  pfds[1].fd = conn_fd;
  pfds[1].events = POLLOUT;
  int timeout_ms = 10000;

  while (left_fd < 0 || right_fd < 0) {
    rc = poll(pfds, 2, timeout_ms);
    if (rc <= 0) goto fail;

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
        goto fail;
      right_fd = conn_fd;
      int fl = fcntl(right_fd, F_GETFL, 0);
      fcntl(right_fd, F_SETFL, fl & ~O_NONBLOCK);
    }
  }

  close(listen_fd);
  listen_fd = -1;

  // Exchange bootstrap info with neighbors
  struct qp_boot myinfo;
  memset(&myinfo, 0, sizeof(myinfo));
  if (exchange_qp(left_fd, &myinfo, &pg->left_qp, 0) != 0) goto fail;
  if (exchange_qp(right_fd, &myinfo, &pg->right_qp, 1) != 0) goto fail;

  close(left_fd);
  close(right_fd);
  left_fd = right_fd = -1;
  pg->max_inline_data = 0;
  return 0;

fail:
  if (listen_fd >= 0) close(listen_fd);
  if (conn_fd >= 0 && conn_fd != right_fd) close(conn_fd);
  if (left_fd >= 0) close(left_fd);
  if (right_fd >= 0) close(right_fd);
  if (ai) freeaddrinfo(ai);
  return -1;
}

// Initialize a process group using a whitespace-separated 'serverlist'.
// Determines this host's rank and world size, applies environment overrides,
// and returns an opaque handle via 'out_handle'. Establishes a TCP ring when
// world > 1.
int connect_process_group(const char *serverlist, void **out_handle) {
  if (!serverlist || !out_handle) return -1;

  char host[256];
  if (gethostname(host, sizeof(host)) != 0) return -1;

  size_t n = 0, idx = 0;
  char **hosts = NULL;
  if (parse_server_list(serverlist, host, &n, &idx, &hosts) != 0) return -1;

  struct pg *pg = calloc(1, sizeof(*pg));
  if (!pg) {
    for (size_t i = 0; i < n; ++i) free(hosts[i]);
    free(hosts);
    return -1;
  }

  pg->rank = (int)idx;
  pg->world = (int)n;
  pg->hosts = hosts;
  pg_init_env(pg);

  const char *ps = getenv("PG_PORT");
  pg->port = ps ? atoi(ps) : 18515;

  if (pg->world > 1) {
    if (ring_connect(pg) != 0) goto fail;
  }

  *out_handle = pg;
  return 0;

fail:
  if (hosts) {
    for (size_t i = 0; i < n; ++i) free(hosts[i]);
    free(hosts);
  }
  free(pg);
  return -1;
}

// Destroy a process group handle returned by connect_process_group().
int pg_close(void *pg_handle) {
  struct pg *pg = pg_handle;
  if (!pg) return 0;

  if (pg->hosts) {
    for (int i = 0; i < pg->world; ++i) free(pg->hosts[i]);
    free(pg->hosts);
  }

  free(pg);
  return 0;
}

// Reduce-Scatter: Reduce data across all ranks using 'op' and scatter the
// reduced chunks so that each rank receives its own segment.
int pg_reduce_scatter(void *sendbuf, void *recvbuf, int count, DATATYPE dt,
                      OPERATION op, void *pg_handle) {
  struct pg *pg = pg_handle;
  if (!pg || !sendbuf || !recvbuf || count < 0) return -1;

  pg_init_env(pg);
  if (pg->world == 1) {
    size_t es = elem_size(dt);
    memcpy(recvbuf, sendbuf, (size_t)count * es);
    return 0;
  }

  size_t chunk_elems = chunk_elems_from_bytes(pg->chunk_bytes, dt);
  if (!chunk_elems) return -1;

  for (int r = 0; r < pg->world - 1; ++r) {
    int send_idx = rs_send_chunk_index(r, pg->rank, pg->world);
    int recv_idx = rs_recv_chunk_index(r, pg->rank, pg->world);

    size_t send_off = 0, send_len = 0;
    size_t recv_off = 0, recv_len = 0;

    chunk_offsets((size_t)count, chunk_elems, dt, (size_t)send_idx, &send_off,
                  &send_len);
    chunk_offsets((size_t)count, chunk_elems, dt, (size_t)recv_idx, &recv_off,
                  &recv_len);

    if (!recv_len) continue;
    pg_sendrecv_inline(pg, (char *)sendbuf + send_off,
                       (char *)recvbuf + recv_off, recv_len, dt, op);
  }

  return 0;
}

// All-Gather: Gather chunks from all ranks so that every rank ends up with the
// concatenation of all ranks' data in 'recvbuf'.
int pg_all_gather(void *sendbuf, void *recvbuf, int count, DATATYPE dt,
                  void *pg_handle) {
  struct pg *pg = pg_handle;
  if (!pg || !recvbuf || count < 0) return -1;

  pg_init_env(pg);
  if (sendbuf && sendbuf != recvbuf) {
    size_t es = elem_size(dt);
    memcpy(recvbuf, sendbuf, (size_t)count * es);
  }

  if (pg->world == 1) return 0;

  size_t chunk_elems = chunk_elems_from_bytes(pg->chunk_bytes, dt);
  if (!chunk_elems) return -1;

  for (int r = 0; r < pg->world - 1; ++r) {
    int send_idx = (pg->rank + 1 - r + pg->world) % pg->world;
    int recv_idx = (pg->rank - r + pg->world) % pg->world;

    size_t send_off = 0, send_len = 0;
    size_t recv_off = 0, recv_len = 0;

    chunk_offsets((size_t)count, chunk_elems, dt, (size_t)send_idx, &send_off,
                  &send_len);
    chunk_offsets((size_t)count, chunk_elems, dt, (size_t)recv_idx, &recv_off,
                  &recv_len);

    if (!recv_len) continue;
    memcpy((char *)recvbuf + recv_off, (char *)recvbuf + send_off, recv_len);
  }

  return 0;
}

// All-Reduce: Compute reduction across all ranks so each rank gets the full
// reduced result. Implemented as Reduce-Scatter followed by All-Gather.
int pg_all_reduce(void *sendbuf, void *recvbuf, int count, DATATYPE dt,
                  OPERATION op, void *pg_handle) {
  struct pg *pg = pg_handle;
  if (!pg || !sendbuf || !recvbuf || count < 0) return -1;

  size_t es = elem_size(dt);
  if (sendbuf != recvbuf) memcpy(recvbuf, sendbuf, (size_t)count * es);
  if (pg_reduce_scatter(recvbuf, recvbuf, count, dt, op, pg) != 0) return -1;

  return pg_all_gather(recvbuf, recvbuf, count, dt, pg);
}
