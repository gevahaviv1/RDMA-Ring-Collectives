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

#include <infiniband/verbs.h>
#include "constants.h"
#include "pg_internal.h"
#include "RDMA_api.h"

/*
 * This file implements the core logic for the process group, including
 * initialization, resource management, and collective communication algorithms.
 */

//==============================================================================
// Forward Declarations
//==============================================================================

static void pg_free_resources(struct pg *pg);

//==============================================================================
// Environment and Hostname Parsing
//==============================================================================

/**
 * @brief Initializes pg fields from environment variables if they are not already set.
 * This allows runtime tuning of performance-related parameters.
 */
void pg_init_env(struct pg *pg) {
    const char *s;
    if (!pg->eager_max && (s = getenv("PG_EAGER_MAX"))) {
        pg->eager_max = strtoul(s, NULL, 0);
    }
    if (!pg->chunk_bytes && (s = getenv("PG_CHUNK_BYTES"))) {
        pg->chunk_bytes = strtoul(s, NULL, 0);
    }
    if (!pg->inflight && (s = getenv("PG_INFLIGHT"))) {
        pg->inflight = atoi(s);
    }
    if (!pg->port && (s = getenv("PG_PORT"))) {
        pg->port = atoi(s);
    }
}

/**
 * @brief Parses a whitespace-separated list of hostnames.
 * @return 0 on success, -1 on failure (e.g., 'me' not in list, or OOM).
 */
static int parse_server_list(const char *list, const char *me, size_t *n_out,
                             size_t *idx_out, char ***hosts_out) {
    size_t n = 0, capacity = 8;
    char **hosts = malloc(capacity * sizeof(char *));
    if (!hosts) return -1;

    const char *p = list;
    while (*p) {
        while (isspace((unsigned char)*p)) p++;
        if (!*p) break;
        const char *start = p;
        while (*p && !isspace((unsigned char)*p)) p++;

        if (n == capacity) {
            capacity *= 2;
            char **new_hosts = realloc(hosts, capacity * sizeof(char *));
            if (!new_hosts) goto fail_alloc;
            hosts = new_hosts;
        }
        hosts[n] = strndup(start, p - start);
        if (!hosts[n]) goto fail_alloc;
        n++;
    }

    for (size_t i = 0; i < n; ++i) {
        if (strcmp(hosts[i], me) == 0) {
            *idx_out = i;
            *n_out = n;
            *hosts_out = hosts;
            return 0;
        }
    }

    fprintf(stderr, "Hostname '%s' not found in server list\n", me);
fail_alloc:
    for (size_t i = 0; i < n; ++i) free(hosts[i]);
    free(hosts);
    return -1;
}

//==============================================================================
// Collective Algorithm Helpers
//==============================================================================

/** @brief Returns the size in bytes of a single element of a given DATATYPE. */
static size_t elem_size(DATATYPE dt) {
    switch (dt) {
        case DT_INT32: return sizeof(int32_t);
        case DT_DOUBLE: return sizeof(double);
        default: return 0;
    }
}

/** @brief Computes how many whole elements of a datatype fit in a chunk. */
static size_t chunk_elems_from_bytes(size_t chunk_bytes, DATATYPE dt) {
    size_t es = elem_size(dt);
    if (!es) return 0;
    size_t elems = chunk_bytes / es;
    return elems > 0 ? elems : 1;
}

/** @brief Computes the byte offset and length for a chunk index. */
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

/** @brief Computes the send chunk index for a rank in a ring reduce-scatter. */
static int rs_send_chunk_index(int round, int rank, int world) {
    return (rank - round + world) % world;
}

/** @brief Computes the receive chunk index for a rank in a ring reduce-scatter. */
static int rs_recv_chunk_index(int round, int rank, int world) {
    return (rank - round - 1 + world) % world;
}

/** @brief Applies a reduction operation element-wise from 'src' to 'dst'. */
static void reduce_inplace(void *dst, const void *src, size_t n, DATATYPE dt, OPERATION op) {
    if (dt == DT_INT32) {
        int32_t *d = dst; const int32_t *s = src;
        for (size_t i = 0; i < n; ++i) d[i] = (op == OP_SUM) ? (d[i] + s[i]) : (d[i] * s[i]);
    } else if (dt == DT_DOUBLE) {
        double *d = dst; const double *s = src;
        for (size_t i = 0; i < n; ++i) d[i] = (op == OP_SUM) ? (d[i] + s[i]) : (d[i] * s[i]);
    }
}

/**
 * @brief MOCK IMPLEMENTATION of a send-receive operation.
 * In a real implementation, this would post RDMA sends/receives.
 * Here, it just simulates the data transfer and reduction locally.
 */
static int pg_sendrecv_mock(struct pg *pg, void *sendbuf, void *recvbuf,
                              size_t bytes, DATATYPE dt, OPERATION op) {
    (void)pg; // Unused in mock
    size_t es = elem_size(dt);
    size_t n = es ? bytes / es : 0;
    if (n > 0) {
        reduce_inplace(recvbuf, sendbuf, n, dt, op);
    }
    size_t rem = es ? bytes % es : 0;
    if (rem > 0) {
        memcpy((char *)recvbuf + n * es, (char *)sendbuf + n * es, rem);
    }
    return 0;
}

//==============================================================================
// Public API Implementation
//==============================================================================

int connect_process_group(const char *serverlist, void **out_handle) {
    if (!serverlist || !out_handle) return -1;

    char self_host[256];
    if (gethostname(self_host, sizeof(self_host)) != 0) {
        fprintf(stderr, "gethostname failed: %s\n", strerror(errno));
        return -1;
    }

    struct pg *pg = calloc(1, sizeof(*pg));
    if (!pg) return -1;

    if (parse_server_list(serverlist, self_host, (size_t *)&pg->world_size, (size_t *)&pg->rank, &pg->hosts) != 0) {
        free(pg);
        return -1;
    }

    // Set defaults and apply environment overrides
    pg->eager_max = PG_DEFAULT_EAGER_MAX;
    pg->chunk_bytes = PG_DEFAULT_CHUNK_BYTES;
    pg->inflight = PG_DEFAULT_INFLIGHT;
    pg->port = PG_DEFAULT_PORT;
    pg->ib_port = PG_DEFAULT_IB_PORT;
    pg_init_env(pg);

    // Initialize RDMA resources
    if (!(pg->ctx = rdma_open_device())) goto fail;
    if (!(pg->pd = rdma_alloc_pd(pg->ctx))) goto fail;
    if (!(pg->cq = rdma_create_cq(pg->ctx, pg->inflight * 2))) goto fail;

    // Allocate and register a communication buffer
    size_t bufsize = pg->chunk_bytes * (size_t)(pg->inflight * 2);
    if (bufsize < MIN_BUFFER_SIZE) bufsize = MIN_BUFFER_SIZE;
    pg->buf = aligned_alloc(PAGE_SIZE, bufsize);
    if (!pg->buf) goto fail;
    pg->mr = rdma_reg_mr(pg->pd, pg->buf, bufsize);
    if (!pg->mr) goto fail;

    // Create QPs for ring communication
    uint32_t inline_hint = (pg->eager_max < MAX_INLINE_SIZE) ? (uint32_t)pg->eager_max : MAX_INLINE_SIZE;
    pg->qp_left  = rdma_create_qp(pg->pd, pg->cq, inline_hint);
    pg->qp_right = rdma_create_qp(pg->pd, pg->cq, inline_hint);
    if (!pg->qp_left || !pg->qp_right) goto fail;

    // Transition QPs to INIT state
    if (rdma_qp_to_init(pg->qp_left, pg->ib_port, 1) != 0) goto fail;
    if (rdma_qp_to_init(pg->qp_right, pg->ib_port, 1) != 0) goto fail;

    // Establish network connections and exchange bootstrap info
    if (pg->world_size > 1) {
        if (pgnet_ring_connect(pg) != 0) goto fail;
    }

    *out_handle = pg;
    return 0;

fail:
    fprintf(stderr, "Process group connection failed during setup.\n");
    pg_free_resources(pg);
    return -1;
}

int pg_close(void *pg_handle) {
    if (!pg_handle) return 0;
    pg_free_resources((struct pg *)pg_handle);
    return 0;
}

int pg_reduce_scatter(void *sendbuf, void *recvbuf, int count, DATATYPE dt,
                      OPERATION op, void *pg_handle) {
    struct pg *pg = pg_handle;
    if (!pg || !sendbuf || !recvbuf || count < 0 || elem_size(dt) == 0) return -1;

    if (pg->world_size == 1) {
        memcpy(recvbuf, sendbuf, (size_t)count * elem_size(dt));
        return 0;
    }

    size_t chunk_elems = chunk_elems_from_bytes(pg->chunk_bytes, dt);

    // In a real implementation, this loop would be pipelined with RDMA operations.
    for (int r = 0; r < pg->world_size - 1; ++r) {
        int send_idx = rs_send_chunk_index(r, pg->rank, pg->world_size);
        int recv_idx = rs_recv_chunk_index(r, pg->rank, pg->world_size);

        size_t send_off, send_len, recv_off, recv_len;
        chunk_offsets((size_t)count, chunk_elems, dt, (size_t)send_idx, &send_off, &send_len);
        chunk_offsets((size_t)count, chunk_elems, dt, (size_t)recv_idx, &recv_off, &recv_len);

        if (recv_len > 0) {
            // MOCK: This should be an RDMA send to the right neighbor and recv from the left.
            pg_sendrecv_mock(pg, (char *)sendbuf + send_off, (char *)recvbuf + recv_off, recv_len, dt, op);
        }
    }
    return 0;
}

int pg_all_gather(void *sendbuf, void *recvbuf, int count, DATATYPE dt, void *pg_handle) {
    struct pg *pg = pg_handle;
    if (!pg || !recvbuf || count < 0 || elem_size(dt) == 0) return -1;

    size_t es = elem_size(dt);
    size_t total_bytes = (size_t)count * (size_t)pg->world_size * es;
    size_t rank_bytes = (size_t)count * es;

    // Copy local data into the correct slot in the final buffer.
    if (sendbuf) {
        memcpy((char *)recvbuf + (size_t)pg->rank * rank_bytes, sendbuf, rank_bytes);
    }

    if (pg->world_size == 1) return 0;

    // In a real implementation, this would use RDMA to exchange data.
    for (int r = 0; r < pg->world_size - 1; ++r) {
        int send_rank_idx = (pg->rank - r + pg->world_size) % pg->world_size;
        int recv_rank_idx = (pg->rank - r - 1 + pg->world_size) % pg->world_size;

        void *sbuf = (char *)recvbuf + (size_t)send_rank_idx * rank_bytes;
        void *rbuf = (char *)recvbuf + (size_t)recv_rank_idx * rank_bytes;

        // MOCK: This should be an RDMA send/recv pair.
        pg_sendrecv_mock(pg, sbuf, rbuf, rank_bytes, dt, OP_SUM); // op is ignored for memcpy
    }

    return 0;
}

int pg_all_reduce(void *sendbuf, void *recvbuf, int count, DATATYPE dt, OPERATION op, void *pg_handle) {
    struct pg *pg = pg_handle;
    if (!pg || !sendbuf || !recvbuf || count < 0) return -1;

    // For out-of-place, copy send buffer to receive buffer to start.
    if (sendbuf != recvbuf) {
        memcpy(recvbuf, sendbuf, (size_t)count * elem_size(dt));
    }

    // Step 1: Reduce-Scatter. Each rank gets a reduced chunk of the result.
    if (pg_reduce_scatter(recvbuf, recvbuf, count, dt, op, pg) != 0) {
        return -1;
    }

    // Step 2: All-Gather. Exchange the reduced chunks so all ranks have the full result.
    return pg_all_gather(recvbuf, recvbuf, count, dt, pg);
}

//==============================================================================
// Internal Resource Management
//==============================================================================

/**
 * @brief Frees all resources associated with a process group.
 * This is the central cleanup function, ensuring resources are released in the correct order.
 */
static void pg_free_resources(struct pg *pg) {
    if (!pg) return;

    // Destroy QPs first, as they depend on other resources.
    if (pg->qp_left)  ibv_destroy_qp(pg->qp_left);
    if (pg->qp_right) ibv_destroy_qp(pg->qp_right);
    if (pg->cq)       ibv_destroy_cq(pg->cq);
    if (pg->mr)       ibv_dereg_mr(pg->mr);
    if (pg->pd)       ibv_dealloc_pd(pg->pd);
    if (pg->ctx)      ibv_close_device(pg->ctx);

    // Close network sockets
    if (pg->left_fd > 0)  close(pg->left_fd);
    if (pg->right_fd > 0) close(pg->right_fd);

    // Free memory allocations.
    if (pg->buf)      free(pg->buf);
    if (pg->hosts) {
        for (int i = 0; i < pg->world_size; ++i) {
            free(pg->hosts[i]);
        }
        free(pg->hosts);
    }

    // Finally, free the pg struct itself.
    free(pg);
}
