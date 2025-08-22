#include "pg.h"

/* Forward declarations for RDMA structures to avoid external dependencies */
struct ibv_context;
struct ibv_pd;
struct ibv_cq;
struct ibv_qp;
struct ibv_mr;

struct pg_handle {
    struct ibv_context *ctx;          /* device context */
    struct ibv_pd *pd;                /* protection domain */
    struct ibv_cq *cq;                /* completion queue */
    struct ibv_qp *qps[2];            /* two queue pairs */
    uint32_t max_inline_data;         /* inline threshold */

    int rank;                         /* rank in communicator */
    int world_size;                   /* total participants */
    int left_index;                   /* neighbor to the left */
    int right_index;                  /* neighbor to the right */

    int bootstrap_socks[2];           /* TCP bootstrap sockets */

    struct ibv_mr *data_mrs[2];       /* exposed data windows */
    struct ibv_mr *ctrl_mr;           /* small control window */

    uint32_t neighbor_rkeys[2];       /* neighbors' rkeys */
    uintptr_t neighbor_base_addrs[2]; /* neighbors' base addresses */

    size_t chunk_bytes;               /* pipeline chunk size */
    int inflight_limit;               /* maximum inflight operations */

    int rx_credits[2];                /* receive credit counters */
};

int pg_rank(const pg_handle *handle) {
    return handle ? handle->rank : -1;
}

int pg_world_size(const pg_handle *handle) {
    return handle ? handle->world_size : -1;
}
