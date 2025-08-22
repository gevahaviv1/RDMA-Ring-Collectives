#ifndef PG_H
#define PG_H

#include <stddef.h>
#include <stdint.h>

/* Forward declarations for RDMA structures to avoid heavy dependencies */
struct ibv_context;
struct ibv_pd;
struct ibv_cq;
struct ibv_qp;
struct ibv_mr;

/* Minimal stand-ins for ibverbs completion types when the real header is absent */
#ifndef INFINIBAND_VERBS_H
enum ibv_wc_status {
    IBV_WC_SUCCESS = 0,
    IBV_WC_LOC_LEN_ERR,
    IBV_WC_LOC_QP_OP_ERR,
    IBV_WC_LOC_EEC_OP_ERR,
    IBV_WC_LOC_PROT_ERR,
    IBV_WC_WR_FLUSH_ERR,
    IBV_WC_MW_BIND_ERR,
    IBV_WC_BAD_RESP_ERR,
    IBV_WC_LOC_ACCESS_ERR,
    IBV_WC_REM_INV_REQ_ERR,
    IBV_WC_REM_ACCESS_ERR,
    IBV_WC_REM_OP_ERR,
    IBV_WC_RETRY_EXC_ERR,
    IBV_WC_RNR_RETRY_EXC_ERR,
    IBV_WC_LOC_RDD_VIOL_ERR,
    IBV_WC_REM_INV_RD_REQ_ERR,
    IBV_WC_REM_ABORT_ERR,
    IBV_WC_INV_EECN_ERR,
    IBV_WC_INV_EEC_STATE_ERR,
    IBV_WC_FATAL_ERR,
    IBV_WC_RESP_TIMEOUT_ERR,
    IBV_WC_GENERAL_ERR,
};

struct ibv_wc {
    uint64_t wr_id;
    enum ibv_wc_status status;
};
#endif /* INFINIBAND_VERBS_H */

/* Datatype identifiers */
typedef enum {
    DT_INT32,
    DT_DOUBLE
} DATATYPE;

/* Supported reduction operations */
typedef enum {
    OP_SUM,
    OP_PROD
} OPERATION;

/* Default pipeline thresholds */
#define PG_DEFAULT_CHUNK_BYTES 4096
#define PG_DEFAULT_INFLIGHT_LIMIT 4

/* Control message receive ring depth per neighbor */
#define PG_CTRL_RECV_SLOTS 8

/* Tiny packed control messages */
struct __attribute__((packed)) pg_msg_rts {
    uint32_t chunk_id;
    uint32_t bytes;
};

struct __attribute__((packed)) pg_msg_cts {
    uint32_t chunk_id;
    uint64_t dst_off;
    uint32_t bytes;
};

struct __attribute__((packed)) pg_msg_done {
    uint32_t chunk_id;
};

union pg_ctrl_msg {
    struct pg_msg_rts rts;
    struct pg_msg_cts cts;
    struct pg_msg_done done;
};

/* Internal handle structure */
typedef struct pg_handle {
    struct ibv_context *ctx;
    struct ibv_pd *pd;
    struct ibv_cq *cq;
    struct ibv_qp *qps[2];
    uint32_t max_inline_data;

    int rank;
    int world_size;
    int left_index;
    int right_index;

    int bootstrap_socks[2];

    struct ibv_mr *data_mrs[2];
    struct ibv_mr *ctrl_mr;

    uint32_t neighbor_rkeys[2];
    uintptr_t neighbor_base_addrs[2];

    size_t chunk_bytes;
    int inflight_limit;

    union pg_ctrl_msg ctrl_recv_bufs[2][PG_CTRL_RECV_SLOTS];
    int ctrl_head[2];
    int rx_credits[2];
} pg_handle;

/* Internal helpers */
void pg_ctrl_init(pg_handle *handle);
union pg_ctrl_msg *pg_ctrl_next_recv(pg_handle *handle, int peer);
int pg_ctrl_send(pg_handle *handle, int peer, struct ibv_qp *qp,
                 void *msg, size_t len);
void pg_ctrl_return_credit(pg_handle *handle, int peer);
int post_send_inline(pg_handle *handle, struct ibv_qp *qp,
                     void *msg, size_t len);
int poll_cq_until(struct ibv_cq *cq, int min_n, int timeout_ms,
                  struct ibv_wc **wcs_out);

int pg_sendrecv_inline(pg_handle *handle, void *sendbuf, void *recvbuf,
                       size_t bytes, size_t eager_bytes, DATATYPE dtype,
                       OPERATION op);

int pg_reduce_scatter(pg_handle *handle, void *sendbuf, void *recvbuf,
                      size_t count, DATATYPE dtype, OPERATION op);

int pg_all_gather(pg_handle *handle, void *recvbuf,
                  size_t count, DATATYPE dtype);

int pg_all_reduce(pg_handle *handle, void *sendbuf, void *recvbuf,
                  size_t count, DATATYPE dtype, OPERATION op);

/* Accessors */
int pg_rank(const pg_handle *handle);
int pg_world_size(const pg_handle *handle);

/* Create and destroy a process group handle */
pg_handle *pg_create(int rank, int world_size, size_t chunk_bytes,
                     int inflight_limit);
void pg_destroy(pg_handle *handle);

/* QP bootstrap information exchanged with neighbors */
typedef struct {
    uint32_t qpn;
    uint16_t lid;      /* 0 when using gid */
    uint8_t gid[16];   /* all zeros when using lid */
} qp_boot;

/* Transition both QPs to RTS using neighbor bootstrap info */
int pg_qps_to_rts(pg_handle *handle, const qp_boot boots[2]);

/* Update data window with application-provided buffers */
int pg_set_window(pg_handle *handle, void *sendbuf, void *recvbuf,
                  size_t bytes);

#endif /* PG_H */
