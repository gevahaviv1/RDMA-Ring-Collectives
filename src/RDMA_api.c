#include "RDMA_api.h"
#include <stdio.h>
#include <string.h>

/*
 * Implementation of the low-level RDMA helper functions declared in RDMA_api.h.
 * These functions provide a simplified interface to the InfiniBand verbs API.
 */

//==============================================================================
// RDMA Resource Management
//==============================================================================

/**
 * @brief Opens the first available RDMA device found by ibv_get_device_list.
 */
struct ibv_context* rdma_open_device() {
    struct ibv_device **dev_list = ibv_get_device_list(NULL);
    if (!dev_list) {
        fprintf(stderr, "Failed to get RDMA device list\n");
        return NULL;
    }

    struct ibv_device *dev = dev_list[0];
    if (!dev) {
        fprintf(stderr, "No RDMA devices found\n");
        ibv_free_device_list(dev_list);
        return NULL;
    }

    struct ibv_context *ctx = ibv_open_device(dev);
    if (!ctx) {
        fprintf(stderr, "Failed to open RDMA device '%s'\n", ibv_get_device_name(dev));
    }

    ibv_free_device_list(dev_list);
    return ctx;
}

/**
 * @brief Allocates a Protection Domain (PD).
 */
struct ibv_pd* rdma_alloc_pd(struct ibv_context *ctx) {
    if (!ctx) return NULL;
    struct ibv_pd *pd = ibv_alloc_pd(ctx);
    if (!pd) {
        fprintf(stderr, "Failed to allocate Protection Domain\n");
    }
    return pd;
}

/**
 * @brief Creates a Completion Queue (CQ).
 */
struct ibv_cq* rdma_create_cq(struct ibv_context *ctx, int entries) {
    if (!ctx || entries <= 0) return NULL;
    struct ibv_cq *cq = ibv_create_cq(ctx, entries, NULL, NULL, 0);
    if (!cq) {
        fprintf(stderr, "Failed to create Completion Queue with %d entries\n", entries);
    }
    return cq;
}

/**
 * @brief Registers a Memory Region (MR).
 */
struct ibv_mr* rdma_reg_mr(struct ibv_pd *pd, void *buf, size_t size) {
    if (!pd || !buf || size == 0) return NULL;
    int access = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;
    struct ibv_mr *mr = ibv_reg_mr(pd, buf, size, access);
    if (!mr) {
        fprintf(stderr, "Failed to register Memory Region of size %zu\n", size);
    }
    return mr;
}

/**
 * @brief Creates a reliable, connected Queue Pair (QP).
 */
struct ibv_qp* rdma_create_qp(struct ibv_pd *pd, struct ibv_cq *cq, uint32_t max_inline_hint) {
    if (!pd || !cq) return NULL;

    struct ibv_qp_init_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.send_cq = cq;
    attr.recv_cq = cq;
    attr.qp_type = IBV_QPT_RC;
    attr.cap.max_send_wr = 1024;      // Sufficient for most cases
    attr.cap.max_recv_wr = 1024;
    attr.cap.max_send_sge = 1;
    attr.cap.max_recv_sge = 1;
    attr.cap.max_inline_data = max_inline_hint;

    struct ibv_qp *qp = ibv_create_qp(pd, &attr);
    if (!qp) {
        fprintf(stderr, "Failed to create Queue Pair\n");
    }
    return qp;
}

//==============================================================================
// QP State Transitions
//==============================================================================

/**
 * @brief Transitions a QP to the INIT state.
 */
int rdma_qp_to_init(struct ibv_qp *qp, uint8_t port, int allow_remote_rw) {
    if (!qp || port == 0) return -1;

    struct ibv_qp_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_INIT;

    // Pick a valid P_Key index. Prefer default partition key 0x7fff if present.
    uint16_t pkey = 0;
    uint16_t pkey_index = 0;
    struct ibv_port_attr port_attr;
    memset(&port_attr, 0, sizeof(port_attr));
    if (ibv_query_port(qp->context, port, &port_attr) == 0) {
        int n = port_attr.pkey_tbl_len;
        for (int i = 0; i < n; ++i) {
            if (ibv_query_pkey(qp->context, port, (int)i, &pkey) == 0) {
                // Default partition has lower 15 bits 0x7fff; prefer full membership (bit 15 set)
                if ((pkey & 0x7fff) == 0x7fff) { pkey_index = (uint16_t)i; break; }
            }
        }
    }
    attr.pkey_index = pkey_index;
    fprintf(stderr, "[rdma] QP%u INIT on port %u with pkey_index=%u\n",
            qp->qp_num, (unsigned)port, (unsigned)pkey_index);
    attr.port_num = port;
    attr.qp_access_flags = IBV_ACCESS_LOCAL_WRITE |
                           (allow_remote_rw ? (IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ) : 0);

    int mask = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
    if (ibv_modify_qp(qp, &attr, mask)) {
        fprintf(stderr, "Failed to transition QP to INIT state\n");
        return -1;
    }
    return 0;
}

/**
 * @brief Transitions a QP to the Ready to Receive (RTR) state.
 */
int rdma_qp_to_rtr(struct ibv_qp *qp, struct qp_boot *remote, uint8_t port, uint8_t sgid_index, enum ibv_mtu mtu) {
    if (!qp || !remote || port == 0) return -1;

    struct ibv_port_attr port_attr;
    memset(&port_attr, 0, sizeof(port_attr));
    if (ibv_query_port(qp->context, port, &port_attr) != 0) {
        fprintf(stderr, "Failed to query port attributes for port %u\n", port);
        return -1;
    }

    struct ibv_qp_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTR;
    // Clamp to active MTU on the link
    attr.path_mtu = (port_attr.active_mtu < mtu) ? port_attr.active_mtu : mtu;
    attr.dest_qp_num = remote->qpn;
    attr.rq_psn = remote->psn;
    attr.max_dest_rd_atomic = 1;
    attr.min_rnr_timer = 12; // 0.64ms, a common default

    // Address vector depends on link layer
    attr.ah_attr.sl = 0;
    attr.ah_attr.src_path_bits = 0;
    attr.ah_attr.port_num = port;

    int mask = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
               IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;

    if (port_attr.link_layer == IBV_LINK_LAYER_ETHERNET) {
        // RoCEv2: use global routing via GIDs
        attr.ah_attr.is_global = 1;
        memcpy(&attr.ah_attr.grh.dgid, remote->gid, 16);
        attr.ah_attr.grh.sgid_index = sgid_index;
        attr.ah_attr.grh.hop_limit = 64;
        attr.ah_attr.dlid = 0; // not used on RoCE
        fprintf(stderr, "[rdma] to RTR: RoCE remote qpn=%u rq_psn=%u mtu=%d gid[0]=%02x\n",
                remote->qpn, remote->psn, (int)mtu, remote->gid[0]);
    } else {
        // InfiniBand: address by LID; no GRH
        attr.ah_attr.is_global = 0;
        attr.ah_attr.dlid = remote->lid;
        fprintf(stderr, "[rdma] to RTR: IB remote qpn=%u rq_psn=%u mtu=%d lid=%u\n",
                remote->qpn, remote->psn, (int)mtu, (unsigned)remote->lid);
    }

    if (ibv_modify_qp(qp, &attr, mask)) {
        fprintf(stderr, "Failed to transition QP to RTR state\n");
        return -1;
    }
    return 0;
}

/**
 * @brief Transitions a QP to the Ready to Send (RTS) state.
 */
int rdma_qp_to_rts(struct ibv_qp *qp, uint32_t local_psn) {
    if (!qp) return -1;

    struct ibv_qp_attr attr;
    memset(&attr, 0, sizeof(attr));
    attr.qp_state = IBV_QPS_RTS;
    attr.timeout = 14;      // A common default
    attr.retry_cnt = 7;     // Max retry count
    attr.rnr_retry = 7;     // Infinite retry for RNR NACKs
    attr.sq_psn = local_psn;
    attr.max_rd_atomic = 1;

    int mask = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY |
               IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;
    if (ibv_modify_qp(qp, &attr, mask)) {
        fprintf(stderr, "Failed to transition QP to RTS state\n");
        return -1;
    }
    return 0;
}

//==============================================================================
// RDMA Query Helpers
//==============================================================================

/**
 * @brief Queries the GID for a specific device port and index.
 */
int rdma_query_gid(struct ibv_context *ctx, uint8_t port, uint8_t gid_index, union ibv_gid *out_gid) {
    if (!ctx || !out_gid || port == 0) return -1;
    if (ibv_query_gid(ctx, port, gid_index, out_gid) != 0) {
        fprintf(stderr, "Failed to query GID for port %d, index %d\n", port, gid_index);
        return -1;
    }
    return 0;
}

/**
 * @brief Queries the maximum inline data size for a QP.
 */
int rdma_qp_query_inline(struct ibv_qp *qp, uint32_t *out_max_inline) {
    if (!qp || !out_max_inline) return -1;

    struct ibv_qp_attr attr;
    struct ibv_qp_init_attr init_attr;
    memset(&attr, 0, sizeof(attr));
    memset(&init_attr, 0, sizeof(init_attr));

    if (ibv_query_qp(qp, &attr, IBV_QP_CAP, &init_attr) != 0) {
        fprintf(stderr, "Failed to query QP capabilities for max_inline_data\n");
        return -1;
    }

    *out_max_inline = init_attr.cap.max_inline_data;
    return 0;
}
