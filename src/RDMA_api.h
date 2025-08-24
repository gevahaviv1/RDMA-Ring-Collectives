#ifndef RDMA_API_H
#define RDMA_API_H

#include <infiniband/verbs.h>
#include "../include/pg.h"

/*
 * This header defines a lightweight, low-level API for interacting with the
 * InfiniBand verbs framework. It provides helper functions to simplify RDMA
 * resource creation, QP state transitions, and querying device attributes.
 */

//==============================================================================
// RDMA Resource Management
//==============================================================================

/**
 * @brief Opens the first available RDMA device.
 * @return A pointer to the device context on success, or NULL on failure.
 */
struct ibv_context* rdma_open_device();

/**
 * @brief Allocates a Protection Domain (PD) on a given device.
 * @param ctx The device context.
 * @return A pointer to the PD on success, or NULL on failure.
 */
struct ibv_pd* rdma_alloc_pd(struct ibv_context *ctx);

/**
 * @brief Creates a Completion Queue (CQ) on a given device.
 * @param ctx The device context.
 * @param entries The minimum required number of entries in the CQ.
 * @return A pointer to the CQ on success, or NULL on failure.
 */
struct ibv_cq* rdma_create_cq(struct ibv_context *ctx, int entries);

/**
 * @brief Registers a memory buffer (Memory Region - MR) with a PD.
 * @param pd The Protection Domain.
 * @param buf Pointer to the memory buffer to register.
 * @param size The size of the buffer in bytes.
 * @return A pointer to the MR on success, or NULL on failure.
 */
struct ibv_mr* rdma_reg_mr(struct ibv_pd *pd, void *buf, size_t size);

/**
 * @brief Creates a reliable, connected Queue Pair (QP).
 * @param pd The Protection Domain.
 * @param cq The Completion Queue to associate with the QP.
 * @param max_inline_hint A hint for the max inline data size.
 * @return A pointer to the QP on success, or NULL on failure.
 */
struct ibv_qp* rdma_create_qp(struct ibv_pd *pd, struct ibv_cq *cq, uint32_t max_inline_hint);

//==============================================================================
// QP State Transitions
//==============================================================================

/**
 * @brief Transitions a QP to the INIT state.
 * @param qp The QP to modify.
 * @param port The device port number (1-based).
 * @param allow_remote_rw If non-zero, enables remote read/write access.
 * @return 0 on success, -1 on failure.
 */
int rdma_qp_to_init(struct ibv_qp *qp, uint8_t port, int allow_remote_rw);

/**
 * @brief Transitions a QP to the Ready to Receive (RTR) state.
 * @param qp The QP to modify.
 * @param remote Information about the remote peer's QP and memory.
 * @param port The local device port number.
 * @param sgid_index The GID index for the path.
 * @param mtu The path MTU.
 * @return 0 on success, -1 on failure.
 */
int rdma_qp_to_rtr(struct ibv_qp *qp, struct qp_boot *remote, uint8_t port, uint8_t sgid_index, enum ibv_mtu mtu);

/**
 * @brief Transitions a QP to the Ready to Send (RTS) state.
 * @param qp The QP to modify.
 * @param local_psn The initial local Packet Sequence Number.
 * @return 0 on success, -1 on failure.
 */
int rdma_qp_to_rts(struct ibv_qp *qp, uint32_t local_psn);

//==============================================================================
// RDMA Query Helpers
//==============================================================================

/**
 * @brief Queries the GID for a specific device port and index.
 * @param ctx The device context.
 * @param port The device port number.
 * @param gid_index The index of the GID to query.
 * @param out_gid Pointer to store the resulting GID.
 * @return 0 on success, non-zero on failure.
 */
int rdma_query_gid(struct ibv_context *ctx, uint8_t port, uint8_t gid_index, union ibv_gid *out_gid);

/**
 * @brief Queries the maximum inline data size for a QP.
 * @param qp The QP to query.
 * @param out_max_inline Pointer to store the result.
 * @return 0 on success, -1 on failure.
 */
int rdma_qp_query_inline(struct ibv_qp *qp, uint32_t *out_max_inline);

#endif // RDMA_API_H
