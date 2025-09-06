#ifndef PG_H
#define PG_H

#include <stddef.h>
#include <stdint.h>

/*
 * This header defines the public API for the RDMA-accelerated Process Group (PG)
 * library. It includes functions for initialization, collective communication,
 * and teardown, as well as the necessary data structures for operation.
 */

// Forward declarations for ibverbs types to avoid including verbs.h in public API.
struct ibv_context;
struct ibv_pd;
struct ibv_cq;
struct ibv_qp;
struct ibv_mr;

/**
 * @brief Bootstrap information exchanged over TCP to establish RDMA connections.
 * Each rank sends this structure to its neighbors to enable them to transition
 * their Queue Pairs (QPs) to a connected state (RTR/RTS).
 */
struct qp_boot {
  uint32_t qpn;       /**< Queue Pair Number of the sender's QP. */
  uint32_t psn;       /**< Packet Sequence Number for the first packet. */
  uint8_t  gid[16];   /**< Global Identifier (GID) for RoCEv2 addressing. */
  uint16_t lid;       /**< Local Identifier (LID) for InfiniBand addressing. */
  uint64_t addr;      /**< Base address of the registered memory region (MR). */
  uint32_t rkey;      /**< Remote key for accessing the MR. */
  uint32_t bytes;     /**< Total size of the MR in bytes. */
};

/**
 * @brief Opaque handle representing a process group instance.
 * This structure holds all state for a participant in the collective group,
 * including rank, world size, network info, and RDMA resources.
 */
struct pg {
  //-- Topology and Tuning Parameters --//
  int      rank;          /**< This process's rank (0 to world_size-1). */
  int      world_size;    /**< Total number of processes in the group. */
  size_t   chunk_bytes;   /**< Size of chunks for pipelined collectives. */
  size_t   eager_max;     /**< Eager protocol threshold in bytes. */
  int      inflight;      /**< Number of inflight chunks for pipelining. */

  //-- Bootstrap and Network Configuration --//
  char   **hosts;         /**< Array of hostnames for all ranks. */
  int      port;          /**< TCP port for bootstrap connections. */
  int      left_fd;       /**< TCP socket for left neighbor. */
  int      right_fd;      /**< TCP socket for right neighbor. */
  uint8_t  ib_port;       /**< InfiniBand device port (1-based). */
  uint8_t  gid_index;     /**< GID index for RoCEv2. */

  //-- RDMA Resources --//
  struct ibv_context *ctx;      /**< Device context. */
  struct ibv_pd      *pd;       /**< Protection Domain. */
  struct ibv_cq      *cq;       /**< Completion Queue. */
  struct ibv_qp      *qp_left;  /**< QP for communication with left neighbor. */
  struct ibv_qp      *qp_right; /**< QP for communication with right neighbor. */
  struct ibv_mr      *mr;       /**< Memory Region for RDMA operations. */
  void               *buf;      /**< Buffer backing the Memory Region. */

  //-- Remote Peer Information --//
  struct qp_boot left_qp;       /**< Bootstrap info from the left neighbor. */
  struct qp_boot right_qp;      /**< Bootstrap info from the right neighbor. */

  //-- Capabilities --//
  uint32_t max_inline_data; /**< Max inline data size supported by the QP. */

  //-- PSNs (local sequence numbers) --//
  uint32_t psn_left;   /**< 24-bit PSN used for qp_left (local). */
  uint32_t psn_right;  /**< 24-bit PSN used for qp_right (local). */
};

/** @brief Supported element datatypes for collective operations. */
typedef enum {
    DT_INT32,  /**< 32-bit signed integer. */
    DT_DOUBLE  /**< 64-bit double-precision floating point. */
} DATATYPE;

/** @brief Supported reduction operations. */
typedef enum {
    OP_SUM,    /**< Summation. */
    OP_PROD    /**< Product. */
} OPERATION;

/**
 * @brief Initializes the process group and connects all ranks.
 *
 * Parses a whitespace-separated server list, determines this host's rank, and
 * establishes a ring-based connection for bootstrap. Initializes RDMA resources.
 *
 * @param serverlist A string containing a whitespace-separated list of hostnames.
 * @param out_handle On success, a pointer to the created process group handle.
 * @return 0 on success, -1 on failure (e.g., invalid arguments, network error).
 */
int connect_process_group(const char *serverlist, void **out_handle);

/**
 * @brief Tears down the process group and releases all resources.
 *
 * @param pg_handle The handle returned by connect_process_group().
 * @return Always returns 0. Safe to call with a NULL handle.
 */
int pg_close(void *pg_handle);

/**
 * @brief Performs an all-reduce operation.
 *
 * Reduces `count` elements from `sendbuf` across all ranks using `op` and
 * places the final result in `recvbuf` on every rank.
 *
 * @param sendbuf   The source buffer.
 * @param recvbuf   The destination buffer.
 * @param count     Number of elements to reduce.
 * @param datatype  The datatype of the elements.
 * @param op        The reduction operation to perform.
 * @param pg_handle The process group handle.
 * @return 0 on success, -1 on failure.
 */
int pg_all_reduce(void *sendbuf, void *recvbuf, int count, DATATYPE datatype,
                  OPERATION op, void *pg_handle);

/**
 * @brief Performs a reduce-scatter operation.
 *
 * Reduces `count` elements from `sendbuf` across all ranks and scatters the
 * result, so each rank receives an equal-sized chunk in its `recvbuf`.
 *
 * @param sendbuf   The source buffer.
 * @param recvbuf   The destination buffer for this rank's chunk.
 * @param count     Total number of elements in the source buffer.
 * @param datatype  The datatype of the elements.
 * @param op        The reduction operation to perform.
 * @param pg_handle The process group handle.
 * @return 0 on success, -1 on failure.
 */
int pg_reduce_scatter(void *sendbuf, void *recvbuf, int count,
                      DATATYPE datatype, OPERATION op, void *pg_handle);

/**
 * @brief Performs an all-gather operation.
 *
 * Gathers `count` elements from each rank and concatenates them into `recvbuf`
 * on all ranks. The final `recvbuf` size must be `count * world` elements.
 *
 * @param sendbuf   The source buffer containing this rank's data.
 * @param recvbuf   The destination buffer for the gathered data.
 * @param count     Number of elements to contribute from each rank.
 * @param datatype  The datatype of the elements.
 * @param pg_handle The process group handle.
 * @return 0 on success, -1 on failure.
 */
int pg_all_gather(void *sendbuf, void *recvbuf, int count, DATATYPE datatype,
                  void *pg_handle);

#endif /* PG_H */
