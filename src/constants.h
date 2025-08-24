#ifndef CONSTANTS_H
#define CONSTANTS_H

/*
 * This header defines default tuning parameters and constants for the process
 * group (PG) and RDMA bootstrap process. These values can be overridden at
 * runtime via environment variables where applicable.
 */

//==============================================================================
// Network and Bootstrap Defaults
//==============================================================================

/**
 * @brief Default TCP port for the bootstrap ring connection.
 * Used by all ranks to discover each other and exchange RDMA QP info.
 * Overridden by the PG_PORT environment variable.
 */
#define PG_DEFAULT_PORT 18515

//==============================================================================
// RDMA Resource and QP Defaults
//==============================================================================

/**
 * @brief Default InfiniBand device port to use.
 * Most HCAs use port 1 as the first active port.
 */
#define PG_DEFAULT_IB_PORT 1

/**
 * @brief Default GID index to use for RoCEv2.
 * Index 0 is typically the raw IPv4-mapped GID.
 */
#define PG_DEFAULT_GID_INDEX 0

/**
 * @brief Hint for the maximum size of inline data in a send WR (Work Request).
 * The actual value is determined by the provider's capabilities. This is a
 * requested upper bound.
 */
#define PG_DEFAULT_INLINE_HINT 256

/**
 * @brief Default number of entries in the Completion Queue (CQ).
 * This determines how many Work Completions can be buffered before polling.
 */
#define PG_DEFAULT_CQ_ENTRIES 1024

//==============================================================================
// Collective Algorithm Tuning Defaults
//==============================================================================

/**
 * @brief Default threshold for using eager protocol (bytes).
 * Messages smaller than this may be sent inline or via a more optimized path.
 * Overridden by the PG_EAGER_MAX environment variable.
 */
#define PG_DEFAULT_EAGER_MAX 4096

/**
 * @brief Default chunk size for scatter/gather operations (bytes).
 * Large transfers are broken down into chunks of this size.
 * Overridden by the PG_CHUNK_BYTES environment variable.
 */
#define PG_DEFAULT_CHUNK_BYTES 4096

/**
 * @brief Default number of inflight messages or chunks in pipelined collectives.
 * Controls the depth of the pipeline for algorithms like ring all-reduce.
 * Overridden by the PG_INFLIGHT environment variable.
 */
#define PG_DEFAULT_INFLIGHT 4

#endif // CONSTANTS_H
