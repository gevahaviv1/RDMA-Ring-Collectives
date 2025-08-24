#ifndef PG_INTERNAL_H
#define PG_INTERNAL_H

#include "../include/pg.h"
#include "constants.h"

/*
 * This internal header defines functions and structures used exclusively by the
 * process group (PG) implementation. It is not part of the public API and
 * should not be included by applications using the PG library.
 */

/**
 * @brief Initializes tuning parameters from environment variables.
 *
 * If the corresponding fields in the `struct pg` are zero-initialized, this
 * function populates them with values from the following environment variables:
 * - `PG_EAGER_MAX`: Eager protocol threshold.
 * - `PG_CHUNK_BYTES`: Pipelining chunk size.
 * - `PG_INFLIGHT`: Number of concurrent inflight chunks.
 *
 * If the environment variables are not set, it falls back to the default
 * values defined in `constants.h`.
 *
 * @param pg The process group handle to initialize.
 */
void pg_init_env(struct pg *pg);

/**
 * @brief Establishes TCP connections and exchanges bootstrap QP info.
 *
 * This function creates a ring of TCP connections among all ranks. Each rank
 * connects to its right neighbor (`rank + 1`) and accepts a connection from its
 * left neighbor (`rank - 1`). It then exchanges `qp_boot` structures over these
 * connections to enable RDMA QP setup.
 *
 * @param pg The process group handle, which must have hosts, rank, and world
 *           size initialized.
 * @return 0 on success, -1 on failure.
 */
int pgnet_ring_connect(struct pg *pg);

#endif /* PG_INTERNAL_H */
