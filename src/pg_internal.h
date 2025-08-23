#ifndef PG_INTERNAL_H
#define PG_INTERNAL_H

#include <stddef.h>
#include <stdint.h>

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
void pg_init_env(struct pg *pg);

// Establish ring TCP connectivity and exchange bootstrap info for QPs.
int ring_connect(struct pg *pg);

#endif /* PG_INTERNAL_H */
