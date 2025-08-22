#ifndef PG_H
#define PG_H

#include <stddef.h>
#include <stdint.h>

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

/* Forward declaration of the internal handle */
typedef struct pg_handle pg_handle;

/* Default pipeline thresholds */
#define PG_DEFAULT_CHUNK_BYTES 4096
#define PG_DEFAULT_INFLIGHT_LIMIT 4

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
