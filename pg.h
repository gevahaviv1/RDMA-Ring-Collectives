#ifndef PG_H
#define PG_H

#include <stddef.h>

/* Public datatypes */
typedef enum { DT_INT32, DT_DOUBLE } DATATYPE;

typedef enum { OP_SUM, OP_PROD } OPERATION;

/* Opaque handle */
typedef struct pg_handle pg_handle;

/* Public API */
pg_handle *pg_create(int rank, int world_size, size_t chunk_bytes,
                     int inflight_limit);
void pg_close(pg_handle *handle);

int pg_reduce_scatter(pg_handle *handle, void *sendbuf, void *recvbuf,
                      size_t count, DATATYPE dtype, OPERATION op);

int pg_all_gather(pg_handle *handle, void *recvbuf, size_t count,
                  DATATYPE dtype);

int pg_all_reduce(pg_handle *handle, void *sendbuf, void *recvbuf, size_t count,
                  DATATYPE dtype, OPERATION op);

#endif /* PG_H */
