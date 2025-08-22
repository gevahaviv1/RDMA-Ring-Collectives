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

#endif /* PG_H */
