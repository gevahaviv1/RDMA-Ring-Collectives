#ifndef PG_H
#define PG_H

#include <stddef.h>

typedef enum { DT_INT32, DT_DOUBLE } DATATYPE;
typedef enum { OP_SUM, OP_PROD } OPERATION;

int connect_process_group(const char *serverlist, void **out_handle);

int pg_close(void *pg_handle);

int pg_all_reduce(void *sendbuf, void *recvbuf, int count, DATATYPE datatype,
                  OPERATION op, void *pg_handle);

int pg_reduce_scatter(void *sendbuf, void *recvbuf, int count,
                      DATATYPE datatype, OPERATION op, void *pg_handle);

int pg_all_gather(void *sendbuf, void *recvbuf, int count, DATATYPE datatype,
                  void *pg_handle);

#endif /* PG_H */
