#ifndef PG_H
#define PG_H

#include <stddef.h>

// Supported element datatypes for collectives.
typedef enum { DT_INT32, DT_DOUBLE } DATATYPE;
// Supported reduction operations.
typedef enum { OP_SUM, OP_PROD } OPERATION;

// Create a process group handle based on a whitespace-separated server list.
// Each entry is a hostname; the entry matching this host determines its rank.
// On success, returns 0 and stores an opaque handle in out_handle.
int connect_process_group(const char *serverlist, void **out_handle);

// Destroy a process group handle created by connect_process_group().
// Always returns 0. Safe to call with NULL.
int pg_close(void *pg_handle);

// All-Reduce: Reduce 'count' elements from sendbuf across all ranks using 'op'
// and write the full reduced result to recvbuf on every rank.
// Returns 0 on success, -1 on invalid arguments.
int pg_all_reduce(void *sendbuf, void *recvbuf, int count, DATATYPE datatype,
                  OPERATION op, void *pg_handle);

// Reduce-Scatter: Reduce 'count' elements across all ranks using 'op' and
// scatter equal chunks so each rank receives its own segment into recvbuf.
// Returns 0 on success, -1 on invalid arguments.
int pg_reduce_scatter(void *sendbuf, void *recvbuf, int count,
                      DATATYPE datatype, OPERATION op, void *pg_handle);

// All-Gather: Gather 'count' elements from each rank so every rank's recvbuf
// contains the concatenation of all ranks' data. Returns 0 on success.
int pg_all_gather(void *sendbuf, void *recvbuf, int count, DATATYPE datatype,
                  void *pg_handle);

#endif /* PG_H */
