#ifndef CHUNK_PLANNER_H
#define CHUNK_PLANNER_H

#include <stddef.h>
#include <stdint.h>
#include "pg.h"  /* for DATATYPE */

/* Convert chunk_bytes to element count for the given datatype. */
size_t chunk_elems_from_bytes(size_t chunk_bytes, DATATYPE dt);

/* Number of chunks needed to cover count elements with chunk_elems per chunk. */
size_t chunk_count(size_t count, size_t chunk_elems);

/* Reduce-scatter send chunk index for round r and rank p. Returns -1 if world_size <=0. */
int rs_send_chunk_index(int round, int rank, int world_size);

/* Reduce-scatter recv chunk index for round r and rank p. Returns -1 if world_size <=0. */
int rs_recv_chunk_index(int round, int rank, int world_size);

/* Compute byte offset and length for chunk index in reduce-scatter. */
void rs_chunk_offsets(size_t count, size_t chunk_elems, DATATYPE dt,
                      size_t chunk_index, size_t *offset_bytes,
                      size_t *len_bytes);

/* Compute byte offset and length for chunk index in all-gather. */
void ag_chunk_offsets(size_t count, size_t chunk_elems, DATATYPE dt,
                      size_t chunk_index, size_t *offset_bytes,
                      size_t *len_bytes);

#endif /* CHUNK_PLANNER_H */
