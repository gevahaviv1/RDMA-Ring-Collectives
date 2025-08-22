#include "chunk_planner.h"

static size_t elem_size(DATATYPE dt) {
    switch (dt) {
    case DT_INT32:
        return sizeof(int32_t);
    case DT_DOUBLE:
        return sizeof(double);
    default:
        return 0;
    }
}

size_t chunk_elems_from_bytes(size_t chunk_bytes, DATATYPE dt) {
    size_t es = elem_size(dt);
    if (es == 0) return 0;
    size_t elems = chunk_bytes / es;
    if (elems == 0) elems = 1;
    return elems;
}

size_t chunk_count(size_t count, size_t chunk_elems) {
    if (chunk_elems == 0) return 0;
    return (count + chunk_elems - 1) / chunk_elems;
}

int rs_send_chunk_index(int round, int rank, int world_size) {
    if (world_size <= 0) return -1;
    int r = round % world_size;
    int p = rank % world_size;
    if (p < 0) p += world_size;
    return (p - r + world_size) % world_size;
}

int rs_recv_chunk_index(int round, int rank, int world_size) {
    if (world_size <= 0) return -1;
    int r = round % world_size;
    int p = rank % world_size;
    if (p < 0) p += world_size;
    return (p - r - 1 + world_size) % world_size;
}

static void chunk_offsets(size_t count, size_t chunk_elems, DATATYPE dt,
                          size_t chunk_index, size_t *offset_bytes,
                          size_t *len_bytes) {
    size_t es = elem_size(dt);
    size_t start_elem = chunk_index * chunk_elems;
    if (start_elem >= count) {
        *offset_bytes = 0;
        *len_bytes = 0;
        return;
    }
    size_t end_elem = start_elem + chunk_elems;
    if (end_elem > count) end_elem = count;
    *offset_bytes = start_elem * es;
    *len_bytes = (end_elem - start_elem) * es;
}

void rs_chunk_offsets(size_t count, size_t chunk_elems, DATATYPE dt,
                      size_t chunk_index, size_t *offset_bytes,
                      size_t *len_bytes) {
    chunk_offsets(count, chunk_elems, dt, chunk_index, offset_bytes, len_bytes);
}

void ag_chunk_offsets(size_t count, size_t chunk_elems, DATATYPE dt,
                      size_t chunk_index, size_t *offset_bytes,
                      size_t *len_bytes) {
    chunk_offsets(count, chunk_elems, dt, chunk_index, offset_bytes, len_bytes);
}
