#include "chunk_planner.h"
#include <assert.h>
#include <stdio.h>

static void test_chunk_elems(void) {
    assert(chunk_elems_from_bytes(32, DT_INT32) == 8);
    assert(chunk_elems_from_bytes(40, DT_DOUBLE) == 5);
}

static void test_chunk_count(void) {
    size_t elems = chunk_elems_from_bytes(16, DT_INT32); /* 4 */
    assert(chunk_count(10, elems) == 3);
    assert(chunk_count(8, elems) == 2);
}

static void test_rs_indices(void) {
    int N = 4;
    /* rank 0 */
    assert(rs_send_chunk_index(0, 0, N) == 0);
    assert(rs_recv_chunk_index(0, 0, N) == 3);
    assert(rs_send_chunk_index(1, 0, N) == 3);
    assert(rs_recv_chunk_index(1, 0, N) == 2);
    assert(rs_send_chunk_index(2, 0, N) == 2);
    assert(rs_recv_chunk_index(2, 0, N) == 1);
    /* rank 2 */
    assert(rs_send_chunk_index(0, 2, N) == 2);
    assert(rs_recv_chunk_index(0, 2, N) == 1);
    assert(rs_send_chunk_index(1, 2, N) == 1);
    assert(rs_recv_chunk_index(1, 2, N) == 0);
}

static void check_offsets(size_t count, size_t last_bytes) {
    size_t elems = chunk_elems_from_bytes(16, DT_INT32); /* 4 */
    size_t off, len;
    rs_chunk_offsets(count, elems, DT_INT32, 0, &off, &len);
    assert(off == 0 && len == 16);
    rs_chunk_offsets(count, elems, DT_INT32, 1, &off, &len);
    assert(off == 16 && len == 16);
    rs_chunk_offsets(count, elems, DT_INT32, 2, &off, &len);
    assert(off == 32 && len == last_bytes);
    /* ag offsets mirror rs */
    ag_chunk_offsets(count, elems, DT_INT32, 2, &off, &len);
    assert(off == 32 && len == last_bytes);
}

static void test_offsets(void) {
    check_offsets(10, 8); /* last chunk has 2 elements */
    check_offsets(9, 4);  /* last chunk has 1 element */
}

int main(void) {
    test_chunk_elems();
    test_chunk_count();
    test_rs_indices();
    test_offsets();
    printf("Chunk planner tests passed\n");
    return 0;
}
