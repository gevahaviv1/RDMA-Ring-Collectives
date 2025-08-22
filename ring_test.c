#include "ring.h"

#include <assert.h>
#include <stdio.h>

static void test_neighbors(void) {
    /* basic wrap-around */
    assert(left_of(0, 4) == 3);
    assert(right_of(0, 4) == 1);
    assert(left_of(2, 4) == 1);
    assert(right_of(3, 4) == 0);

    /* rank normalization */
    assert(left_of(4, 4) == 3);
    assert(right_of(-1, 4) == 0);

    /* invalid world size */
    assert(left_of(0, 0) == -1);
    assert(right_of(0, -5) == -1);
}

int main(void) {
    test_neighbors();
    printf("Ring neighbor tests passed\n");
    return 0;
}
