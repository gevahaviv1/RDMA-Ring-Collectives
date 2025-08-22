#include "reduce.h"
#include <assert.h>
#include <stdio.h>

static void test_int32_sum(void) {
    int32_t dst[3] = {1, 2, 3};
    int32_t src[3] = {4, 5, 6};
    reduce_inplace(dst, src, 3, DT_INT32, OP_SUM);
    assert(dst[0] == 5 && dst[1] == 7 && dst[2] == 9);
}

static void test_int32_prod(void) {
    int32_t dst[3] = {1, 2, 3};
    int32_t src[3] = {4, 5, 6};
    reduce_inplace(dst, src, 3, DT_INT32, OP_PROD);
    assert(dst[0] == 4 && dst[1] == 10 && dst[2] == 18);
}

static void test_double_sum(void) {
    double dst[3] = {1.0, 2.0, 3.0};
    double src[3] = {4.0, 5.0, 6.0};
    reduce_inplace(dst, src, 3, DT_DOUBLE, OP_SUM);
    assert(dst[0] == 5.0 && dst[1] == 7.0 && dst[2] == 9.0);
}

static void test_double_prod(void) {
    double dst[3] = {1.0, 2.0, 3.0};
    double src[3] = {4.0, 5.0, 6.0};
    reduce_inplace(dst, src, 3, DT_DOUBLE, OP_PROD);
    assert(dst[0] == 4.0 && dst[1] == 10.0 && dst[2] == 18.0);
}

int main(void) {
    test_int32_sum();
    test_int32_prod();
    test_double_sum();
    test_double_prod();
    printf("reduce_inplace tests passed\n");
    return 0;
}

