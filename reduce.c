#include "reduce.h"
#include <assert.h>
#include <stdint.h>

void reduce_inplace(void *dst, const void *src, size_t n_elems,
                    DATATYPE dt, OPERATION op) {
    assert(dst);
    assert(src);
    assert(op == OP_SUM || op == OP_PROD);

    switch (dt) {
    case DT_INT32: {
        int32_t *d = (int32_t *)dst;
        const int32_t *s = (const int32_t *)src;
        if (op == OP_SUM) {
            for (size_t i = 0; i < n_elems; ++i)
                d[i] += s[i];
        } else { /* OP_PROD */
            for (size_t i = 0; i < n_elems; ++i)
                d[i] *= s[i];
        }
        break;
    }
    case DT_DOUBLE: {
        double *d = (double *)dst;
        const double *s = (const double *)src;
        if (op == OP_SUM) {
            for (size_t i = 0; i < n_elems; ++i)
                d[i] += s[i];
        } else { /* OP_PROD */
            for (size_t i = 0; i < n_elems; ++i)
                d[i] *= s[i];
        }
        break;
    }
    default:
        assert(!"Unsupported datatype");
    }
}

