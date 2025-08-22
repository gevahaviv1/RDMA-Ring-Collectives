#ifndef REDUCE_H
#define REDUCE_H

#include <stddef.h>
#include "pg.h"  /* for DATATYPE and OPERATION */

void reduce_inplace(void *dst, const void *src, size_t n_elems,
                    DATATYPE dt, OPERATION op);

#endif /* REDUCE_H */
