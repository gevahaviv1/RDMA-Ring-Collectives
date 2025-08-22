#include "pg.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

struct ibv_cq { int dummy; };

static int poll_called = 0;
int ibv_poll_cq(struct ibv_cq *cq, int num_entries, struct ibv_wc *wc) {
    (void)cq;
    if (poll_called++) {
        return 0;
    }
    if (num_entries >= 2) {
        wc[0].wr_id = 1;
        wc[0].status = IBV_WC_SUCCESS;
        wc[1].wr_id = 2;
        wc[1].status = IBV_WC_LOC_LEN_ERR;
        return 2;
    }
    return 0;
}

int main(void) {
    struct ibv_cq cq;
    struct ibv_wc *wcs = NULL;
    int n = poll_cq_until(&cq, 2, 100, &wcs);
    assert(n == 2);
    assert(wcs);
    assert(wcs[0].wr_id == 1);
    assert(wcs[0].status == IBV_WC_SUCCESS);
    assert(wcs[1].status == IBV_WC_LOC_LEN_ERR);
    free(wcs);
    printf("CQ poll tests passed\n");
    return 0;
}
