#include "rtscts.h"

void ctrl_queue_init(struct ctrl_queue *q) {
    if (!q)
        return;
    q->head = 0;
    q->tail = 0;
}

void ctrl_send(struct ctrl_queue *q, const struct ctrl_msg *m) {
    if (!q || !m)
        return;
    q->msgs[q->tail % CTRL_QUEUE_CAP] = *m;
    q->tail++;
}

int ctrl_recv(struct ctrl_queue *q, struct ctrl_msg *m) {
    if (!q || !m)
        return 0;
    if (q->head == q->tail)
        return 0;
    *m = q->msgs[q->head % CTRL_QUEUE_CAP];
    q->head++;
    return 1;
}

void send_rts(struct ctrl_queue *q, int chunk_id, size_t bytes) {
    struct ctrl_msg m = {CTRL_RTS, chunk_id, 0, bytes};
    ctrl_send(q, &m);
}

int recv_rts(struct ctrl_queue *q, int *chunk_id, size_t *bytes) {
    struct ctrl_msg m;
    if (!ctrl_recv(q, &m) || m.type != CTRL_RTS)
        return 0;
    if (chunk_id)
        *chunk_id = m.chunk_id;
    if (bytes)
        *bytes = m.bytes;
    return 1;
}

void send_cts(struct ctrl_queue *q, int chunk_id, size_t dst_off, size_t bytes) {
    struct ctrl_msg m = {CTRL_CTS, chunk_id, dst_off, bytes};
    ctrl_send(q, &m);
}

int recv_cts(struct ctrl_queue *q, int *chunk_id, size_t *dst_off, size_t *bytes) {
    struct ctrl_msg m;
    if (!ctrl_recv(q, &m) || m.type != CTRL_CTS)
        return 0;
    if (chunk_id)
        *chunk_id = m.chunk_id;
    if (dst_off)
        *dst_off = m.off;
    if (bytes)
        *bytes = m.bytes;
    return 1;
}

void send_done(struct ctrl_queue *q, int chunk_id) {
    struct ctrl_msg m = {CTRL_DONE, chunk_id, 0, 0};
    ctrl_send(q, &m);
}

int recv_done(struct ctrl_queue *q, int *chunk_id) {
    struct ctrl_msg m;
    if (!ctrl_recv(q, &m) || m.type != CTRL_DONE)
        return 0;
    if (chunk_id)
        *chunk_id = m.chunk_id;
    return 1;
}
