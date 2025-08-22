#ifndef RTSCTS_H
#define RTSCTS_H

#include <stddef.h>

#define CTRL_QUEUE_CAP 16

enum ctrl_type {
    CTRL_RTS,
    CTRL_CTS,
    CTRL_DONE
};

struct ctrl_msg {
    enum ctrl_type type;
    int chunk_id;
    size_t off;
    size_t bytes;
};

struct ctrl_queue {
    struct ctrl_msg msgs[CTRL_QUEUE_CAP];
    int head;
    int tail;
};

void ctrl_queue_init(struct ctrl_queue *q);
void ctrl_send(struct ctrl_queue *q, const struct ctrl_msg *m);
int ctrl_recv(struct ctrl_queue *q, struct ctrl_msg *m);

void send_rts(struct ctrl_queue *q, int chunk_id, size_t bytes);
int recv_rts(struct ctrl_queue *q, int *chunk_id, size_t *bytes);

void send_cts(struct ctrl_queue *q, int chunk_id, size_t dst_off, size_t bytes);
int recv_cts(struct ctrl_queue *q, int *chunk_id, size_t *dst_off, size_t *bytes);

void send_done(struct ctrl_queue *q, int chunk_id);
int recv_done(struct ctrl_queue *q, int *chunk_id);

#endif /* RTSCTS_H */
