#include "RDMA_api.h"

/* Global */
static int page_size;

/* Helpers */

enum ibv_mtu pp_mtu_to_enum(int mtu)
{
    switch (mtu) {
            case 256:  return IBV_MTU_256;
            case 512:  return IBV_MTU_512;
            case 1024: return IBV_MTU_1024;
            case 2048: return IBV_MTU_2048;
            case 4096: return IBV_MTU_4096;
            default:   return -1;
        }
}

uint16_t pp_get_local_lid(struct ibv_context *context, int port)
{
    struct ibv_port_attr attr;

    if (ibv_query_port(context, port, &attr))
        return 0;

    return attr.lid;
}

int pp_get_port_info(struct ibv_context *context, int port,
                     struct ibv_port_attr *attr)
{
    return ibv_query_port(context, port, attr);
}

void wire_gid_to_gid(const char *wgid, union ibv_gid *gid)
{
    char tmp[9];
    uint32_t v32;
    int i;

    for (tmp[8] = 0, i = 0; i < 4; ++i) {
            memcpy(tmp, wgid + i * 8, 8);
            sscanf(tmp, "%x", &v32);
            *(uint32_t *)(&gid->raw[i * 4]) = ntohl(v32);
        }
}

void gid_to_wire_gid(const union ibv_gid *gid, char wgid[])
{
    int i;

    for (i = 0; i < 4; ++i)
        sprintf(&wgid[i * 8], "%08x", htonl(*(uint32_t *)(gid->raw + i * 4)));
}

int pp_connect_ctx(PingpongContext *ctx, int port, int my_psn,
                          enum ibv_mtu mtu, int sl,
                          PingpongDest *dest, int sgid_idx)
{
    struct ibv_qp_attr attr = {
        .qp_state		= IBV_QPS_RTR,
        .path_mtu		= mtu,
        .dest_qp_num		= dest->qpn,
        .rq_psn			= dest->psn,
        .max_dest_rd_atomic	= 1,
        .min_rnr_timer		= 12,
        .ah_attr		= {
            .is_global	= 0,
            .dlid		= dest->lid,
            .sl		= sl,
            .src_path_bits	= 0,
            .port_num	= port
        }
    };

    if (dest->gid.global.interface_id) {
            attr.ah_attr.is_global = 1;
            attr.ah_attr.grh.hop_limit = 1;
            attr.ah_attr.grh.dgid = dest->gid;
            attr.ah_attr.grh.sgid_index = sgid_idx;
        }
    if (ibv_modify_qp(ctx->qp, &attr,
                      IBV_QP_STATE              |
                      IBV_QP_AV                 |
                      IBV_QP_PATH_MTU           |
                      IBV_QP_DEST_QPN           |
                      IBV_QP_RQ_PSN             |
                      IBV_QP_MAX_DEST_RD_ATOMIC |
                      IBV_QP_MIN_RNR_TIMER)) {
            fprintf(stderr, "Failed to modify QP to RTR\n");
            return 1;
        }

    attr.qp_state	    = IBV_QPS_RTS;
    attr.timeout	    = 14;
    attr.retry_cnt	    = 7;
    attr.rnr_retry	    = 7;
    attr.sq_psn	    = my_psn;
    attr.max_rd_atomic  = 1;
    if (ibv_modify_qp(ctx->qp, &attr,
                      IBV_QP_STATE              |
                      IBV_QP_TIMEOUT            |
                      IBV_QP_RETRY_CNT          |
                      IBV_QP_RNR_RETRY          |
                      IBV_QP_SQ_PSN             |
                      IBV_QP_MAX_QP_RD_ATOMIC)) {
            fprintf(stderr, "Failed to modify QP to RTS\n");
            return 1;
        }

    return 0;
}

PingpongDest *pp_client_exch_dest(const char *servername, int port,
                                                 const PingpongDest *my_dest)
{
    struct addrinfo *res, *t;
    struct addrinfo hints = {
        .ai_family   = AF_INET,
        .ai_socktype = SOCK_STREAM
    };
    char *service;
    char msg[sizeof "0000:000000:000000:00000000000000000000000000000000"];
    int n;
    int sockfd = -1;
    PingpongDest *rem_dest = NULL;
    char gid[33];

    if (asprintf(&service, "%d", port) < 0)
        return NULL;

    n = getaddrinfo(servername, service, &hints, &res);

    if (n < 0) {
            fprintf(stderr, "%s for %s:%d\n", gai_strerror(n), servername, port);
            free(service);
            return NULL;
        }
    for (t = res; t; t = t->ai_next) {

            sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
            if (sockfd >= 0) {
                    if (!connect(sockfd, t->ai_addr, t->ai_addrlen))
                        break;
                    close(sockfd);
                    sockfd = -1;
                }
        }

    freeaddrinfo(res);
    free(service);

    if (sockfd < 0) {
            fprintf(stderr, "Couldn't connect to %s:%d\n", servername, port);
            return NULL;
        }

    gid_to_wire_gid(&my_dest->gid, gid);
    sprintf(msg, "%04x:%06x:%06x:%s", my_dest->lid, my_dest->qpn, my_dest->psn, gid);
    if (write(sockfd, msg, sizeof msg) != sizeof msg) {
            fprintf(stderr, "Couldn't send local address\n");
            goto out;
        }

    if (read(sockfd, msg, sizeof msg) != sizeof msg) {
            perror("client read");
            fprintf(stderr, "Couldn't read remote address\n");
            goto out;
        }

    write(sockfd, "done", sizeof "done");

    rem_dest = malloc(sizeof *rem_dest);
    if (!rem_dest)
        goto out;

    sscanf(msg, "%x:%x:%x:%s", &rem_dest->lid, &rem_dest->qpn, &rem_dest->psn, gid);
    wire_gid_to_gid(gid, &rem_dest->gid);

    out:
    close(sockfd);
    return rem_dest;
}

PingpongDest *pp_server_exch_dest(PingpongContext *ctx,
                                                 int ib_port, enum ibv_mtu mtu,
                                                 int port, int sl,
                                                 const PingpongDest *my_dest,
                                                 int sgid_idx)
{
    struct addrinfo *res, *t;
    struct addrinfo hints = {
        .ai_flags    = AI_PASSIVE,
        .ai_family   = AF_INET,
        .ai_socktype = SOCK_STREAM
    };
    char *service;
    char msg[sizeof "0000:000000:000000:00000000000000000000000000000000"];
    int n;
    int sockfd = -1, connfd;
    PingpongDest *rem_dest = NULL;
    char gid[33];

    if (asprintf(&service, "%d", port) < 0)
        return NULL;

    n = getaddrinfo(NULL, service, &hints, &res);

    if (n < 0) {
            fprintf(stderr, "%s for port %d\n", gai_strerror(n), port);
            free(service);
            return NULL;
        }

    for (t = res; t; t = t->ai_next) {

            sockfd = socket(t->ai_family, t->ai_socktype, t->ai_protocol);
            if (sockfd >= 0) {
                    n = 1;

                    setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &n, sizeof n);

                    if (!bind(sockfd, t->ai_addr, t->ai_addrlen))
                        break;
                    close(sockfd);
                    sockfd = -1;
                }
        }

    freeaddrinfo(res);
    free(service);

    if (sockfd < 0) {
            fprintf(stderr, "Couldn't listen to port %d\n", port);
            return NULL;
        }

    listen(sockfd, 1);

    connfd = accept(sockfd, NULL, 0);
    close(sockfd);
    if (connfd < 0) {
            fprintf(stderr, "accept() failed\n");
            return NULL;
        }

    n = read(connfd, msg, sizeof msg);
    if (n != sizeof msg) {
            perror("server read");
            fprintf(stderr, "%d/%d: Couldn't read remote address\n", n, (int) sizeof msg);
            goto out;
        }

    rem_dest = malloc(sizeof *rem_dest);
    if (!rem_dest)
        goto out;

    sscanf(msg, "%x:%x:%x:%s", &rem_dest->lid, &rem_dest->qpn, &rem_dest->psn, gid);
    wire_gid_to_gid(gid, &rem_dest->gid);

    if (pp_connect_ctx(ctx, ib_port, my_dest->psn, mtu, sl, rem_dest, sgid_idx)) {
            fprintf(stderr, "Couldn't connect to remote QP\n");
            free(rem_dest);
            rem_dest = NULL;
            goto out;
        }


    gid_to_wire_gid(&my_dest->gid, gid);
    sprintf(msg, "%04x:%06x:%06x:%s", my_dest->lid, my_dest->qpn, my_dest->psn, gid);
    if (write(connfd, msg, sizeof msg) != sizeof msg) {
            fprintf(stderr, "Couldn't send local address\n");
            free(rem_dest);
            rem_dest = NULL;
            goto out;
        }

    read(connfd, msg, sizeof msg);

    out:
    close(connfd);
    return rem_dest;
}

PingpongContext *pp_init_ctx(struct ibv_device *ib_dev, int size,
                                            int rx_depth, int tx_depth, int port,
                                            int use_event, int is_server)
{
    PingpongContext *ctx;

    ctx = calloc(1, sizeof *ctx);
    if (!ctx)
        return NULL;

    ctx->size     = size;
    ctx->rx_depth = rx_depth;
    ctx->routs    = rx_depth;
    for (int i = 0; i < MAXIMUM_HANDLE_REQUESTS_BUFFERS; i++) {
            ctx->buf[i] = malloc(roundup(size, page_size));
            if (!ctx->buf[i]) {
                    fprintf(stderr, "Couldn't allocate work buf.\n");
                    return NULL;
                }
            memset(ctx->buf[i], 0x7b + is_server, size);
        }

    ctx->context = ibv_open_device(ib_dev);
    if (!ctx->context) {
            fprintf(stderr, "Couldn't get context for %s\n",
                    ibv_get_device_name(ib_dev));
            return NULL;
        }

    if (use_event) {
            ctx->channel = ibv_create_comp_channel(ctx->context);
            if (!ctx->channel) {
                    fprintf(stderr, "Couldn't create completion channel\n");
                    return NULL;
                }
        } else
        ctx->channel = NULL;

    ctx->pd = ibv_alloc_pd(ctx->context);
    if (!ctx->pd) {
            fprintf(stderr, "Couldn't allocate PD\n");
            return NULL;
        }
    for (int i = 0; i < MAXIMUM_HANDLE_REQUESTS_BUFFERS; i++) {
            ctx->mr[i] = ibv_reg_mr(ctx->pd, ctx->buf[i], size, IBV_ACCESS_LOCAL_WRITE);
            if (!ctx->mr[i]) {
                    fprintf(stderr, "Couldn't register MR\n");
                    return NULL;
                }
        }



    ctx->cq = ibv_create_cq(ctx->context, rx_depth + tx_depth, NULL,
                            ctx->channel, 0);
    if (!ctx->cq) {
            fprintf(stderr, "Couldn't create CQ\n");
            return NULL;
        }

    {
        struct ibv_qp_init_attr attr = {
            .send_cq = ctx->cq,
            .recv_cq = ctx->cq,
            .cap     = {
                .max_send_wr  = tx_depth,
                .max_recv_wr  = rx_depth,
                .max_send_sge = 1,
                .max_recv_sge = 1
            },
            .qp_type = IBV_QPT_RC
        };

        ctx->qp = ibv_create_qp(ctx->pd, &attr);
        if (!ctx->qp)  {
                fprintf(stderr, "Couldn't create QP\n");
                return NULL;
            }
    }

    {
        struct ibv_qp_attr attr = {
            .qp_state        = IBV_QPS_INIT,
            .pkey_index      = 0,
            .port_num        = port,
            .qp_access_flags = IBV_ACCESS_REMOTE_READ |
                               IBV_ACCESS_REMOTE_WRITE
        };

        if (ibv_modify_qp(ctx->qp, &attr,
                          IBV_QP_STATE              |
                          IBV_QP_PKEY_INDEX         |
                          IBV_QP_PORT               |
                          IBV_QP_ACCESS_FLAGS)) {
                fprintf(stderr, "Failed to modify QP to INIT\n");
                return NULL;
            }
    }

    return ctx;
}



int pp_post_recv(PingpongContext *ctx, int n)
{
    struct ibv_sge list = {
        .addr	= (uintptr_t) ctx->buf[ctx->currBuffer],
        .length = ctx->size,
        .lkey	= ctx->mr[ctx->currBuffer]->lkey
    };
    struct ibv_recv_wr wr = {
        .wr_id	    = PINGPONG_RECV_WRID,
        .sg_list    = &list,
        .num_sge    = 1,
        .next       = NULL
    };
    struct ibv_recv_wr *bad_wr;
    int i;

    for (i = 0; i < n; ++i)
        if (ibv_post_recv(ctx->qp, &wr, &bad_wr))
            break;

    return i;
}

int pp_post_send(PingpongContext *ctx, const char *local_ptr, void *remote_ptr, uint32_t remote_key,
                        enum ibv_wr_opcode opcode) {
    struct ibv_sge list = {
        .addr	= (uintptr_t) (local_ptr ? local_ptr : ctx->buf[ctx->currBuffer]),
        .length = ctx->size,
        .lkey	= ctx->mr[ctx->currBuffer]->lkey
    };
    struct ibv_send_wr *bad_wr, wr = {
        .wr_id	    = PINGPONG_SEND_WRID,
        .sg_list    = &list,
        .num_sge    = 1,
        .opcode     = opcode,
        .send_flags = IBV_SEND_SIGNALED,
        .next       = NULL
    };
    if (remote_ptr) {
            wr.wr.rdma.remote_addr = (uintptr_t) remote_ptr;
            wr.wr.rdma.rkey = remote_key;
        }
    int a = ibv_post_send(ctx->qp, &wr, &bad_wr);
    return a;
}

int pp_wait_completions(PingpongContext *ctx, int iters) {
    int rcnt = 0, scnt = 0;
    while (rcnt + scnt < iters) {
            struct ibv_wc wc[WC_BATCH];
            int ne, i;

            do {
                    ne = ibv_poll_cq(ctx->cq, WC_BATCH, wc);
                    if (ne < 0) {
                            fprintf(stderr, "poll CQ failed %d\n", ne);
                            return 1;
                        }

                } while (ne < 1);
            for (i = 0; i < ne; ++i) {
                    if (wc[i].status != IBV_WC_SUCCESS) {
                            fprintf(stderr, "Failed status %s (%d) for wr_id %d\n",
                                    ibv_wc_status_str(wc[i].status),
                                    wc[i].status, (int) wc[i].wr_id);
                            return  1;
                        }


                    switch ((int) wc[i].wr_id) {
                            case PINGPONG_SEND_WRID:
                                ++scnt;
                            break;

                            case PINGPONG_RECV_WRID:
                                ++rcnt;
                            break;

                            default:
                                fprintf(stderr, "Completion for unknown wr_id %d\n",
                                        (int) wc[i].wr_id);
                            return 1;
                        }
                }

        }
    return 0;
}


/* API Functions */

int RDMA_disconnect(PingpongContext *ctx)
{
    if (ibv_destroy_qp(ctx->qp)) {
        fprintf(stderr, "Couldn't destroy QP\n");
        return 1;
    }

    if (ibv_destroy_cq(ctx->cq)) {
        fprintf(stderr, "Couldn't destroy CQ\n");
        return 1;
    }
    for (int i = 0; i < MAXIMUM_HANDLE_REQUESTS_BUFFERS; i++) {
        if (ibv_dereg_mr(ctx->mr[i])) {
            fprintf(stderr, "Couldn't deregister MR\n");
            return 1;
        }
    }


    if (ibv_dealloc_pd(ctx->pd)) {
        fprintf(stderr, "Couldn't deallocate PD\n");
        return 1;
    }

    if (ctx->channel) {
        if (ibv_destroy_comp_channel(ctx->channel)) {
            fprintf(stderr, "Couldn't destroy completion channel\n");
            return 1;
        }
    }

    if (ibv_close_device(ctx->context)) {
        fprintf(stderr, "Couldn't release context\n");
        return 1;
    }
    for (int i = 0; i < MAXIMUM_HANDLE_REQUESTS_BUFFERS; i++)
        free(ctx->buf[i]);
    free(ctx);
    return 0;
}

int RDMA_connect(char *servername, PingpongContext **save_ctx){
    struct ibv_device      **dev_list;
    struct ibv_device       *ib_dev;
    PingpongContext *ctx;
    PingpongDest    my_dest;
    PingpongDest    *rem_dest;
    int                      port = PORT;
    int                      ib_port = 1;
    enum ibv_mtu             mtu = IBV_MTU_2048;
    int                      rx_depth = 5000;
    int                      tx_depth = 5000;
    int                      use_event = 0;
    int                      size = MAXIMUM_SIZE;
    int                      sl = 0;
    int                      gidx = -1;
    char                     gid[33];

    srand48(getpid() * time(NULL));

    page_size = sysconf(_SC_PAGESIZE);

    dev_list = ibv_get_device_list(NULL);
    if (!dev_list) {
        perror("Failed to get IB devices list");
        return 1;
    }

    ib_dev = *dev_list;
    if (!ib_dev) {
        fprintf(stderr, "No IB devices found\n");
        return 1;
    }

    ctx = pp_init_ctx(ib_dev, size, rx_depth, tx_depth, ib_port, use_event, !servername);

    if (!ctx) return 1;

    if (pp_get_port_info(ctx->context, ib_port, &ctx->portinfo)) {
        fprintf(stderr, "Couldn't get port info\n");
        return 1;
    }

    my_dest.lid = ctx->portinfo.lid;
    if (ctx->portinfo.link_layer == IBV_LINK_LAYER_INFINIBAND && !my_dest.lid) {
        fprintf(stderr, "Couldn't get local LID\n");
        return 1;
    }

    memset(&my_dest.gid, 0, sizeof my_dest.gid);

    my_dest.qpn = ctx->qp->qp_num;
    my_dest.psn = lrand48() & 0xffffff;
    inet_ntop(AF_INET6, &my_dest.gid, gid, sizeof gid);

    if (servername)
        rem_dest = pp_client_exch_dest(servername, port, &my_dest);
    else
        rem_dest = pp_server_exch_dest(ctx, ib_port, mtu, port, sl, &my_dest, gidx);

    if (!rem_dest) return 1;

    inet_ntop(AF_INET6, &rem_dest->gid, gid, sizeof gid);

    if (servername)
        if (pp_connect_ctx(ctx, ib_port, my_dest.psn, mtu, sl, rem_dest, gidx))
            return 1;

    *save_ctx = ctx;
    ibv_free_device_list(dev_list);
    free(rem_dest);
    return 0;
}

int send_RDMA_read(PingpongContext *ctx, char* target,
                   unsigned long size,
                   void* remote_addr, uint32_t remote_key){
    struct ibv_mr *temp_mr = ctx->mr[ctx->currBuffer];
    struct ibv_mr *curr_client_mr = ibv_reg_mr(ctx->pd,
                                               target,
                                               size,
                                               IBV_ACCESS_REMOTE_WRITE |
                                               IBV_ACCESS_LOCAL_WRITE);
    ctx->mr[ctx->currBuffer] = curr_client_mr;
    ctx->size = size;
    if (pp_post_send(ctx,
                     target,
                     remote_addr,
                     remote_key,
                     IBV_WR_RDMA_READ)) {
            printf("%d%s", 1, "Error server send");
            return 1;
        }
    if(pp_wait_completions(ctx, 1)) {
            printf("%s", "Error completions");
            return 1;
        }
    ctx->mr[ctx->currBuffer] = temp_mr;
    ibv_dereg_mr(curr_client_mr);

    return 0;
}


int send_RDMA_write(PingpongContext *ctx, const char* value,
                    void* remote_addr, uint32_t remote_key){
    size_t size_value = strlen(value) + 1;
    struct ibv_mr* ctxMR = (struct ibv_mr*)ctx->mr[ctx->currBuffer];
    struct ibv_mr* clientMR = ibv_reg_mr(ctx->pd, (char *) value,
                                         size_value, IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_LOCAL_WRITE);

    ctx->mr[ctx->currBuffer] = clientMR;
    ctx->size = size_value;


    if (pp_post_send(ctx,
                     value,
                     remote_addr,
                     remote_key,
                     IBV_WR_RDMA_WRITE)){
            printf("%d%s", 1, "Error server send");
            return 1;
        }

    if(pp_wait_completions(ctx, 1)) {
            printf("%s", "Error completions");
            return 1;
        }

    ctx->mr[ctx->currBuffer] = (struct ibv_mr*) ctxMR;
    ibv_dereg_mr(clientMR);
    return 0;
}

int send_fin(PingpongContext * ctx) {
    ctx->size = 1;
    if (pp_post_send(ctx, NULL, NULL, 0, IBV_WR_SEND)) {
            printf("%d%s", 1, "Error server send");
            return 1;
        }
    if(pp_wait_completions(ctx, 1)) {
            printf("%s", "Error completions");
            return 1;
        }
    return 0;
}
int receive_fin(PingpongContext * ctx){
    ctx->size = 1;
    if (pp_post_recv(ctx, 1) != 1) {
            printf("%d%s", 1, "Error server send");
            return 1;
        }
    if(pp_wait_completions(ctx, 1)) {
            printf("%s", "Error completions");
            return 1;
        }
    return 0;
}



int send_packet(PingpongContext *ctx) {
    ctx->size = sizeof (Packet);
    if (pp_post_send(ctx, NULL, NULL, 0, IBV_WR_SEND)) {
            printf("%d%s", 1, "Error server send");
            return 1;
        }
    if(pp_wait_completions(ctx, 1)) {
            printf("%s", "Error completions");
            return 1;
        }
    return 0;
}

int receive_packet(PingpongContext *ctx) {
    ctx->size = sizeof (Packet);
    if (pp_post_recv(ctx, 1) != 1) {
            printf("%d%s", 1, "Error server send");
            return 1;
        }
    if(pp_wait_completions(ctx, 1)) {
            printf("%s", "Error completions");
            return 1;
        }
    return 0;
}

int receive_packet_async(PingpongContext *ctx) {
    ctx->size = sizeof (Packet);
    if (pp_post_recv(ctx, 1) != 1) {
            printf("%d%s", 1, "Error server receive");
            return 1;
        }
    return 0;
}
