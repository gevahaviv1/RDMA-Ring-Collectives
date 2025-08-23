#ifndef _CONSTANTS_H_
#define _CONSTANTS_H_


#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/param.h>
#include <sys/time.h>
#include <stdlib.h>
#include <getopt.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <time.h>
#include <infiniband/verbs.h>
#include <math.h>


#define MAXIMUM_HANDLE_REQUESTS_BUFFERS 5
#define WC_BATCH (1)
#define MAXIMUM_SIZE (1024*1024) // 1 MB
#define BITS_4_KB (4096)
#define DATABASE_INITIAL_CAPACITY (4)
#define DATABASE_FACTOR_CAPACITY (2)
#define NUMBER_OF_CLIENTS (1)
#define PORT 4792
#define KEY_NOT_FOUND -1

#define RESET   "\033[0m"
#define RED     "\033[31m"
#define GREEN   "\033[32m"


enum {
  PINGPONG_RECV_WRID = 1,
  PINGPONG_SEND_WRID = 2,
};

typedef struct PingpongContext {
  struct ibv_context *context;          // RDMA device context
  struct ibv_comp_channel *channel;     // Event notification channel
  struct ibv_pd *pd;                    // Protection domain for resources
  struct ibv_mr *mr[MAXIMUM_HANDLE_REQUESTS_BUFFERS];  // Memory regions for RDMA operations
  struct ibv_cq *cq;                    // Completion queue for tracking operations
  struct ibv_qp *qp;                    // Queue pair for data transmission
  void *buf[MAXIMUM_HANDLE_REQUESTS_BUFFERS];  // Buffers for send/receive data
  unsigned long size;                   // Size of each buffer
  int rx_depth;                         // Depth of the receive queue
  int routs;                            // Number of posted receive requests
  struct ibv_port_attr portinfo;        // Attributes of the RDMA device port
  size_t currBuffer;                    // Index of the current buffer in use
} PingpongContext;

typedef struct PingpongDest {
  int lid;
  int qpn;
  int psn;
  union ibv_gid gid;
} PingpongDest;


typedef enum RequestType {
  GET,
  SET
} RequestType;

typedef enum ProtocolType {
  EAGER,
  RENDEZVOUS
} ProtocolType;

typedef struct Rendezvous {
  uint32_t rkey;
  void *remote_addr;
  unsigned long size;
} Rendezvous;

typedef struct Packet {
  char value[BITS_4_KB];
  char response_value[BITS_4_KB];
  char key[BITS_4_KB];
  char *dynamic_value;
  RequestType request_type;
  ProtocolType protocol_type;
  Rendezvous rendezvous_get;
  Rendezvous rendezvous_set;
} Packet;

typedef struct DataNode {
  char key[BITS_4_KB];
  char value[BITS_4_KB];
  char *dynamic_value;
  ProtocolType protocol_type;
} DataNode;


typedef struct Database {
  DataNode *data_arr;
  int curr_size;
  int capacity;
} Database;

#endif //_CONSTANTS_H_
