# RDMA Ring Collectives

## Project Overview
Single-process-per-host library for Reduce-Scatter → All-Gather → All-Reduce on an RDMA ring.
Targets cluster setups needing lightweight collectives without MPI.
Uses two RC QPs per peer (left/right) and a TCP bootstrap for QP/MR exchange.
Control messages are SEND inline with pre-posted RECVs and credit-based flow control.
Large payloads are pulled with RDMA READ, enabling zero-copy and natural completion.

## Features & Constraints
- Ring topology over two RC QPs per neighbor.
- Eager vs rendezvous:
  - SEND inline for control and small data; credits prevent RNR.
  - RDMA READ for bulk transfers; WRITE-with-Immediate avoided.
  - READ completion fires on requester, so no extra ack.
  - READ places bytes directly into recvbuf for zero-copy.
- Pipelining via tunable chunk size and inflight depth.
- DATATYPE: `int32`, `double`; OPERATION: `sum`, `prod`.
- Out of scope: MPI compatibility, multi-NIC striping, GPU buffers.

## Prerequisites
- Linux, rdma-core/libibverbs, gcc or clang, make.
- RDMA fabric (IB or RoCE) and SSH access between hosts.
- Consistent MTU and clock sync across nodes.

## Build
```bash
make
```
Resulting tree:
```
include/pg.h
libpg.a
src/
```

## Quick Start
Pass a space-separated host list to `connect_process_group("host1 host2 host3", &pg);`.
Minimal pseudo-code:
```c
void *pg;
connect_process_group(hosts, &pg);
pg_all_reduce(send, recv, cnt, DT_INT32, OP_SUM, pg);
pg_close(pg);
```
Launch on N hosts:
```bash
for h in $HOSTS; do ssh $h ./app "$HOSTS" & done; wait
```

## API Reference
`int connect_process_group(const char *serverlist, void **out_handle);`
: join the ring defined by space-separated hostnames.

`int pg_close(void *pg_handle);`
: tear down the process group.

`int pg_all_reduce(void *sendbuf, void *recvbuf, int count,
                   DATATYPE datatype, OPERATION op, void *pg_handle);`
: in-place if `sendbuf==recvbuf`, else out-of-place.

`int pg_reduce_scatter(void *sendbuf, void *recvbuf, int count,
                        DATATYPE datatype, OPERATION op, void *pg_handle);`
`int pg_all_gather(void *sendbuf, void *recvbuf, int count,
                    DATATYPE datatype, void *pg_handle);`
Return `0` on success, `<0` for verbs/logic errors.

## Algorithm Sketch
Each call performs a ring Reduce-Scatter followed by a logical shift and zero-copy
All-Gather. After RS, chunks are rotated one hop right before AG. RDMA READ pulls
remote segments directly into the destination buffer, so All-Gather needs no extra copy.

## Tuning
- `PG_EAGER_MAX` – bytes sent eagerly via SEND (default 8KB).
- `PG_CHUNK_BYTES` – RDMA READ chunk size.
- `PG_INFLIGHT` – number of concurrent RDMA READs.
Choose chunk `~MTU` multiples; increase inflight for high-latency links.

## Validation
- Compare against CPU reference using `int` sum and double epsilon checks.
- Smoke test with N=2 then N=4 hosts before scaling.

## Performance Measurement Discipline
Warm up once, sweep message sizes logarithmically.
Record `size, rounds, inflight, time_us, GB/s` to a tab/CSV file.

## Troubleshooting
- RNR NAK: ensure RECVs are pre-posted.
- Inline SEND too small: query device `max_inline_data`.
- rkey or address mismatch: verify TCP bootstrap data.
- LID/GID issues: check subnet configuration.
- Firewall blocks TCP bootstrap: open ports.
- MTU mismatch: align `ibv_devinfo` across nodes.
- CQ overrun: increase CQ size or poll faster.
- Memory registration failure: raise `ulimit -l`.
- Credit deadlock: ensure SEND completions return credits.

## Limitations & TODOs
Single CQ and thread per process; no retransmit or failure handling.
No GPU buffer support or multi-NIC striping.

## License / Acknowledgments
MIT-style license. Built for the advanced systems course using rdma-core.
