#!/usr/bin/env python3
"""KV cache traffic simulator for the Mooncake Transfer Engine.

The tool simulates the cross-node KV cache traffic of an LLM serving
deployment: one process (target) exposes a registered KV pool, the other
(initiator) issues batched block transfers that look like prefill writes or
decode reads. It is intended for validating transport changes (for example
RDMA endpoint / QP handling) on CPU-only hosts with SoftRoCE, where real
model execution is neither possible nor useful.

Transfer semantics:
  * A "request" moves --blocks_per_request blocks of --block_size bytes.
  * Requests are placed into a pool of slots, so offsets are reused the same
    way an evicting KV cache reuses cache blocks.
  * --op write  : prefill push (local -> remote)
  * --op read   : decode fetch (remote -> local)
  * --op mix    : both, chosen per request
  * --verify    : write requests are read back and checked (block content and
                  a per-request stamp), so dropped or misplaced writes fail.

Usage:
  # Target (KV pool owner)
  python3 kv_traffic_sim.py --mode target --local_server_name mcb \\
      --protocol rdma --device_name rxe-b --pool_size_gb 1 \\
      --info-file /tmp/kv_target.json

  # Initiator (traffic generator)
  python3 kv_traffic_sim.py --mode initiator --local_server_name mca \\
      --target_server_name mcb --protocol rdma --device_name rxe-a \\
      --target-info-file /tmp/kv_target.json \\
      --block_size 65536 --blocks_per_request 256 \\
      --num_requests 200 --concurrency 8 --op mix --verify

Exit codes:
  0  all transfers succeeded and verification passed
  1  transfer failures, verification mismatches, or fatal error
  2  invalid arguments (also used by --self-check failures)
"""

import argparse
import ctypes
import ctypes.util
import json
import math
import random
import signal
import struct
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

MIB = 1024 * 1024
GIB = 1024 * 1024 * 1024
PAGE = 4096

# Bytes at the start of block 0 that carry the per-request stamp. The rest of
# the block keeps the block-index pattern used for placement verification.
STAMP_SIZE = 8


def parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="Simulate cross-node KV cache traffic over Mooncake "
        "Transfer Engine.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--mode", choices=["target", "initiator"], required=True)
    p.add_argument(
        "--local_server_name",
        required=True,
        help="Local segment name, e.g. mca or 10.10.0.1:12345",
    )
    p.add_argument(
        "--target_server_name",
        default="",
        help="Target segment name (initiator mode only)",
    )
    p.add_argument(
        "--metadata_server",
        default="P2PHANDSHAKE",
        help="Metadata service address or P2PHANDSHAKE",
    )
    p.add_argument(
        "--protocol",
        default="rdma",
        help="Transport protocol, e.g. rdma, tcp, efa",
    )
    p.add_argument(
        "--device_name",
        default="",
        help="Local RDMA device, e.g. rxe-a, mlx5_0 (empty: auto)",
    )
    p.add_argument(
        "--pool_size_gb",
        type=float,
        default=1.0,
        help="KV pool size in GiB (must match on both sides)",
    )
    p.add_argument(
        "--use_gpu",
        action="store_true",
        help="Allocate the pool in GPU memory (requires torch and a GPU)",
    )
    p.add_argument("--gpu_id", type=int, default=0)

    w = p.add_argument_group("workload")
    w.add_argument(
        "--block_size",
        type=int,
        default=64 * 1024,
        help="Single KV block size in bytes, must be 4 KiB aligned",
    )
    w.add_argument(
        "--blocks_per_request",
        type=int,
        default=256,
        help="KV blocks moved by one simulated request",
    )
    w.add_argument(
        "--num_requests",
        type=int,
        default=200,
        help="Total simulated requests to issue (initiator mode)",
    )
    w.add_argument(
        "--concurrency",
        type=int,
        default=8,
        help="Concurrent requests in flight (initiator mode)",
    )
    w.add_argument(
        "--op",
        choices=["write", "read", "mix"],
        default="write",
        help="Transfer direction per request",
    )
    w.add_argument(
        "--rate",
        type=float,
        default=0.0,
        help="Global request rate limit in req/s (0 = unlimited)",
    )
    w.add_argument(
        "--warmup",
        type=int,
        default=5,
        help="Warmup transfers before measurement",
    )
    w.add_argument(
        "--num_slots",
        type=int,
        default=0,
        help="Slots in the pool (0 = derive from pool size)",
    )
    w.add_argument(
        "--verify",
        action="store_true",
        help="Read back write requests and check content and stamp",
    )
    w.add_argument(
        "--verify_blocks",
        type=int,
        default=4,
        help="Blocks per verified request to read back (including block 0)",
    )
    w.add_argument("--seed", type=int, default=0x4B56)

    o = p.add_argument_group("output")
    o.add_argument(
        "--info-file",
        default="",
        help="Target mode: write pool info JSON here",
    )
    o.add_argument(
        "--target-info-file",
        default="",
        help="Initiator mode: read pool info written by target",
    )
    o.add_argument(
        "--output_json",
        default="",
        help="Initiator mode: write result metrics JSON here",
    )
    o.add_argument(
        "--self-check",
        action="store_true",
        help="Validate arguments and print the workload plan, then exit",
    )
    return p.parse_args(argv)


# ---------------------------------------------------------------------------
# memory helpers
# ---------------------------------------------------------------------------


def alloc_cpu_memory(size_bytes):
    """Allocate page-aligned CPU memory, preferring 2 MiB hugepages."""
    libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)
    PROT_READ, PROT_WRITE = 0x1, 0x2
    MAP_PRIVATE, MAP_ANONYMOUS, MAP_HUGETLB = 0x02, 0x20, 0x40000
    MAP_FAILED = ctypes.c_void_p(-1).value

    libc.mmap.restype = ctypes.c_void_p
    libc.mmap.argtypes = [
        ctypes.c_void_p,
        ctypes.c_size_t,
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_int,
        ctypes.c_long,
    ]

    flags = MAP_PRIVATE | MAP_ANONYMOUS
    ptr = libc.mmap(None, size_bytes, PROT_READ | PROT_WRITE,
                    flags | MAP_HUGETLB, -1, 0)
    if ptr and ptr != MAP_FAILED:
        return ptr, True
    ptr = libc.mmap(None, size_bytes, PROT_READ | PROT_WRITE, flags, -1, 0)
    if not ptr or ptr == MAP_FAILED:
        raise RuntimeError(
            f"mmap failed for {size_bytes} bytes, errno={ctypes.get_errno()}"
        )
    return ptr, False


def alloc_gpu_memory(size_bytes, gpu_id):
    import torch

    torch.cuda.set_device(gpu_id)
    tensor = torch.zeros(size_bytes, dtype=torch.uint8, device=f"cuda:{gpu_id}")
    return tensor.data_ptr(), tensor


def allocate(size_bytes, args):
    if args.use_gpu:
        addr, tensor = alloc_gpu_memory(size_bytes, args.gpu_id)
        return addr, tensor
    addr, _huge = alloc_cpu_memory(size_bytes)
    return addr, None


# ---------------------------------------------------------------------------
# workload helpers
# ---------------------------------------------------------------------------


def block_fill_byte(index):
    return index & 0xFF


def build_static_payload(block_size, blocks_per_request):
    """One payload where block i is filled with byte (i & 0xFF).

    The first STAMP_SIZE bytes of block 0 are reserved for a per-request
    stamp that the initiator patches before every write.
    """
    block = bytearray([block_fill_byte(0)] * block_size)
    block[:STAMP_SIZE] = b"\x00" * STAMP_SIZE
    chunks = [bytes(block)]
    for i in range(1, blocks_per_request):
        chunks.append(bytes([block_fill_byte(i)]) * block_size)
    return b"".join(chunks)


def percentile(sorted_values, pct):
    if not sorted_values:
        return 0.0
    idx = max(0, min(len(sorted_values) - 1,
                     int(math.ceil(pct / 100.0 * len(sorted_values))) - 1))
    return sorted_values[idx]


def resolve_plan(args):
    """Validate arguments and derive the workload geometry."""
    if args.block_size <= 0 or args.block_size % PAGE != 0:
        raise ValueError(f"--block_size must be a positive multiple of {PAGE}")
    if args.blocks_per_request <= 0:
        raise ValueError("--blocks_per_request must be positive")
    if args.num_requests <= 0:
        raise ValueError("--num_requests must be positive")
    if args.concurrency <= 0:
        raise ValueError("--concurrency must be positive")
    if args.pool_size_gb <= 0:
        raise ValueError("--pool_size_gb must be positive")
    if args.rate < 0:
        raise ValueError("--rate must be >= 0")
    if args.verify and args.verify_blocks <= 0:
        raise ValueError("--verify_blocks must be positive")
    if args.verify and args.verify_blocks > args.blocks_per_request:
        raise ValueError("--verify_blocks cannot exceed --blocks_per_request")
    if args.op == "read" and args.verify:
        raise ValueError("--verify needs writes; use --op write or mix")

    request_size = args.block_size * args.blocks_per_request
    pool_bytes = int(args.pool_size_gb * GIB)
    if pool_bytes < request_size:
        raise ValueError(
            f"pool too small: {pool_bytes} bytes < one request "
            f"({request_size} bytes)"
        )
    max_slots = pool_bytes // request_size
    num_slots = args.num_slots if args.num_slots > 0 else min(64, max_slots)
    if num_slots > max_slots:
        raise ValueError(
            f"--num_slots {num_slots} exceeds pool capacity {max_slots}"
        )
    return {
        "block_size": args.block_size,
        "blocks_per_request": args.blocks_per_request,
        "request_size": request_size,
        "pool_bytes": pool_bytes,
        "num_slots": num_slots,
        "slot_stride": request_size,
        "concurrency": args.concurrency,
        "num_requests": args.num_requests,
    }


def print_plan(plan, args):
    total_bytes = plan["request_size"] * args.num_requests
    print("KV traffic plan")
    print(f"  mode            : {args.mode}")
    print(f"  protocol/device : {args.protocol} / "
          f"{args.device_name or 'auto'}")
    print(f"  block size      : {plan['block_size']} bytes")
    print(f"  blocks/request  : {plan['blocks_per_request']}")
    print(f"  request size    : {plan['request_size'] / MIB:.2f} MiB")
    print(f"  pool            : {plan['pool_bytes'] / GIB:.2f} GiB, "
          f"{plan['num_slots']} slots")
    print(f"  requests        : {args.num_requests} "
          f"({total_bytes / MIB:.1f} MiB total)")
    print(f"  concurrency     : {plan['concurrency']}")
    print(f"  op / verify     : {args.op} / {args.verify}")
    if args.rate > 0:
        print(f"  rate limit      : {args.rate:.2f} req/s")


# ---------------------------------------------------------------------------
# target
# ---------------------------------------------------------------------------


def run_target(args):
    from mooncake.engine import TransferEngine

    plan = resolve_plan(args)
    engine = TransferEngine()
    ret = engine.initialize(
        args.local_server_name, args.metadata_server, args.protocol,
        args.device_name,
    )
    if ret != 0:
        raise RuntimeError(f"engine initialization failed: {ret}")

    pool_addr, keepalive = allocate(plan["pool_bytes"], args)
    ret = engine.register_memory(pool_addr, plan["pool_bytes"])
    if ret != 0:
        raise RuntimeError(f"memory registration failed: {ret}")

    server_name = args.local_server_name
    rpc_name = server_name
    if args.metadata_server == "P2PHANDSHAKE":
        host = server_name.rpartition(":")[0]
        rpc_name = f"{host}:{engine.get_rpc_port()}"

    info = {
        "server_name": server_name,
        "rpc_server_name": rpc_name,
        "pool_addr": pool_addr,
        "pool_bytes": plan["pool_bytes"],
        "slot_stride": plan["slot_stride"],
        "num_slots": plan["num_slots"],
        "protocol": args.protocol,
    }
    payload = json.dumps(info)
    if args.info_file:
        with open(args.info_file, "w", encoding="utf-8") as f:
            f.write(payload + "\n")
    print(f"TARGET_INFO:{payload}", flush=True)

    stop = threading.Event()
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    print("target ready, waiting for initiator (Ctrl+C to stop)", flush=True)
    stop.wait()
    del keepalive
    print("target stopped", flush=True)
    return 0


# ---------------------------------------------------------------------------
# initiator
# ---------------------------------------------------------------------------


class Metrics:
    def __init__(self):
        self.lock = threading.Lock()
        self.latencies = []
        self.success = 0
        self.failed = 0
        self.verified = 0
        self.mismatched = 0
        self.errors = []

    def record(self, latency_s, ok, error=""):
        with self.lock:
            self.latencies.append(latency_s)
            if ok:
                self.success += 1
            else:
                self.failed += 1
                if error and len(self.errors) < 8:
                    self.errors.append(error)

    def record_verify(self, ok, error=""):
        with self.lock:
            self.verified += 1
            if not ok:
                self.mismatched += 1
                if error and len(self.errors) < 8:
                    self.errors.append(error)


def load_target_info(args):
    info = {}
    if args.target_info_file:
        with open(args.target_info_file, "r", encoding="utf-8") as f:
            info = json.load(f)
    server_name = args.target_server_name or info.get("server_name", "")
    if not server_name:
        raise ValueError("--target_server_name or --target-info-file required")
    return server_name, info


def run_initiator(args):
    from mooncake.engine import TransferEngine

    plan = resolve_plan(args)
    target_name, info = load_target_info(args)

    engine = TransferEngine()
    ret = engine.initialize(
        args.local_server_name, args.metadata_server, args.protocol,
        args.device_name,
    )
    if ret != 0:
        raise RuntimeError(f"engine initialization failed: {ret}")

    remote_addr = info.get("pool_addr", 0)
    if not remote_addr:
        remote_addr = engine.get_first_buffer_address(target_name)
    if not remote_addr:
        raise RuntimeError(
            f"cannot resolve target buffer for {target_name}; "
            "check that the target is running and registered"
        )

    request_size = plan["request_size"]
    block_size = plan["block_size"]
    num_blocks = plan["blocks_per_request"]
    assert request_size == block_size * num_blocks

    payload = build_static_payload(block_size, num_blocks)

    if info.get("slot_stride") and info["slot_stride"] != request_size:
        raise ValueError(
            f"slot_stride mismatch: target={info['slot_stride']} "
            f"local={request_size}; check --block_size/--blocks_per_request"
        )
    if info.get("num_slots") and info["num_slots"] != plan["num_slots"]:
        raise ValueError(
            f"num_slots mismatch: target={info['num_slots']} "
            f"local={plan['num_slots']}; check --pool_size_gb/--num_slots"
        )

    # Warm up: establish endpoints / QPs before the measured phase.
    warm_addr, warm_keep = allocate(block_size, args)
    engine.register_memory(warm_addr, block_size)
    for _ in range(max(0, args.warmup)):
        engine.transfer_sync_write(target_name, warm_addr, remote_addr,
                                   block_size)

    total_requests = args.num_requests
    metrics = Metrics()
    issued = [0]
    counter_lock = threading.Lock()
    rng_lock = threading.Lock()
    rng = random.Random(args.seed)
    rate = args.rate
    next_slot_time = [time.perf_counter()]

    def next_request_index():
        with counter_lock:
            if issued[0] >= total_requests:
                return None
            rid = issued[0]
            issued[0] += 1
        return rid

    def pick_slot():
        with rng_lock:
            return rng.randrange(plan["num_slots"])

    def pick_op():
        if args.op != "mix":
            return args.op
        with rng_lock:
            return "write" if rng.random() < 0.5 else "read"

    def throttle():
        if rate <= 0:
            return
        with rng_lock:
            now = time.perf_counter()
            if next_slot_time[0] < now:
                next_slot_time[0] = now
            next_slot_time[0] += 1.0 / rate
            wait = next_slot_time[0] - now
        if wait > 0:
            time.sleep(wait)

    def worker(_wid):
        local_addr, keep = allocate(request_size, args)
        engine.register_memory(local_addr, request_size)
        verify_addr = 0
        verify_keep = None
        if args.verify:
            verify_addr, verify_keep = allocate(block_size, args)
            engine.register_memory(verify_addr, block_size)

        try:
            while True:
                rid = next_request_index()
                if rid is None:
                    return
                throttle()
                slot = pick_slot()
                remote_base = remote_addr + slot * request_size
                op = pick_op()

                if op == "write":
                    engine.write_bytes_to_buffer(local_addr, payload,
                                                 request_size)
                    engine.write_bytes_to_buffer(
                        local_addr, struct.pack("<Q", rid), STAMP_SIZE)
                    src = [local_addr + i * block_size
                           for i in range(num_blocks)]
                    dst = [remote_base + i * block_size
                           for i in range(num_blocks)]
                    t0 = time.perf_counter()
                    rc = engine.batch_transfer_sync_write(
                        target_name, src, dst, [block_size] * num_blocks)
                    dt = time.perf_counter() - t0
                    metrics.record(dt, rc == 0,
                                   "" if rc == 0 else f"write rc={rc}")
                    if rc == 0 and args.verify:
                        check_indices = [0]
                        step = max(1, num_blocks // max(1, args.verify_blocks))
                        check_indices += list(range(step, num_blocks, step))
                        check_indices = check_indices[:args.verify_blocks]
                        for idx in check_indices:
                            r = engine.transfer_sync_read(
                                target_name, verify_addr,
                                remote_base + idx * block_size, block_size)
                            if r != 0:
                                metrics.record_verify(
                                    False, f"verify read rc={r} blk={idx}")
                                continue
                            data = engine.read_bytes_from_buffer(
                                verify_addr, block_size)
                            if idx == 0:
                                stamp = struct.unpack("<Q",
                                                      data[:STAMP_SIZE])[0]
                                ok = (stamp == rid and
                                      data[STAMP_SIZE:] ==
                                      payload[STAMP_SIZE:block_size])
                                err = f"stamp mismatch blk=0 req={rid}"
                            else:
                                start = idx * block_size
                                ok = data == payload[start:start + block_size]
                                err = f"content mismatch blk={idx} req={rid}"
                            metrics.record_verify(ok, "" if ok else err)
                else:
                    dst = [local_addr + i * block_size
                           for i in range(num_blocks)]
                    src = [remote_base + i * block_size
                           for i in range(num_blocks)]
                    t0 = time.perf_counter()
                    rc = engine.batch_transfer_sync_read(
                        target_name, dst, src, [block_size] * num_blocks)
                    dt = time.perf_counter() - t0
                    metrics.record(dt, rc == 0,
                                   "" if rc == 0 else f"read rc={rc}")
        finally:
            del keep
            del verify_keep

    wall_start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=plan["concurrency"]) as pool:
        futures = [pool.submit(worker, i) for i in range(plan["concurrency"])]
        for f in futures:
            f.result()
    wall = time.perf_counter() - wall_start

    moved_bytes = request_size * (metrics.success + metrics.failed)
    result = {
        "requests": total_requests,
        "success": metrics.success,
        "failed": metrics.failed,
        "blocks": num_blocks * (metrics.success + metrics.failed),
        "bytes": moved_bytes,
        "wall_s": round(wall, 6),
        "throughput_gbps": round(moved_bytes / 1e9 / wall, 4) if wall else 0.0,
        "requests_per_s": round(total_requests / wall, 2) if wall else 0.0,
        "latency_ms": {},
        "verify": {
            "enabled": bool(args.verify),
            "checked": metrics.verified,
            "mismatched": metrics.mismatched,
        },
        "errors": metrics.errors,
    }
    lat_sorted = sorted(metrics.latencies)
    if lat_sorted:
        result["latency_ms"] = {
            "mean": round(1000 * sum(lat_sorted) / len(lat_sorted), 3),
            "p50": round(1000 * percentile(lat_sorted, 50), 3),
            "p95": round(1000 * percentile(lat_sorted, 95), 3),
            "p99": round(1000 * percentile(lat_sorted, 99), 3),
            "max": round(1000 * lat_sorted[-1], 3),
        }

    report = {
        "config": {
            "mode": "initiator",
            "target": target_name,
            "protocol": args.protocol,
            "device_name": args.device_name,
            "op": args.op,
            "block_size": block_size,
            "blocks_per_request": num_blocks,
            "pool_size_gb": args.pool_size_gb,
            "num_slots": plan["num_slots"],
            "concurrency": plan["concurrency"],
            "rate": args.rate,
            "verify": bool(args.verify),
            "seed": args.seed,
        },
        "result": result,
    }

    text = json.dumps(report, indent=2)
    print(text)
    if args.output_json:
        with open(args.output_json, "w", encoding="utf-8") as f:
            f.write(text + "\n")

    ok = result["failed"] == 0 and result["verify"]["mismatched"] == 0
    return 0 if ok else 1


def main(argv=None):
    args = parse_args(argv)
    try:
        plan = resolve_plan(args)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    if args.self_check:
        print_plan(plan, args)
        print("self-check: OK")
        return 0

    try:
        if args.mode == "target":
            return run_target(args)
        return run_initiator(args)
    except Exception as exc:  # noqa: BLE001 - report and exit non-zero
        print(f"error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
