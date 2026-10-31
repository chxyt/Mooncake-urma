# KV 流量模拟工具（kv_traffic_sim.py）

用于在没有 GPU、没有真实 RDMA 网卡的环境里，模拟大模型推理时的跨节点 KV Cache 流量，从而验证 Mooncake Transfer Engine 的传输行为（尤其是 RDMA transport 的改动）。

- 脚本：`mooncake-transfer-engine/example/kv_traffic_sim.py`
- target 进程：注册一块 KV 池，等待 initiator
- initiator 进程：按“块 → 请求 → 池槽位”的语义批量搬运 KV，并统计延迟/吞吐/失败

---

## 1. 为什么是这个模型

真实推理里，KV Cache 的搬运具有这几个特征，工具都做了对应：

| 真实场景 | 工具里的对应 |
|---|---|
| KV 按 block 组织（如 64 KiB / 块） | `--block_size` |
| 一个请求的 KV 是连续的多块 | `--blocks_per_request` |
| 池子会复用、覆盖（LRU 淘汰） | 请求随机落在 `--num_slots` 个槽位上 |
| prefill 写 KV / decode 读 KV | `--op write / read / mix` |
| 多请求并发 | `--concurrency` |
| 需要确认数据真的写到了对端 | `--verify`（回读校验 + 每请求 stamp） |

它不加载模型，不需要 CUDA，也不需要推理引擎，因此非常适合在 SoftRoCE 环境里做 RDMA 改动的回归验证。

---

## 2. 前置条件

### 2.1 Python 绑定

```bash
# 用当前源码构建（带上你的改动）
cd /home/work/mk/Mooncake
python3 -m pip install . -Ccmake.define.USE_CUDA=OFF
```

或者使用现成的非 CUDA wheel（仅用于对照）：

```bash
pip install mooncake-transfer-engine-non-cuda
```

### 2.2 RDMA 设备

CPU 环境用 SoftRoCE 即可（完整步骤见仓库根目录的 `rdma-validation-env.md`）：

```bash
sudo modprobe rdma_rxe
sudo ip netns exec mca rdma link add rxe-a type rxe netdev veth-a
sudo ip netns exec mca ibv_devinfo -l
```

没有 RDMA 设备时，也可以先用 `--protocol tcp` 跑通流程，但这样就**不会覆盖 RDMA 代码路径**。

### 2.3 自检（不需要设备）

```bash
python3 mooncake-transfer-engine/example/kv_traffic_sim.py \
  --mode initiator --local_server_name mca --target_server_name mcb \
  --self-check
```

输出计划并以 0 退出，参数不合法时以 2 退出。适合放进 CI 做静态检查。

---

## 3. 快速开始

### 3.1 target（KV 池持有方）

```bash
sudo ip netns exec mcb python3 mooncake-transfer-engine/example/kv_traffic_sim.py \
  --mode target \
  --local_server_name mcb \
  --protocol rdma \
  --device_name rxe-b \
  --pool_size_gb 1 \
  --info-file /tmp/kv_target.json
```

启动后打印一行：

```
TARGET_INFO:{"server_name": "mcb", "rpc_server_name": "mcb:xxxxx", "pool_addr": 140..., "pool_bytes": 1073741824, ...}
```

### 3.2 initiator（流量发生方）

```bash
sudo ip netns exec mca python3 mooncake-transfer-engine/example/kv_traffic_sim.py \
  --mode initiator \
  --local_server_name mca \
  --target_server_name mcb \
  --target-info-file /tmp/kv_target.json \
  --protocol rdma \
  --device_name rxe-a \
  --block_size 65536 \
  --blocks_per_request 256 \
  --num_requests 200 \
  --concurrency 8 \
  --op mix \
  --verify \
  --output_json /tmp/kv_result.json
```

> 两个 netns 共用文件系统，`/tmp/kv_target.json` 可以直接被 initiator 读取。跨机时把该文件拷过去，或手动传 `--target_server_name` + `--pool_size_gb`。

---

## 4. 参数说明

| 参数 | 默认值 | 说明 |
|---|---|---|
| `--mode` | 必填 | `target` / `initiator` |
| `--local_server_name` | 必填 | 本端 segment 名，如 `mca` 或 `10.10.0.1:12345` |
| `--target_server_name` | 空 | initiator 侧对端 segment 名 |
| `--metadata_server` | `P2PHANDSHAKE` | 也可用 `etcd://ip:2379` |
| `--protocol` | `rdma` | `rdma` / `tcp` / `efa` 等 |
| `--device_name` | 空 | 本端 RDMA 设备，如 `rxe-a`；空表示自动发现 |
| `--pool_size_gb` | 1 | KV 池大小（GiB），两端必须一致 |
| `--block_size` | 65536 | 单块字节数，须 4 KiB 对齐 |
| `--blocks_per_request` | 256 | 每请求块数（默认 16 MiB/请求） |
| `--num_requests` | 200 | 总请求数 |
| `--concurrency` | 8 | 并发请求数 |
| `--op` | `write` | `write` / `read` / `mix` |
| `--rate` | 0 | 全局限速（req/s），0 不限 |
| `--warmup` | 5 | 预热传输次数（建链，不计入统计） |
| `--num_slots` | 0 | 池槽位数，0 表示按池大小推导（默认 64） |
| `--verify` | 关 | 回读校验写入内容与 stamp |
| `--verify_blocks` | 4 | 每请求回读抽检的块数（含第 0 块） |
| `--seed` | 固定 | 槽位/方向选择的随机种子，保证可复现 |
| `--info-file` | 空 | target 写池信息 JSON |
| `--target-info-file` | 空 | initiator 读池信息 JSON |
| `--output_json` | 空 | initiator 结果落盘 |
| `--self-check` | 关 | 只校验参数并打印计划 |

`--verify` 的语义：每次写请求前，在本地第 0 块头部写入本次请求序号（stamp）；写完后回读抽检块，校验 stamp 与块内容。**写入丢失、写错偏移、数据截断都会判定失败**。

---

## 5. 输出

```json
{
  "config": {
    "op": "mix", "block_size": 65536, "blocks_per_request": 256,
    "concurrency": 8, "verify": true, "num_slots": 64
  },
  "result": {
    "requests": 200,
    "success": 200,
    "failed": 0,
    "blocks": 51200,
    "bytes": 3355443200,
    "wall_s": 1.842,
    "throughput_gbps": 1.8217,
    "requests_per_s": 108.58,
    "latency_ms": {"mean": 61.2, "p50": 58.9, "p95": 102.4, "p99": 121.7, "max": 130.2},
    "verify": {"enabled": true, "checked": 800, "mismatched": 0},
    "errors": []
  }
}
```

退出码：`0` 全部成功且校验通过；`1` 有传输失败或校验不一致；`2` 参数错误。

> `throughput_gbps` 的单位是 **GB/s**（10^9 字节/秒）。SoftRoCE 下的数值没有性能参考意义，只用于发现“是否明显退化”。

---

## 6. 验证标准

### 6.1 必须通过（P0）

| 编号 | 用例 | 命令要点 | 判定 |
|---|---|---|---|
| V1 | 参数自检 | `--self-check` | 退出码 0 |
| V2 | 写路径 | `--op write --verify` | `failed=0` 且 `verify.mismatched=0`，退出码 0 |
| V3 | 读路径 | `--op read` | `failed=0`，退出码 0 |
| V4 | 混合路径 | `--op mix --verify` | `failed=0` 且 `mismatched=0` |
| V5 | 并发扩展 | `--concurrency 1/8/32` | 三档均 `failed=0`，无崩溃 |
| V6 | 大块传输 | `--block_size 1048576 --blocks_per_request 32` | 覆盖 MR 分块，`failed=0` |
| V7 | 池覆盖复用 | `--num_slots 4`（远小于请求数） | 覆盖写不报错，`mismatched=0` |

### 6.2 稳定性（P1）

```bash
--num_requests 5000 --rate 50 --duration 约 100s
```

判定：`failed=0`、`mismatched=0`、进程 RSS 无明显增长（可用 `ps` 前后对比）。

### 6.3 故障注入（P1）

| 操作 | 期望 |
|---|---|
| 传输中 `ip link set veth-a down` | 本轮出现 `failed>0`，进程不崩溃、不 hang |
| 链路恢复后重新发起请求 | `failed` 不再增长，`rc=0` |
| `kill -9` target 后 initiator 继续发请求 | 明确报错（`rc!=0` 或异常），不 segfault |

故障注入期间**允许失败**，判定重点是：不崩溃、不静默成功、错误可观测。

### 6.4 环境真实性确认（每次回归都做）

- `ibv_devinfo -l` 能看到 `rxe-a/rxe-b`
- 运行后 `/sys/class/infiniband/rxe-a/ports/1/counters/port_xmit_data` 增长
- 把 veth down 掉后请求必须失败（证明没有静默回退到 TCP）

---

## 7. 推荐回归矩阵

| 轮次 | 参数 | 目的 |
|---|---|---|
| R1 冒烟 | block 64K / 16 blocks / 20 req / concurrency 1 | 1 分钟内跑通 |
| R2 基线 | block 64K / 256 blocks / 200 req / concurrency 8 / mix + verify | 主回归 |
| R3 并发 | concurrency 1 / 8 / 32 | 连接与 QP 复用 |
| R4 大块 | block 1M / 32 blocks | 大 MR / 分块注册 |
| R5 长稳 | 5000 req / rate 50 / mix + verify | 泄漏与重连 |
| R6 故障 | R2 + 链路 down/up、kill target | 健壮性 |

每轮保存 `--output_json`，改动前后对比：**V1–V7 必须全绿；p99 与 GB/s 仅记录，不作为性能结论**。

---

## 8. 故障排查

| 现象 | 原因 | 处理 |
|---|---|---|
| `cannot resolve target buffer` | target 未启动 / segment 名不一致 | 用 target 打印的 `server_name`，或传 `--target-info-file` |
| `slot_stride mismatch` | 两端 block 几何参数不一致 | 两端统一 `--block_size` / `--blocks_per_request` |
| `num_slots mismatch` | 两端池大小不一致 | 统一 `--pool_size_gb` 或 `--num_slots` |
| 全部成功但真机没有 RDMA 流量 | 自动回退 TCP | 检查 `--device_name`、`ibv_devinfo`、GID |
| `memory registration failed` | 内存不足 / hugepage 配置 | 降低 `--pool_size_gb`、减少 `--concurrency` |
| 首次请求延迟特别高 | 建链开销 | 增大 `--warmup` |

---

## 9. 与后续 RDMA 改动的关系

改 RDMA transport 后，按第 7 节矩阵重跑，重点观察：

1. `failed` 与 `mismatched` 是否为 0（正确性）
2. 故障注入下是否出现 hang（QP 状态机）
3. 大块用例是否触发 `max_mr_size`（内存注册分块）
4. 并发用例下 QP 是否被正确复用/回收（连接管理）

性能数字请留到真实 RDMA 网卡上再测。
