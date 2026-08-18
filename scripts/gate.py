#!/usr/bin/env python3
"""agent-transport 性能门禁 —— 确定性 benchmark + baseline 比对（回归 >1% 红门）。

用法（参考机，release 构建）：
  python scripts/gate.py            # 与已提交 bench-data/baseline.json 比对
  python scripts/gate.py --init     # 首次记录 baseline（负载哈希锁定，漂移即拒）

口径：
  - 跑 RUNS 轮确定性 benchmark（固定负载、固定 seed、零随机）；
  - 延迟取三轮最优（min short_p50/p99），吞吐取三轮最优（max ops/s）——
    与已提交 baseline 相比，任一维度回归 >1% → 退出码 1（CI 红门）。
"""
import json
import subprocess
import sys
from pathlib import Path

# Windows 控制台默认 GBK，统一 UTF-8 输出，避免中文乱码
try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

ROOT = Path(__file__).resolve().parent.parent
BASELINE = ROOT / "bench-data" / "baseline.json"
RUNS = 3
REGRESSION = 0.01


def run_bench() -> list:
    """跑 deterministic bench，返回各轮 agent-transport 指标 dict。"""
    results = []
    for _ in range(RUNS):
        out = subprocess.run(
            ["cargo", "bench", "--bench", "mixed", "--", "--json"],
            cwd=ROOT, capture_output=True, text=True, encoding="utf-8",
        )
        if out.returncode != 0:
            sys.exit(f"bench 失败:\n{out.stderr}")
        for line in out.stdout.splitlines():
            line = line.strip()
            if line.startswith('{"tag":"agent-transport"'):
                results.append(json.loads(line))
                break
        else:
            sys.exit("未找到 agent-transport 输出行")
    return results


def best(results: list, field: str, minimize: bool = True) -> float:
    vals = [r[field] for r in results]
    return (min if minimize else max)(vals)


def main() -> None:
    init = "--init" in sys.argv
    rs = run_bench()
    p50 = best(rs, "short_p50_us")
    p99 = best(rs, "short_p99_us")
    ops = best(rs, "ops_per_sec", minimize=False)
    wl_hash = rs[0]["workload_hash"]

    new_data = {
        "workload_hash": wl_hash,
        "short_p50_us": round(p50, 1),
        "short_p99_us": round(p99, 1),
        "ops_per_sec": round(ops, 1),
        "runs": RUNS,
        "regression_fraction": REGRESSION,
    }

    if init or not BASELINE.exists():
        BASELINE.parent.mkdir(parents=True, exist_ok=True)
        BASELINE.write_text(json.dumps(new_data, indent=2) + "\n")
        print(f"[gate] baseline 已记录 → {BASELINE.name}（workload_hash={wl_hash}）")
        print(json.dumps(new_data, indent=2))
        return

    base = json.loads(BASELINE.read_text())
    if base.get("workload_hash") != wl_hash:
        sys.exit(f"[gate] 工作负载漂移：baseline 哈希 {base.get('workload_hash')} ≠ 当前 {wl_hash}。"
                 f"改过 bench 常量？请 --init 重录 baseline。")

    checks = [
        ("short_p50_us", p50, base["short_p50_us"], True, "短RPC P50"),
        ("short_p99_us", p99, base["short_p99_us"], True, "短RPC P99"),
        ("ops_per_sec", ops, base["ops_per_sec"], False, "吞吐 ops/s"),
    ]
    failed = False
    print("[gate] 与 baseline 比对（回归 >1% 红门）:")
    for field, cur, b, is_latency, name in checks:
        if is_latency:
            bad = cur > b * (1 + REGRESSION)
            ratio = cur / b if b else float("inf")
            note = "FAIL" if bad else "ok"
            print(f"  {name:<10} baseline {b:>10.1f}  现在 {cur:>10.1f}  比值 {ratio:>6.3f}  {note}")
        else:
            bad = cur < b * (1 - REGRESSION)
            ratio = cur / b if b else float("inf")
            note = "FAIL" if bad else "ok"
            print(f"  {name:<10} baseline {b:>10.1f}  现在 {cur:>10.1f}  比值 {ratio:>6.3f}  {note}")
        failed |= bad

    if failed:
        sys.exit("[gate] GATE FAIL：性能回归超过 1%，检查本次改动。")
    print("[gate] GATE PASS")


if __name__ == "__main__":
    main()
