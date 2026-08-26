"""collect_v2/infra.py 最小冒烟：分类重试、限速间隔、工作池并发封顶。

运行：python3 -m collect_v2.smoke_infra
"""

from __future__ import annotations

import asyncio
import socket
import time

import httpx

from collect_v2 import infra


async def main() -> None:
    infra.RETRY_INTERVAL = 0.05  # 冒烟加速，不改拍板默认值

    # 1) 确定性失败：404 立即抛错、零重试
    calls = {"n": 0}

    def h404(req):
        calls["n"] += 1
        return httpx.Response(404)

    infra.set_client(httpx.AsyncClient(transport=httpx.MockTransport(h404)), proxy=True)
    try:
        await infra.request("wikimedia", "GET", "http://mock/a")
        raise AssertionError("404 应抛 DeterministicError")
    except infra.DeterministicError:
        pass
    assert calls["n"] == 1, f"404 不应重试, calls={calls['n']}"
    print("[PASS] 404 确定性失败零重试")

    # 2) 瞬态失败转成功：500,500,200 共 3 次请求
    seq = iter([500, 500, 200])
    calls2 = {"n": 0}

    def h500_then_ok(req):
        calls2["n"] += 1
        return httpx.Response(next(seq))

    infra.set_client(httpx.AsyncClient(transport=httpx.MockTransport(h500_then_ok)), proxy=True)
    resp = await infra.request("wikimedia", "GET", "http://mock/b")
    assert resp.status_code == 200 and calls2["n"] == 3, calls2
    print("[PASS] 瞬态 500 重试后成功（3 次请求）")

    # 3) 瞬态失败用尽：恒 500，共 4 次请求后抛 TransientExhaustedError
    calls3 = {"n": 0}

    def h500(req):
        calls3["n"] += 1
        return httpx.Response(500)

    infra.set_client(httpx.AsyncClient(transport=httpx.MockTransport(h500)), proxy=True)
    try:
        await infra.request("wikimedia", "GET", "http://mock/c")
        raise AssertionError("恒 500 应抛 TransientExhaustedError")
    except infra.TransientExhaustedError:
        pass
    assert calls3["n"] == infra.MAX_RETRIES + 1, calls3
    print("[PASS] 瞬态重试用尽（1+3 次）抛 TransientExhaustedError")

    # 4) 域名非法：gaierror 链 → DeterministicError 零重试
    calls4 = {"n": 0}

    def h_dns(req):
        calls4["n"] += 1
        try:
            raise socket.gaierror(-2, "Name or service not known")
        except socket.gaierror as e:
            raise httpx.ConnectError("dns fail", request=req) from e

    infra.set_client(httpx.AsyncClient(transport=httpx.MockTransport(h_dns)), proxy=True)
    try:
        await infra.request("wikimedia", "GET", "http://mock/d")
        raise AssertionError("域名非法应抛 DeterministicError")
    except infra.DeterministicError:
        pass
    assert calls4["n"] == 1, calls4
    print("[PASS] 域名非法确定性失败零重试")

    # 5) 限速器最小间隔：rate=20，5 次 acquire 至少 4/20 秒
    rl = infra.RateLimiter(rate=20.0)
    t0 = time.monotonic()
    for _ in range(5):
        await rl.acquire()
    elapsed = time.monotonic() - t0
    assert elapsed >= 0.2 * 0.9, f"限速间隔不足: {elapsed:.3f}s"
    print(f"[PASS] 限速器最小间隔生效（5 次耗时 {elapsed:.3f}s）")

    # 6) 工作池并发封顶：limit=2，峰值并发不超 2
    pool = infra.WorkPool(limit=2)
    state = {"cur": 0, "max": 0}

    async def job():
        state["cur"] += 1
        state["max"] = max(state["max"], state["cur"])
        await asyncio.sleep(0.05)
        state["cur"] -= 1

    for _ in range(6):
        pool.submit(job())
    await pool.join()
    assert state["max"] <= 2, state
    print(f"[PASS] 工作池并发封顶（峰值 {state['max']} ≤ 2）")

    # 7) 未登记源拒绝放行
    try:
        infra.gate_for("no_such_source")
        raise AssertionError("未登记源应报错")
    except ValueError:
        pass
    print("[PASS] 未登记源拒绝放行")

    await infra.close_client()
    print("冒烟全部通过")


if __name__ == "__main__":
    asyncio.run(main())
