"""端到端：真实 aiocqhttp（AstrBot 侧）+ 真实 WS 服务端（ArkLink 侧）+ 中继本体。

链路：

    AstrBot（真实 CQHttp(use_ws_reverse=True)，监听 127.0.0.1:P2/ws）
        ▲  X-Self-ID / X-Client-Role: Universal
        │
    ArkBridge（两头都主动连）
        │  OneBot 11 正向 WS 客户端
        ▼
    假 ArkLink（真 WebSocket 服务端，扮演 `arklink serve onebot`）

没有任何 mock：握手、掩码帧、echo 配对、事件格式全部是真的。
"""

from __future__ import annotations

import asyncio
import logging
import socket
import sys
import time
from pathlib import Path

_HERE = Path(__file__).resolve().parent
ROOT = _HERE.parents[1]
sys.path[:0] = [str(_HERE), str(_HERE.parent), str(ROOT)]

try:
    from aiocqhttp import CQHttp  # noqa: F401  仅仅用来判断有没有装
except ImportError:  # pragma: no cover
    print(
        "SKIP: 未安装 aiocqhttp —— 这条用例的对端必须是真的 AstrBot 协议栈才有意义。\n"
        "      pip install aiocqhttp quart websockets  然后重跑。"
    )
    raise SystemExit(0)

from fakes.astrbot_peer import AstrBotPeer  # noqa: E402
from fakes.fake_arklink import FakeArkLink  # noqa: E402

from arkbridge.bridge import Bridge  # noqa: E402
from arkbridge.config import BridgeConfig  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("relay-e2e")

PASS: list[str] = []
FAIL: list[str] = []
SELF_ID = 10001
ARK_TOKEN = "ark-token"
ASTR_TOKEN = "astr-token"


def check(name: str, cond: bool, detail: str = "") -> bool:
    (PASS if cond else FAIL).append(name)
    logger.info("%s：%s %s", "通过" if cond else "失败", name, detail)
    return cond


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


async def wait_for(pred, timeout: float = 10.0, what: str = ""):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        got = pred()
        if got:
            return got
        await asyncio.sleep(0.05)
    raise AssertionError(f"等待超时：{what}")


async def main() -> int:
    upstream = FakeArkLink(token=ARK_TOKEN, self_id=SELF_ID)
    await upstream.start()

    astr_port = free_port()
    cfg = BridgeConfig(
        name="e2e",
        arklink=upstream.url,
        astrbot=f"ws://127.0.0.1:{astr_port}/ws",
        self_id=0,                       # 交给中继自己从 get_login_info 学
        arklink_token=ARK_TOKEN,
        astrbot_token=ASTR_TOKEN,
        api_timeout=8.0,
        queue=50,
        ping_interval=5.0,
        reconnect_max=3.0,
    ).normalized()
    bridge = Bridge(cfg)
    bridge_task = asyncio.create_task(bridge.run(), name="bridge")

    peer: AstrBotPeer | None = None
    try:
        # ---------- 1. ArkLink 侧连上 & 账号自学 ----------
        try:
            await asyncio.wait_for(upstream.connected.wait(), 10)
            check("能连上 ArkLink（带 token 的握手被接受）", True)
        except asyncio.TimeoutError:
            check("能连上 ArkLink（带 token 的握手被接受）", False, "10s 内没连上")
            return _report()

        try:
            await wait_for(lambda: bridge.self_id, 5, "self_id 自学")
            check("self_id 从 get_login_info 学到（不用手写配置）", bridge.self_id == SELF_ID,
                  f"got {bridge.self_id}")
        except AssertionError as exc:
            check("self_id 从 get_login_info 学到（不用手写配置）", False, str(exc))

        # ---------- 2. AstrBot 还没起来：事件先缓冲 ----------
        await upstream.push_group_message(654321, 1001, "第一条")
        await upstream.push_group_message(654321, 1002, "第二条")
        await asyncio.sleep(0.3)
        check("AstrBot 未连接时事件进缓冲区而不是丢弃",
              bridge.stats["buffered"] == 2, f"buffered={bridge.stats['buffered']}")

        # ---------- 3. 服务端起来后自动补发 ----------
        peer = AstrBotPeer(port=astr_port, token=ASTR_TOKEN)
        await peer.start()
        try:
            await asyncio.wait_for(peer.connected.wait(), 15)
            check("按 X-Self-ID / Universal 被 AstrBot 接受", True)
        except asyncio.TimeoutError:
            check("按 X-Self-ID / Universal 被 AstrBot 接受", False, "15s 内没连上")
            return _report()

        try:
            await wait_for(lambda: len(peer.messages) >= 2, 10, "补发两条缓冲事件")
            texts = [m["message_str"] for m in peer.messages[:2]]
            check("断线期间的事件在连上后按序补发", texts == ["第一条", "第二条"], f"{texts}")
            check("补发事件的 self_id 正确", peer.messages[0]["self_id"] == str(SELF_ID),
                  peer.messages[0]["self_id"])
        except AssertionError as exc:
            check("断线期间的事件在连上后按序补发", False, str(exc))

        # ---------- 4. 群消息 + at 段（AstrBot 会因此查群成员） ----------
        await upstream.push_group_message(654321, 999, "在吗", at_bot=True)
        try:
            abm = (await wait_for(
                lambda: [m for m in peer.messages if m["message_str"] == "在吗"], 8, "群消息"
            ))[0]
            check("群消息到达 AstrBot", abm["type"] == "GROUP")
            check("at 段未被丢弃（get_group_member_info 穿透中继成功）",
                  bool(abm.get("ats")) and abm["ats"][0]["qq"] == str(SELF_ID), f"ats={abm.get('ats')}")
        except AssertionError as exc:
            check("群消息到达 AstrBot", False, str(exc))

        # ---------- 5. 私聊 + 通知 ----------
        await upstream.push_private_message(888, "私聊内容")
        try:
            await wait_for(lambda: [m for m in peer.messages if m["message_str"] == "私聊内容"], 8, "私聊")
            check("私聊消息到达 AstrBot", True)
        except AssertionError as exc:
            check("私聊消息到达 AstrBot", False, str(exc))

        await upstream.push({
            "post_type": "notice", "notice_type": "notify", "sub_type": "poke",
            "group_id": 654321, "user_id": 999, "target_id": SELF_ID,
        })
        try:
            await wait_for(lambda: peer.notices, 8, "notice")
            check("notice（戳一戳）到达 AstrBot", peer.notices[0]["poke_target"] == str(SELF_ID),
                  f"{peer.notices[0]}")
        except AssertionError as exc:
            check("notice（戳一戳）到达 AstrBot", False, str(exc))

        # ---------- 6. AstrBot 发消息 → ArkLink ----------
        res = await peer.send_group(654321, "pong", self_id=SELF_ID)
        # aiocqhttp 在 retcode==0 时只返回 data
        ok = isinstance(res, dict) and res.get("message_id")
        check("AstrBot 发群消息拿到正常应答（echo 配对没串）", bool(ok), f"res={res}")
        call = upstream.find_call("send_group_msg")
        check("ArkLink 侧收到 send_group_msg", call is not None)
        params = (call or {}).get("params") or {}
        segs = params.get("message") or []
        check("消息段数组原样透传", bool(segs) and segs[0]["type"] == "text" and segs[0]["data"]["text"] == "pong",
              f"{segs}")

        res = await peer.send_private(888, "私聊回复")
        check("私聊发送同样拿到正常应答", isinstance(res, dict) and bool(res.get("message_id")), f"{res}")

        # ---------- 7. 不支持的动作：错误码原样回传 ----------
        try:
            await peer.bot.call_action(action="no_such_api")
            check("不支持的 API 会把 retcode 回传给 AstrBot", False, "居然没抛异常")
        except Exception as exc:  # aiocqhttp 抛 Error
            retcode = getattr(exc, "retcode", None) or getattr(exc, "code", None)
            check("不支持的 API 会把 retcode 回传给 AstrBot", retcode == 1404 or "1404" in str(exc),
                  f"{exc!r}")

        # ---------- 8. 慢请求不堵后面 ----------
        upstream.delay_next["send_group_msg"] = 1.5      # 只让第一条慢
        slow = asyncio.create_task(peer.send_group(654321, "slow"))
        await asyncio.sleep(0.2)
        started = time.monotonic()
        fast = await peer.send_group(654321, "fast")
        elapsed = time.monotonic() - started
        await slow
        upstream.delay_next.clear()
        check("并发调用互不阻塞（慢请求不卡后面的）", elapsed < 1.0, f"第二条耗时 {elapsed:.2f}s")
        check("并发两条都成功", isinstance(fast, dict) and bool(fast.get("message_id")), f"{fast}")

        # ---------- 9. 大消息（覆盖 16 位 / 64 位长度编码） ----------
        big = "あ" * 40000            # UTF-8 下 ≈ 120KB，走 64 位长度分支
        await upstream.push_group_message(654321, 777, big)
        try:
            got = await wait_for(
                lambda: [m for m in peer.messages if len(m["message_str"]) > 1000], 15, "大消息"
            )
            check("120KB 文本完整穿过中继（帧长度编码正确）", got[0]["message_str"] == big,
                  f"len={len(got[0]['message_str'])}")
        except AssertionError as exc:
            check("120KB 文本完整穿过中继（帧长度编码正确）", False, str(exc))

        # ---------- 10. 断线重连 ----------
        await upstream.drop()
        await asyncio.sleep(0.5)
        try:
            await wait_for(lambda: upstream.connected.is_set(), 10, "中继自动重连 ArkLink")
            check("ArkLink 断开后自动重连", True)
        except AssertionError as exc:
            check("ArkLink 断开后自动重连", False, str(exc))

        before = len(peer.messages)
        await upstream.push_group_message(654321, 555, "重连之后")
        try:
            await wait_for(lambda: len(peer.messages) > before
                           and peer.messages[-1]["message_str"] == "重连之后", 10, "重连后的事件")
            check("重连后事件继续下行", True)
        except AssertionError as exc:
            check("重连后事件继续下行", False, str(exc))

        check("AstrBot 侧没有任何转换异常", not peer.errors, f"errors={peer.errors}")
        logger.info("中继统计：%s", bridge.describe())
    finally:
        if peer is not None:
            await peer.stop()
        await bridge.stop()
        try:
            await asyncio.wait_for(bridge_task, 10)
        except (asyncio.TimeoutError, asyncio.CancelledError, Exception):
            bridge_task.cancel()
        await upstream.stop()

    return _report()


def _report() -> int:
    print("\n========== ArkBridge 端到端结果 ==========")
    for name in PASS:
        print(f"  [PASS] {name}")
    for name in FAIL:
        print(f"  [FAIL] {name}")
    print(f"共 {len(PASS)} 通过 / {len(FAIL)} 失败")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
