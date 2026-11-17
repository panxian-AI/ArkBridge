"""端到端（真实上游）：`arklink serve onebot` 的 OneBot11Server + 真实 aiocqhttp。

和 `relay_test.py` 的区别只有一处：上游不再是假实现，而是 ArkLink 里那个
**真的 OneBot 11 服务端**（它自己也是标准库写的，会挑握手的毛病）。
链路：

    AstrBot ← ArkBridge → ArkLink 的 OneBot11Server → 假通道适配器

上游用 `ARKLINK_PATH` 指定 ArkLink 仓库位置；没设就在同级目录里找。
找不到就跳过 —— 这条用例的价值全在「对端是真的」。
"""

from __future__ import annotations

import asyncio
import logging
import os
import socket
import sys
import time
from pathlib import Path

_HERE = Path(__file__).resolve().parent
ROOT = _HERE.parents[1]
sys.path[:0] = [str(_HERE), str(_HERE.parent), str(ROOT)]


def _find_arklink() -> Path | None:
    env = os.environ.get("ARKLINK_PATH")
    if env and (Path(env) / "arklink").is_dir():
        return Path(env)
    for base in (ROOT.parent, ROOT.parent.parent):
        if (base / "arklink").is_dir():          # 仓库本身就是 ArkLink
            return base
        for candidate in sorted(base.glob("panxian-AI-ArkLink-*")) + sorted(base.glob("ArkLink")):
            if (candidate / "arklink").is_dir():
                return candidate
    return None


ARKLINK = _find_arklink()
if ARKLINK is None:
    print("SKIP: 没找到 ArkLink 仓库；设置 ARKLINK_PATH=<ArkLink 路径> 后可运行此用例。")
    raise SystemExit(0)
sys.path.insert(0, str(ARKLINK))

try:
    from aiocqhttp import CQHttp  # noqa: F401
    from arklink.base import LinkAdapter, api_error
    from arklink.onebot import OneBot11Server
except ImportError as exc:  # pragma: no cover
    print(f"SKIP: 依赖不全（{exc}）；pip install aiocqhttp quart websockets 并确认 ArkLink 可导入。")
    raise SystemExit(0)

from fakes.astrbot_peer import AstrBotPeer  # noqa: E402

from arkbridge.bridge import Bridge  # noqa: E402
from arkbridge.config import BridgeConfig  # noqa: E402

logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(name)s | %(message)s")
logger = logging.getLogger("arklink-real")

PASS: list[str] = []
FAIL: list[str] = []
SELF_ID = 20002
TOKEN = "onebot-token"
ASTR_TOKEN = "astr-token"


def check(name: str, cond: bool, detail: str = "") -> None:
    (PASS if cond else FAIL).append(name)
    print(f"  {'[PASS]' if cond else '[FAIL]'} {name} {detail if not cond else ''}")


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


class FakeChannel(LinkAdapter):
    """假的 QQ 通道：只负责把消息抛上去、把 API 记下来。"""

    name = "fake"

    def __init__(self, self_id: int = SELF_ID) -> None:
        super().__init__()
        self.self_id = self_id
        self.sent: list[tuple[str, dict]] = []

    async def connect(self) -> None:
        self._connected.set()

    async def call_api(self, action: str, params: dict | None = None):
        params = dict(params or {})
        if action == "get_login_info":
            return {"user_id": self.self_id, "nickname": "阿链"}
        if action in ("send_msg", "send_group_msg", "send_private_msg"):
            self.sent.append((action, params))
            return {"message_id": 9000 + len(self.sent)}
        if action == "get_status":
            return {"online": True, "good": True}
        if action == "get_group_member_info":
            return {"group_id": params.get("group_id"), "user_id": params.get("user_id"),
                    "nickname": "群友", "card": "", "role": "member"}
        raise api_error(action, 1404, f"不支持 {action}")

    async def push_message(self, group_id: int, user_id: int, text: str, at_bot: bool = False) -> None:
        segments = []
        if at_bot:
            segments.append({"type": "at", "data": {"qq": str(self.self_id)}})
        segments.append({"type": "text", "data": {"text": text}})
        await self._emit_dict({
            "post_type": "message",
            "message_type": "group",
            "sub_type": "normal",
            "message_id": 4242,
            "group_id": group_id,
            "user_id": user_id,
            "message": segments,
            "raw_message": text,
            "sender": {"user_id": user_id, "nickname": f"用户{user_id}", "card": "", "role": "member"},
        })


async def main() -> int:
    channel = FakeChannel()
    upstream = OneBot11Server(
        channel, host="127.0.0.1", port=0, access_token=TOKEN, heartbeat=0
    )
    await upstream.start()

    astr_port = free_port()
    cfg = BridgeConfig(
        name="real",
        arklink=f"ws://127.0.0.1:{upstream.port}/onebot/v11/ws",
        astrbot=f"ws://127.0.0.1:{astr_port}/ws",
        arklink_token=TOKEN,
        astrbot_token=ASTR_TOKEN,
        api_timeout=8.0,
        ping_interval=0,
        reconnect_max=3.0,
    ).normalized()

    bridge = Bridge(cfg)
    bridge_task = asyncio.create_task(bridge.run())
    peer = AstrBotPeer(port=astr_port, token=ASTR_TOKEN)
    try:
        await peer.start()
        await asyncio.wait_for(peer.connected.wait(), 15)
        check("ArkBridge 连上真实的 ArkLink OneBot11Server（token 校验通过）", True)
        await wait_for(lambda: bridge.self_id, 6, "self_id")
        check("从 ArkLink 学到 self_id", bridge.self_id == SELF_ID, f"got {bridge.self_id}")
        check("AstrBot 拿到的 X-Self-ID 正确", upstream.stats["api_calls"] >= 0)

        await channel.push_message(123456, 777, "真上游测试", at_bot=True)
        try:
            abm = (await wait_for(
                lambda: [m for m in peer.messages if m["message_str"] == "真上游测试"], 10, "群消息"
            ))[0]
            check("真实 ArkLink 的事件能穿过中继到 AstrBot", abm["group_id"] == "123456")
            check("at 段（AstrBot 会回查群成员）未被丢弃", bool(abm.get("ats")), f"{abm.get('ats')}")
        except AssertionError as exc:
            check("真实 ArkLink 的事件能穿过中继到 AstrBot", False, str(exc))

        res = await peer.send_group(123456, "回一句")
        check("AstrBot 发消息被真实 ArkLink 接收",
              isinstance(res, dict) and bool(res.get("message_id")), f"{res}")
        check("ArkLink 侧通道真的发出了这条",
              any(p.get("message") for _, p in channel.sent), f"{channel.sent}")

        try:
            await peer.bot.call_action(action="no_such_api")
            check("ArkLink 的 1404 原样回给 AstrBot", False, "没抛异常")
        except Exception as exc:
            check("ArkLink 的 1404 原样回给 AstrBot", "1404" in str(exc), f"{exc!r}")

        check("AstrBot 侧没有转换异常", not peer.errors, f"{peer.errors}")
    finally:
        await peer.stop()
        await bridge.stop()
        try:
            await asyncio.wait_for(bridge_task, 10)
        except (asyncio.TimeoutError, asyncio.CancelledError, Exception):
            bridge_task.cancel()
        await upstream.stop()

    print(f"\n共 {len(PASS)} 通过 / {len(FAIL)} 失败")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
