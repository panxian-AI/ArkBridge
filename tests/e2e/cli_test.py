"""端到端（命令行）：验证文档里那一行命令真的能跑起来。

`python -m arkbridge run --arklink ... --astrbot ...` —— 不写配置文件的最小接入路径。
子进程启动中继，两端都是真实协议栈（真 aiocqhttp + 真 WS 服务端）。
"""

from __future__ import annotations

import asyncio
import os
import socket
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
ROOT = _HERE.parents[1]
sys.path[:0] = [str(_HERE), str(_HERE.parent), str(ROOT)]

try:
    from aiocqhttp import CQHttp  # noqa: F401
except ImportError:  # pragma: no cover
    print("SKIP: 未安装 aiocqhttp")
    raise SystemExit(0)

from fakes.astrbot_peer import AstrBotPeer  # noqa: E402
from fakes.fake_arklink import FakeArkLink  # noqa: E402

PASS: list[str] = []
FAIL: list[str] = []
SELF_ID = 30003


def check(name: str, cond: bool, detail: str = "") -> None:
    (PASS if cond else FAIL).append(name)
    print(f"  {'[PASS]' if cond else '[FAIL]'} {name} {'' if cond else detail}")


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


async def wait_for(pred, timeout: float = 20.0, what: str = ""):
    import time

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        got = pred()
        if got:
            return got
        await asyncio.sleep(0.05)
    raise AssertionError(f"等待超时：{what}")


async def main() -> int:
    upstream = FakeArkLink(token="ark-token", self_id=SELF_ID)
    await upstream.start()
    peer = AstrBotPeer(port=free_port(), token="astr-token")
    await peer.start()

    env = dict(os.environ)
    env["PYTHONPATH"] = str(ROOT) + (os.pathsep + env["PYTHONPATH"] if env.get("PYTHONPATH") else "")
    env["PYTHONUNBUFFERED"] = "1"

    proc = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "arkbridge", "run",
        "--arklink", upstream.url,
        "--astrbot", f"ws://127.0.0.1:{peer.port}/ws",
        "--arklink-token", "ark-token",
        "--astrbot-token", "astr-token",
        cwd=str(ROOT),
        env=env,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    logs: list[str] = []

    async def pump() -> None:
        assert proc.stdout is not None
        while True:
            line = await proc.stdout.readline()
            if not line:
                return
            logs.append(line.decode("utf-8", "replace").rstrip())

    pump_task = asyncio.create_task(pump())
    try:
        try:
            await asyncio.wait_for(peer.connected.wait(), 25)
            check("命令行一行起得来：中继连上了 AstrBot", True)
        except asyncio.TimeoutError:
            check("命令行一行起得来：中继连上了 AstrBot", False, "25s 内没连上")
            print("\n".join(logs[-20:]))
            return _report()

        check("子进程打印了启动横幅", any("ArkBridge" in line for line in logs), f"{logs[:3]}")

        await upstream.push_group_message(111222, 555, "命令行测试")
        try:
            abm = (await wait_for(lambda: [m for m in peer.messages if m["message_str"] == "命令行测试"], 15))[0]
            check("事件经子进程中继到达 AstrBot", abm["self_id"] == str(SELF_ID), f"{abm}")
        except AssertionError as exc:
            check("事件经子进程中继到达 AstrBot", False, str(exc))

        res = await peer.send_group(111222, "回一句")
        check("AstrBot 发消息经子进程落到 ArkLink",
              isinstance(res, dict) and bool(res.get("message_id")), f"{res}")
    finally:
        if proc.returncode is None:
            proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), 10)
            except asyncio.TimeoutError:
                proc.kill()
        pump_task.cancel()
        await peer.stop()
        await upstream.stop()

    print(f"\n共 {len(PASS)} 通过 / {len(FAIL)} 失败")
    return 1 if FAIL else 0


def _report() -> int:
    print(f"\n共 {len(PASS)} 通过 / {len(FAIL)} 失败")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
