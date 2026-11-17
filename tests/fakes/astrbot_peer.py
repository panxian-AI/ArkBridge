"""假 AstrBot：用真实的 `aiocqhttp.CQHttp(use_ws_reverse=True)` 当对端。

AstrBot 的 `astrbot/core/platform/sources/aiocqhttp/` 就是在这个库上做的，
所以握手头校验、`echo` 应答配对、`X-Self-ID` 路由、call_action 注入参数
全部由库自己实现 —— 中继只要错一点就会当场暴露，而不是被 mock 放过。

同时按 AstrBot 真实代码路径做转换：`sender["user_id"]` 直接下标、
`message` 必须是数组、at 段要查群成员、reply 段要 get_msg 回查。
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from aiocqhttp import CQHttp, Event

logger = logging.getLogger("fake-astrbot")


class AstrBotPeer:
    def __init__(self, port: int, token: str = "") -> None:
        self.port = port
        self.token = token
        self.bot = CQHttp(
            use_ws_reverse=True,
            import_name="arkbridge-fake-astrbot",
            api_timeout_sec=15,
            access_token=token or None,
        )
        self.connected = asyncio.Event()
        self.messages: list[dict[str, Any]] = []
        self.notices: list[dict[str, Any]] = []
        self.errors: list[str] = []
        self._task: asyncio.Task[Any] | None = None
        self._stop: asyncio.Event | None = None
        self._register()

    # ---------------------------------------------------------------- 事件处理
    def _register(self) -> None:
        bot = self.bot

        @bot.on_websocket_connection
        def _on_conn(_ws: Any) -> None:
            self.connected.set()

        @bot.on_message("group")
        async def _group(event: Event) -> None:
            try:
                self.messages.append(await self._convert(event))
            except Exception as exc:  # AstrBot 的做法：记日志后丢弃整条
                self.errors.append(f"group: {exc!r}")
                logger.error("AstrBot 侧转换群消息失败：%r", exc)

        @bot.on_message("private")
        async def _private(event: Event) -> None:
            try:
                self.messages.append(await self._convert(event))
            except Exception as exc:
                self.errors.append(f"private: {exc!r}")
                logger.error("AstrBot 侧转换私聊失败：%r", exc)

        @bot.on_notice()
        async def _notice(event: Event) -> None:
            self.notices.append(
                {
                    "self_id": str(event.self_id),
                    "notice_type": event.get("notice_type"),
                    "user_id": str(event.get("user_id")),
                    "group_id": str(event.get("group_id") or ""),
                    "poke_target": (
                        str(event["target_id"])
                        if event.get("sub_type") == "poke" and event.get("target_id")
                        else None
                    ),
                }
            )

    async def _convert(self, event: Event) -> dict[str, Any]:
        assert event.sender is not None, "sender 缺失"
        assert isinstance(event.message, list), "message 不是数组"
        abm: dict[str, Any] = {
            "self_id": str(event.self_id),
            "sender": {
                "user_id": str(event.sender["user_id"]),
                "nickname": event.sender.get("card") or event.sender.get("nickname", "N/A"),
            },
            "message_id": str(event.message_id),
            "segments": [],
            "message_str": "",
            "group_id": str(event.get("group_id") or ""),
            "type": "GROUP" if event["message_type"] == "group" else "FRIEND",
        }
        routing = {"self_id": event.self_id} if event.self_id else {}

        for seg in event.message:
            kind, data = seg["type"], seg["data"]
            abm["segments"].append(dict(seg))
            if kind == "text":
                abm["message_str"] += (data.get("text") or "").strip()
            elif kind == "at":
                if data["qq"] == "all":
                    continue
                # AstrBot 真的会为每个 at 段查群成员；查不到就丢掉这个 at
                info = await self.bot.call_action(
                    action="get_group_member_info",
                    group_id=event.get("group_id"),
                    user_id=int(data["qq"]),
                    no_cache=False,
                    **routing,
                )
                abm.setdefault("ats", []).append({"qq": data["qq"], "name": (info or {}).get("card", "")})
            elif kind == "reply":
                detail = await self.bot.call_action(
                    action="get_msg", message_id=int(data["id"]), **routing
                )
                detail["post_type"] = "message"
                quoted = Event.from_payload(detail)
                assert quoted is not None, "get_msg 数据无法构造 Event"
                abm["quote"] = {
                    "message_id": str(quoted.message_id),
                    "user_id": str(quoted.sender["user_id"]),
                    "message": quoted.message,
                }
        return abm

    # ---------------------------------------------------------------- 生命周期
    async def start(self) -> None:
        self._stop = asyncio.Event()

        async def _trigger() -> None:
            assert self._stop is not None
            await self._stop.wait()

        self._task = asyncio.create_task(
            self.bot.run_task(host="127.0.0.1", port=self.port, shutdown_trigger=_trigger)
        )
        for _ in range(120):
            try:
                reader, writer = await asyncio.open_connection("127.0.0.1", self.port)
                writer.close()
                return
            except OSError:
                await asyncio.sleep(0.1)
        raise RuntimeError("AstrBot 侧端口没起来")

    async def stop(self) -> None:
        if self._stop is not None:
            self._stop.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass

    # ---------------------------------------------------------------- 调用辅助
    async def send_group(self, group_id: int, text: str, self_id: int | None = None) -> Any:
        kwargs: dict[str, Any] = {"group_id": group_id, "message": [{"type": "text", "data": {"text": text}}]}
        if self_id:
            kwargs["self_id"] = self_id
        return await self.bot.send_group_msg(**kwargs)

    async def send_private(self, user_id: int, text: str) -> Any:
        return await self.bot.send_private_msg(
            user_id=user_id, message=[{"type": "text", "data": {"text": text}}]
        )
