"""假 ArkLink：一个真的 OneBot 11 正向 WS 服务端（服务端用 websockets 库实现）。

它只扮演「实现」那一头该做的事：连接后推事件、按 echo 应答 API。
这样中继的每一行代码都在真实协议下跑，而不是对着 mock 自说自话。
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any

from websockets.asyncio.server import serve

logger = logging.getLogger("fake-arklink")


class FakeArkLink:
    def __init__(self, port: int = 0, token: str = "", self_id: int = 10001) -> None:
        self.port = port
        self.token = token
        self.self_id = self_id
        self.calls: list[dict[str, Any]] = []
        self.messages: dict[int, dict[str, Any]] = {}
        self.connected = asyncio.Event()
        self.client_self_ids: list[str] = []
        #: 让「下一条」某个 action 慢下来（只生效一次，否则并发测试测的是它自己）
        self.delay_next: dict[str, float] = {}
        self.refuse = False
        self._server: Any = None
        self._ws: Any = None
        self._next_id = 3000

    # ------------------------------------------------------------------ 生命周期
    async def start(self) -> "FakeArkLink":
        self._server = await serve(self._handle, "127.0.0.1", self.port, compression=None)
        self.port = self._server.sockets[0].getsockname()[1]
        return self

    async def stop(self) -> None:
        await self.drop()
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None

    @property
    def url(self) -> str:
        return f"ws://127.0.0.1:{self.port}/onebot/v11/ws"

    # ------------------------------------------------------------------ 连接
    async def _handle(self, ws: Any) -> None:
        headers = ws.request.headers
        auth = headers.get("Authorization", "") or ""
        if self.token and auth != f"Bearer {self.token}":
            logger.warning("token 不对：%r", auth)
            await ws.close(1008, "unauthorized")
            return
        self.client_self_ids.append(headers.get("X-Self-ID", ""))

        if self.refuse:
            await ws.close(1011, "refusing for test")
            return

        self._ws = ws
        self.connected.set()
        await ws.send(json.dumps(self._meta("lifecycle", "connect"), ensure_ascii=False))
        await ws.send(json.dumps(self._meta("heartbeat", "heartbeat"), ensure_ascii=False))
        try:
            async for raw in ws:
                data = json.loads(raw)
                if not isinstance(data, dict) or "action" not in data:
                    continue
                self.calls.append(data)
                # 每条请求独立处理：中继的并发能力才测得出来
                asyncio.create_task(self._answer(ws, data))
        except Exception:
            pass
        finally:
            if self._ws is ws:
                self._ws = None
                self.connected.clear()

    async def drop(self) -> None:
        """主动断链，用来测重连。"""
        ws, self._ws = self._ws, None
        self.connected.clear()
        if ws is not None:
            await ws.close(1011, "test drop")

    # ------------------------------------------------------------------ 事件
    def _meta(self, meta_type: str, sub_type: str, extra: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = {
            "time": int(time.time()),
            "self_id": self.self_id,
            "post_type": "meta_event",
            "meta_event_type": meta_type,
            "sub_type": sub_type,
        }
        payload.update(extra or {})
        return payload

    async def push(self, event: dict[str, Any]) -> None:
        ws = self._ws
        if ws is None:
            raise RuntimeError("还没有客户端连上来")
        payload = {"time": int(time.time()), "self_id": self.self_id, **event}
        await ws.send(json.dumps(payload, ensure_ascii=False))

    async def push_group_message(
        self,
        group_id: int,
        user_id: int,
        text: str,
        *,
        at_bot: bool = False,
        message_id: int | None = None,
    ) -> int:
        mid = message_id or self._next_message_id()
        segments: list[dict[str, Any]] = []
        if at_bot:
            segments.append({"type": "at", "data": {"qq": str(self.self_id)}})
        segments.append({"type": "text", "data": {"text": text}})
        event = {
            "post_type": "message",
            "message_type": "group",
            "sub_type": "normal",
            "message_id": mid,
            "group_id": group_id,
            "user_id": user_id,
            "message": segments,
            "raw_message": text,
            "font": 0,
            "sender": {"user_id": user_id, "nickname": f"用户{user_id}", "card": "", "role": "member"},
        }
        self.messages[mid] = event
        await self.push(event)
        return mid

    async def push_private_message(self, user_id: int, text: str) -> int:
        mid = self._next_message_id()
        event = {
            "post_type": "message",
            "message_type": "private",
            "sub_type": "friend",
            "message_id": mid,
            "user_id": user_id,
            "message": [{"type": "text", "data": {"text": text}}],
            "raw_message": text,
            "sender": {"user_id": user_id, "nickname": f"用户{user_id}"},
        }
        self.messages[mid] = event
        await self.push(event)
        return mid

    def _next_message_id(self) -> int:
        self._next_id += 1
        return self._next_id

    # ------------------------------------------------------------------ API
    async def _answer(self, ws: Any, data: dict[str, Any]) -> None:
        action = str(data.get("action") or "")
        params = data.get("params") or {}
        delay = self.delay_next.pop(action, None)
        if delay:
            await asyncio.sleep(delay)
        resp = self._dispatch(action, params)
        resp["echo"] = data.get("echo")
        try:
            await ws.send(json.dumps(resp, ensure_ascii=False))
        except Exception:
            pass

    def _dispatch(self, action: str, params: dict[str, Any]) -> dict[str, Any]:
        def ok(data: Any) -> dict[str, Any]:
            return {"status": "ok", "retcode": 0, "data": data}

        def fail(code: int, msg: str) -> dict[str, Any]:
            return {"status": "failed", "retcode": code, "data": None, "msg": msg}

        if action == "get_login_info":
            return ok({"user_id": self.self_id, "nickname": "阿桥"})
        if action == "get_status":
            return ok({"online": True, "good": True})
        if action in ("send_msg", "send_group_msg", "send_private_msg"):
            mid = self._next_message_id()
            return ok({"message_id": mid})
        if action == "get_msg":
            mid = int(params.get("message_id") or 0)
            event = self.messages.get(mid)
            return ok(event) if event else fail(1404, "消息不存在")
        if action == "get_group_member_info":
            return ok({
                "group_id": params.get("group_id"),
                "user_id": params.get("user_id"),
                "nickname": "群友",
                "card": "",
                "role": "member",
            })
        if action == "get_stranger_info":
            return ok({"user_id": params.get("user_id"), "nickname": "陌生人", "sex": "unknown", "age": 0})
        if action == "delete_msg":
            return ok(None)
        if action == "get_group_list":
            return ok([{"group_id": 654321, "group_name": "测试群"}])
        return fail(1404, f"不支持的 API：{action}")

    # ------------------------------------------------------------------ 断言辅助
    def find_call(self, action: str) -> dict[str, Any] | None:
        for call in reversed(self.calls):
            if call.get("action") == action:
                return call
        return None

    def count_calls(self, action: str) -> int:
        return sum(1 for c in self.calls if c.get("action") == action)
