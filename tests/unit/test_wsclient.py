"""WebSocket 客户端协议细节：掩码、三档长度、分片、ping/pong、close、握手拒绝。

对端是手写的极简服务端，才能精细控制每一帧（真实库不会按需分片）。
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import struct
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from arkbridge.wsclient import HandshakeError, WSClient  # noqa: E402

_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


class RawPeer:
    """够用的 WS 服务端：只握手 + 收发帧，剧情由 `script` 决定。"""

    def __init__(self, script, status: int = 101) -> None:
        self.script = script
        self.status = status
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None
        self.path = ""
        self.headers: dict[str, str] = {}
        self._server: asyncio.AbstractServer | None = None
        self.port = 0

    async def __aenter__(self) -> "RawPeer":
        self._server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self._server.sockets[0].getsockname()[1]
        return self

    async def __aexit__(self, *exc) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        head = await reader.readuntil(b"\r\n\r\n")
        lines = head.decode("latin-1").split("\r\n")
        self.path = lines[0].split(" ")[1]
        for line in lines[1:]:
            if ":" in line:
                k, v = line.split(":", 1)
                self.headers[k.strip().lower()] = v.strip()

        if self.status != 101:
            writer.write(f"HTTP/1.1 {self.status} Nope\r\nContent-Length: 0\r\n\r\n".encode())
            await writer.drain()
            writer.close()
            return

        accept = base64.b64encode(
            hashlib.sha1((self.headers["sec-websocket-key"] + _GUID).encode()).digest()
        ).decode()
        writer.write(
            (
                "HTTP/1.1 101 Switching Protocols\r\n"
                "Upgrade: websocket\r\nConnection: Upgrade\r\n"
                f"Sec-WebSocket-Accept: {accept}\r\n\r\n"
            ).encode()
        )
        await writer.drain()
        self.reader, self.writer = reader, writer
        try:
            await self.script(self)
        except Exception:
            pass
        finally:
            try:
                writer.close()
            except Exception:
                pass


async def _read_frame(reader: asyncio.StreamReader) -> tuple[int, bytes, bool]:
    b1, b2 = await reader.readexactly(2)
    fin = bool(b1 & 0x80)
    opcode = b1 & 0x0F
    masked = bool(b2 & 0x80)
    length = b2 & 0x7F
    if length == 126:
        length = struct.unpack("!H", await reader.readexactly(2))[0]
    elif length == 127:
        length = struct.unpack("!Q", await reader.readexactly(8))[0]
    mask = await reader.readexactly(4) if masked else b""
    payload = await reader.readexactly(length) if length else b""
    if masked and payload:
        payload = bytes(b ^ mask[i & 3] for i, b in enumerate(payload))
    assert fin, "客户端不该发包内分片"
    return opcode, payload, masked


async def _send_frame(peer: RawPeer, opcode: int, payload: bytes, fin: bool = True) -> None:
    n = len(payload)
    b0 = (0x80 if fin else 0) | opcode
    if n < 126:
        header = struct.pack("!BB", b0, n)
    elif n < 1 << 16:
        header = struct.pack("!BBH", b0, 126, n)
    else:
        header = struct.pack("!BBQ", b0, 127, n)
    assert peer.writer is not None
    peer.writer.write(header + payload)
    await peer.writer.drain()


def test_handshake_sends_expected_headers():
    async def run():
        done = asyncio.Event()

        async def script(peer: RawPeer) -> None:
            done.set()
            await asyncio.sleep(0.05)

        async with RawPeer(script) as peer:
            client = await WSClient.connect(
                f"ws://127.0.0.1:{peer.port}/ws?x=1",
                headers={"X-Self-ID": "10001", "X-Client-Role": "Universal"},
                authorization="tok",
                ping_interval=0,
            )
            await asyncio.wait_for(done.wait(), 5)
            assert peer.path == "/ws?x=1"
            assert peer.headers["x-self-id"] == "10001"
            assert peer.headers["x-client-role"] == "Universal"
            assert peer.headers["authorization"] == "Bearer tok"
            assert peer.headers["upgrade"].lower() == "websocket"
            assert peer.headers["sec-websocket-version"] == "13"
            await client.close()

    asyncio.run(run())


def test_send_frame_length_branches_and_masking():
    async def run():
        got: list[bytes] = []
        done = asyncio.Event()

        async def script(peer: RawPeer) -> None:
            for _ in range(3):
                opcode, payload, masked = await _read_frame(peer.reader)  # type: ignore[arg-type]
                assert opcode == 0x1
                assert masked, "客户端帧必须掩码"
                got.append(payload)
            done.set()
            await asyncio.sleep(0.05)

        async with RawPeer(script) as peer:
            client = await WSClient.connect(f"ws://127.0.0.1:{peer.port}/", ping_interval=0)
            payloads = ["a" * 10, "b" * 200, "c" * 70000]      # 7 位 / 16 位 / 64 位长度
            for text in payloads:
                await client.send_text(text)
            await asyncio.wait_for(done.wait(), 5)
            assert [len(p) for p in got] == [10, 200, 70000]
            assert [p.decode() for p in got] == payloads
            await client.close()

    asyncio.run(run())


def test_receive_fragmented_message():
    async def run():
        async def script(peer: RawPeer) -> None:
            await _send_frame(peer, 0x1, "he".encode(), fin=False)
            await _send_frame(peer, 0x0, "ll".encode(), fin=False)
            await _send_frame(peer, 0x0, "o".encode(), fin=True)
            await asyncio.sleep(0.2)

        async with RawPeer(script) as peer:
            client = await WSClient.connect(f"ws://127.0.0.1:{peer.port}/", ping_interval=0)
            assert await asyncio.wait_for(client.recv(), 5) == "hello"
            await client.close()

    asyncio.run(run())


def test_ping_gets_pong():
    async def run():
        pong = asyncio.Event()
        got: list[bytes] = []

        async def script(peer: RawPeer) -> None:
            await _send_frame(peer, 0x9, b"hb")          # ping
            opcode, payload, _ = await _read_frame(peer.reader)  # type: ignore[arg-type]
            got.append(payload)
            assert opcode == 0xA, f"期望 pong，收到 {opcode}"
            await _send_frame(peer, 0x1, b"after-ping")
            pong.set()
            await asyncio.sleep(0.05)

        async with RawPeer(script) as peer:
            client = await WSClient.connect(f"ws://127.0.0.1:{peer.port}/", ping_interval=0)
            assert await asyncio.wait_for(client.recv(), 5) == "after-ping"
            await asyncio.wait_for(pong.wait(), 5)
            assert got == [b"hb"]
            await client.close()

    asyncio.run(run())


def test_close_ends_recv():
    async def run():
        async def script(peer: RawPeer) -> None:
            await _send_frame(peer, 0x8, struct.pack("!H", 1000))
            await asyncio.sleep(0.2)

        async with RawPeer(script) as peer:
            client = await WSClient.connect(f"ws://127.0.0.1:{peer.port}/", ping_interval=0)
            assert await asyncio.wait_for(client.recv(), 5) is None
            assert client.closed

    asyncio.run(run())


def test_large_incoming_message():
    async def run():
        big = "x" * 80000

        async def script(peer: RawPeer) -> None:
            await _send_frame(peer, 0x1, big.encode())
            await asyncio.sleep(0.2)

        async with RawPeer(script) as peer:
            client = await WSClient.connect(f"ws://127.0.0.1:{peer.port}/", ping_interval=0)
            assert await asyncio.wait_for(client.recv(), 5) == big
            await client.close()

    asyncio.run(run())


@pytest.mark.parametrize("status", [401, 403, 404])
def test_handshake_rejected(status: int):
    async def run():
        async def script(peer: RawPeer) -> None:  # pragma: no cover - 不会走到
            return

        async with RawPeer(script, status=status) as peer:
            with pytest.raises(HandshakeError) as exc:
                await WSClient.connect(f"ws://127.0.0.1:{peer.port}/ws", open_timeout=3)
            assert exc.value.status == status

    asyncio.run(run())


def test_connect_refused_is_handshake_error():
    async def run():
        with pytest.raises(HandshakeError):
            await WSClient.connect("ws://127.0.0.1:1/ws", open_timeout=2)

    asyncio.run(run())
