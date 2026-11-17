"""配置解析：默认值、简写、环境变量覆盖、错误提示。"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from arkbridge.config import (  # noqa: E402
    DEFAULT_CONFIG_TEXT,
    BridgeConfig,
    ConfigError,
    load_config,
    normalize_url,
)


def test_url_shortcuts():
    assert normalize_url("127.0.0.1:6199", "astrbot") == "ws://127.0.0.1:6199/ws"
    assert normalize_url("astrbot://127.0.0.1:6199", "astrbot") == "ws://127.0.0.1:6199/ws"
    assert normalize_url("arklink://127.0.0.1:6700", "arklink") == "ws://127.0.0.1:6700/onebot/v11/ws"
    assert normalize_url("http://h:6700/onebot/v11/ws", "arklink") == "ws://h:6700/onebot/v11/ws"
    assert normalize_url("wss://h/ws", "astrbot") == "wss://h/ws"


def test_url_invalid():
    with pytest.raises(ConfigError):
        normalize_url("", "arklink")
    with pytest.raises(ConfigError):
        normalize_url("ftp://h/x", "arklink")


def test_default_template_is_valid(tmp_path: Path):
    path = tmp_path / "bridge.toml"
    path.write_text(DEFAULT_CONFIG_TEXT, encoding="utf-8")
    cfg = load_config(path)
    assert len(cfg.bridges) == 1
    first = cfg.bridges[0]
    assert first.arklink.endswith("/onebot/v11/ws")
    assert first.astrbot.endswith("/ws")
    assert first.self_id == 0
    assert cfg.log_level == "INFO"


def test_multi_bridge_and_types(tmp_path: Path):
    path = tmp_path / "bridge.toml"
    path.write_text(
        """
[log]
level = "debug"

[[bridge]]
name = "g1"
arklink = "127.0.0.1:6700"
astrbot = "127.0.0.1:6199"
self_id = 10001
api_timeout = 12.5
queue = 0

[[bridge]]
name = "g2"
arklink = "arklink://127.0.0.1:6701"
astrbot = "astrbot://127.0.0.1:6200"
""",
        encoding="utf-8",
    )
    cfg = load_config(path)
    assert [b.name for b in cfg.bridges] == ["g1", "g2"]
    assert cfg.bridges[0].self_id == 10001 and cfg.bridges[0].api_timeout == 12.5
    assert cfg.bridges[1].arklink == "ws://127.0.0.1:6701/onebot/v11/ws"
    assert cfg.bridges[1].astrbot == "ws://127.0.0.1:6200/ws"
    assert cfg.log_level == "DEBUG"


def test_env_override(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("ARKBRIDGE_SELF_ID", "30003")
    monkeypatch.setenv("ARKBRIDGE_ASTRBOT", "127.0.0.1:7000")
    cfg = load_config(None)
    assert cfg.bridges[0].self_id == 30003
    assert cfg.bridges[0].astrbot == "ws://127.0.0.1:7000/ws"


def test_unknown_key_is_loud(tmp_path: Path):
    path = tmp_path / "bridge.toml"
    path.write_text("[[bridge]]\narklink = 'x:1'\nastrobot = 'y:2'\n", encoding="utf-8")
    with pytest.raises(ConfigError) as exc:
        load_config(path)
    assert "astrobot" in str(exc.value)


def test_duplicate_names(tmp_path: Path):
    path = tmp_path / "bridge.toml"
    path.write_text(
        "[[bridge]]\nname='a'\n[[bridge]]\nname='a'\n", encoding="utf-8"
    )
    with pytest.raises(ConfigError):
        load_config(path)


def test_bad_timeout():
    with pytest.raises(ConfigError):
        BridgeConfig(api_timeout=0).validate()
