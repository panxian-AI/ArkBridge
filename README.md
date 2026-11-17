# ArkBridge

把 **ArkLink** 的 OneBot 11 服务端，中继给 **AstrBot**（或任何只会当反向 WS 服务端的框架）。

零第三方依赖 —— 一份 Python 标准库代码，`python -m arkbridge run` 就跑。

```
        ┌──────────────────────────────┐
        │  AstrBot                     │
        │  插件 / LLM / 人格 / 工具调用  │
        │  aiocqhttp 平台，只当服务端     │
        └──────────────▲───────────────┘
                       │ 反向 WS：ArkBridge 主动连 /ws
                       │ X-Self-ID · X-Client-Role: Universal
        ┌──────────────┴───────────────┐
        │  ArkBridge                   │   ← 本项目：两头都主动连，纯搬运
        └──────────────▲───────────────┘
                       │ OneBot 11 正向 WS：ArkBridge 当客户端
        ┌──────────────┴───────────────┐
        │  ArkLink  `serve onebot`     │
        │  正向 WS + HTTP API 同端口    │
        └──────────────▲───────────────┘
                       │
                  QQ / NTQQ 通道
```

## 为什么需要它

这事的根源是一个**方向问题**：

- AstrBot 的 `aiocqhttp` 适配器只有一种模式：`CQHttp(use_ws_reverse=True)`，
  也就是 **AstrBot 自己当 WebSocket 服务端**，等「实现」连上来。
- ArkLink 的 `serve onebot` 也是**服务端**，等上层框架连上来。

两个都等着对方连自己，所以 **AstrBot 直连 ArkLink 是握不上手的**——
这不是配置问题，改 address 改一万遍也没用。

ArkBridge 就是中间那个肯主动连出去的角色：

- 以 OneBot 11 **客户端**身份连 ArkLink：收事件、发 API；
- 以 OneBot 11 **实现**身份连 AstrBot：推事件、应答 API。

## 60 秒接上

```bash
# 1) 起 ArkLink 的 OneBot 服务（另开一个窗口）
python -m arklink serve onebot --type ntqq-hook --port 6700

# 2) AstrBot WebUI → 配置 → 消息平台 → 新增 aiocqhttp
#    反向 WS 主机 127.0.0.1   端口 6199   路径 /ws   token 留空

# 3) 起中继（默认就是 6700 → 6199，不用写配置文件）
cd ArkBridge
python -m arkbridge run
```

看到这两行就算成了：

```
ArkLink 已连接：ws://127.0.0.1:6700/onebot/v11/ws
AstrBot 已连接：ws://127.0.0.1:6199/ws（self_id=10001）
```

地址不一样就加参数，或者跑一次向导生成 `bridge.toml`：

```bash
python -m arkbridge run --arklink 10.0.0.2:6700 --astrbot 10.0.0.2:6199
python -m arkbridge setup          # 交互式问答，顺手打印 AstrBot 侧该填什么
python -m arkbridge check          # 只自检，不启动：分别探两头，哪头不通说得明明白白
```

> `self_id`（机器人 QQ）不填也行：ArkBridge 连上 ArkLink 后会问 `get_login_info` 自己学。
> AstrBot 靠它判断「@ 的是不是我」，所以学不到时日志会提醒你，直接在配置里写死即可。

## 和 ArkBot 里那套兼容层有什么区别

ArkBot 的 `compat/` 做的是同一件事，但它绑在 ArkBot 的插件体系里（有事件分发、离线发件箱、通道降级）。
ArkBridge 是**纯中继**：不跑插件、不管通道、不做重试发件箱，只把字节搬过去。

| 你的场景 | 用哪个 |
|---|---|
| 只要 AstrBot，QQ 通道用 ArkLink 顶着 | **ArkBridge**（本文） |
| 想同时用 ArkBot 的插件 + AstrBot | ArkBot 的 `[compat]`（见 [ArkBot: AstrBot 接入](https://github.com/panxian-AI/ArkBot/blob/main/docs/guides/AstrBot接入.md)） |
| 想让中继跑在手机上（省一台机器 / 随身带） | [ArkDroid](https://github.com/panxian-AI/ArkDroid)：APK 里内嵌的就是本项目的 `arkbridge` |
| 用 NoneBot2 / Koishi | 不用本项目，它们能直连 ArkLink 的正向 WS |

三条路的共同点：**AstrBot 侧一行代码都不用改**。

## 它到底做了什么

| 环节 | 行为 |
|---|---|
| 事件下行 | ArkLink → 补 `time` / `self_id` → 原样推给 AstrBot（消息、通知、元事件都过） |
| API 上行 | AstrBot 的 `action` + `params` → 换成本地 `echo` 发往 ArkLink → 按 `echo` 收回 → **还原 AstrBot 原来的 `echo`** 再回给它 |
| 并发 | 每条 API 单独一个任务，一条慢请求不会卡住后面（`api_timeout` 只管它自己） |
| 账号 | 优先用配置的 `self_id`，否则连上 ArkLink 后 `get_login_info` 自学；学到新号会带着新 `X-Self-ID` 重连 AstrBot |
| 断线 | 两端各自指数退避重连（上限 `reconnect_max`），互不牵连 |
| AstrBot 晚启动 | 期间的消息事件先进环形缓冲（`queue` 条），连上后按序补发；元事件不占缓冲 |
| 僵死连接 | 定时 ping，超过 `ping_timeout` 一个字节都没回来就主动断开重连（半开连接是最坑人的那种） |
| 错误透传 | ArkLink 的 `retcode` / `msg` 原样回给 AstrBot —— 它自己会按标准降级，不用中继替它编故事 |

## 配置参考

`bridge.toml`（放当前目录即自动读取，也可以用 `-c` 指定）：

```toml
[[bridge]]
name = "main"                                        # 多账号就写多段 [[bridge]]
arklink = "ws://127.0.0.1:6700/onebot/v11/ws"        # 简写：127.0.0.1:6700 或 arklink://127.0.0.1:6700
astrbot = "ws://127.0.0.1:6199/ws"                   # 简写：127.0.0.1:6199 或 astrbot://127.0.0.1:6199
# self_id = 10001
# arklink_token = ""      # ArkLink 的 access_token
# astrbot_token = ""      # AstrBot 的 ws_reverse_token，两边必须一致
# api_timeout = 60        # 单条 API 超时（秒）
# queue = 200             # AstrBot 断开期间缓存多少条消息事件
# ping_interval = 20      # 心跳间隔（秒），0 = 关
# ping_timeout = 60       # 多久没有任何数据判定僵死
# reconnect_max = 60      # 重连退避上限（秒）

[log]
level = "INFO"
```

环境变量（容器里不想挂配置文件时）：`ARKBRIDGE_ARKLINK`、`ARKBRIDGE_ASTRBOT`、
`ARKBRIDGE_SELF_ID`、`ARKBRIDGE_ARKLINK_TOKEN`、`ARKBRIDGE_ASTRBOT_TOKEN`、`ARKBRIDGE_LOG_LEVEL`。

## 命令行

| 命令 | 作用 |
|---|---|
| `python -m arkbridge run` | 前台运行；`--arklink` / `--astrbot` / `--self-id` / `--arklink-token` / `--astrbot-token` 临时覆盖配置 |
| `python -m arkbridge check` | 自检：分别探 ArkLink（会问账号和在线状态）和 AstrBot（会真握手一次） |
| `python -m arkbridge setup` | 交互式生成 `bridge.toml`，并打印 AstrBot 平台该填哪几个字段 |
| `python -m arkbridge init` | 写一份带注释的模板 |
| `python -m arkbridge --version` | 版本 |

装上之后 `arkbridge` 就是等价命令：`pip install -e .`

## 排障

先把 `python -m arkbridge check` 跑一遍，它会直接指到哪一头。

| 现象 | 原因 | 怎么办 |
|---|---|---|
| `连不上 127.0.0.1:6700` | ArkLink 没起 OneBot 服务 | `python -m arklink serve onebot --port 6700` |
| `返回 401` | 两边 token 不一致 | ArkLink 的 `--token` 与 `arklink_token` 对齐；AstrBot 的 `ws_reverse_token` 与 `astrbot_token` 对齐 |
| `返回 404`（连 AstrBot） | 路径不对 | aiocqhttp 只认 `/ws`，用 `astrbot://host:port` 简写最稳 |
| AstrBot 面板显示在线但发不出消息 | 上游通道还没就绪 | 看 ArkLink 日志；中继会把 `retcode` 原样回传 |
| 群里 @ 机器人没反应 | `self_id` 是 0 | 配置里写死 `self_id = 机器人QQ`，或确认 ArkLink 已登录 |
| 一条消息回两遍 | ArkBot 插件和 AstrBot 都在自动回复 | 迁移期关掉一边的自动回复 |

## 边界（说清楚，免得踩）

- **不补历史**：ArkLink 只向在线客户端推事件，中继断开的那段时间的消息不会补发（缓冲只覆盖「AstrBot 晚连上」这一段）。
- **不做去重**：同时开着 ArkBot 插件和 AstrBot 时，同一条消息会被处理两次，这是使用方式问题，不是 bug。
- **单账号一条链路**：多账号写多段 `[[bridge]]`，每段一个 `self_id`。
- **纯中继**：不做 HTTP 上报、不做发件箱、不落盘。要这些能力用 ArkBot。
- **Python ≥ 3.11**（用 `tomllib`）。

## 测试

生产代码零依赖，但测试**故意用真家伙**，否则协议细节全靠自说自话：

```bash
pip install -e ".[test]"
python -m pytest tests/unit -q            # 18 项：配置、握手、掩码、三档长度、分片、ping/pong、close
python tests/e2e/relay_test.py            # 21 项：真实 aiocqhttp 当 AstrBot 侧
python tests/e2e/cli_test.py              # 4 项：子进程跑文档里那条命令
ARKLINK_PATH=../ArkLink python tests/e2e/arklink_real_test.py   # 9 项：上游换成真的 ArkLink 服务端
```

`relay_test.py` 里的 AstrBot 侧是 `aiocqhttp.CQHttp(use_ws_reverse=True)` —— AstrBot
`astrbot/core/platform/sources/aiocqhttp/` 用的就是它。握手头校验、`echo` 配对、
`X-Self-ID` 路由、`call_action` 注入参数全由库自己实现，中继错一点就当场暴露。

## 目录

```
arkbridge/
  wsclient.py   WebSocket 客户端（RFC 6455 子集，标准库）
  bridge.py     中继核心：两条连接、echo 配对、缓冲、重连
  config.py     配置解析、地址简写、环境变量
  probe.py      自检
  cli.py        命令行
tests/
  unit/         协议与配置
  e2e/          真实对端
  fakes/        假 ArkLink（真 WS 服务端）+ 假 AstrBot（真 aiocqhttp）
```

## License

MIT
