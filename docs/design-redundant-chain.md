# 冗余链路（Redundant Chain）设计

## 1. 背景与目标

现有 `-F a -F b` 是**串行两段**；`-F "host1,host2"` 虽能生成同段多节点，但语义隐式、
无法表达权重/备份/隔离/本段故障切换，且 `loader.Load` 每次重载都重建 hop/node，
失败计数与探测状态被清零。值班同学无法判断两条出口是备份还是串联，单出口故障即全量失败。

本设计在 **gost 模块内**（x 是版本化只读依赖，不新增外部依赖）实现显式的同段冗余语义，
并保证故障切换、隔离自愈、重载状态延续、原子重载与拓扑摘要单一事实源。

## 2. CLI 语法

```
gost -L http://:8080 \
  -F "socks5://127.0.0.1:1080?weight=3|socks5://127.0.0.1:1081?weight=1|socks5://127.0.0.1:1082?backup=true"
```

- 一个 `-F` 值内用 `|` 分隔的多个完整 URL = **同一段链路的并列候选**（parallel candidates）。
- 多个 `-F` = **前后相接的多段链路**（serial segments），语义与历史一致。
- 混用规则（启动前显式报错，见 §7）：
  - 任一 `-F` 含 `|`（新风格）即进入 grouped 模式；grouped 模式下禁止逗号多主机写法
    （逗号是旧隐式并列语法），也禁止与旧风格单节点声明混用。
  - 全是不含 `|` 的 `-F`：走 x 原有 `cmd.BuildConfigFromCmd` 路径，**运行时行为零变化**。

### 2.1 候选级参数（每个 URL 的 query）

| key | 含义 | 校验 |
|---|---|---|
| `weight` | 非负整数权重；未给=均分(权重1)；仅 primary 参与加权 | 非整数/负数/超过 `maxWeight=10000` → 启动拒绝；**0 拒绝**（见下） |
| `backup` | `true` 标记备份候选；有 primary 可用时不接流量 | 仅接受 true/false |
| `maxFails` | 连续失败隔离阈值（候选级覆盖段级） | 正整数 |
| `failTimeout` | 隔离时长（候选级覆盖段级），如 `30s` | 合法 duration、>0 |
| `probe` | 主动探测 URL（tcp/http 探测地址），驱动隔离期满自愈 | 合法 URL |
| `probeInterval` / `probeTimeout` | 探测节奏/超时 | duration >0 |

**关于 weight=0 的冲突处理**：一.2 要求零权重启动前拒绝；五.2 要求"权重为零的候选不获得流量"。
两者字面冲突。本设计遵循更具体的一.2：**weight=0 启动拒绝**；五.2 的意图（存在候选长期
不获得流量、其余按比例）由 `backup=true` 候选承担并在测试中等价覆盖。该取舍在错误信息与
摘要中明示。

### 2.2 段级参数（仅每组第一个 URL 的 query，作用于整段）

| key | 含义 | 默认 |
|---|---|---|
| `strategy` | `random`(默认，加权随机) / `round` / `fifo` | random |
| `maxFails` | 段默认隔离阈值 | 1 |
| `failTimeout` | 段默认隔离时长 | 10s |
| `maxFailover` | 单请求在本段的最大尝试候选数（含首次） | 候选总数 |
| `failoverTimeout` | 本段故障切换的总时限（"无可用候选"必须在该时限内结束） | 30s |

出现在非首个候选上的段级参数 → 启动报错（避免歧义）。

## 3. 运行时模型（internal/topology）

```
Manager (单例，进程级)
 └─ Snapshot（不可变；runtime 与 summary 的唯一事实源）
     └─ Chain（实现 core chain.Chainer）
         └─ Segment（每段一个，实现 core hop.Hop）
             ├─ candidates []*candidateRuntime（含 *chain.Node + 状态机）
             ├─ 选择器（确定性：smooth WRR / round-robin / fifo + backup tier）
             └─ 隔离状态机
```

### 3.1 候选状态机（candidateRuntime，mutex 保护）

```
ACTIVE ──连续失败达 maxFails──▶ ISOLATED（记录 isolatedAt）
ISOLATED ──failTimeout 到期──▶ PROBING（半开，只允许一个试探请求/探测）
PROBING ──建连或探测成功──▶ ACTIVE（计数清零）
PROBING ──失败──▶ ISOLATED（重新计时 isolatedAt=now）
```

- 慢不等于失败：只有建连/握手返回 error 才计数；未配置任何摘除阈值时（单候选兼容路径）
  不做隔离。
- 单次失败不永久摘除：计数未达阈值保持 ACTIVE；隔离必然到期。
- 自愈无需人工/重启：到期后由后台探测（配了 probe）或下次真实流量的试探建连驱动回归，
  以**一次成功的建连或探测**为回归标志，失败重新计时。
- 状态切换只记一条日志（状态迁移在锁内判定，迁移成功才记），禁止每请求刷日志。

### 3.2 选择算法（确定性）

- primary 池优先；primary 全不可用时 backup 池接手；primary 恢复后自动回切。
- `random` 策略使用 **smooth weighted round-robin**（nginx 算法）：无随机源、并行请求下
  选择序列确定、长期严格按权重比例、无锁外竞态；未显式给 weight 的候选 weight=1（均分）。
- `round`：轮询；`fifo`：固定顺序首位。隔离候选在选择阶段被排除。
- 隔离不会导致比例长期失衡：候选回归后 WRR 立即恢复其权重份额（currentWeight 每轮归一）。

### 3.3 本段故障切换（要求 6）

Chain.Route 构造路由时，Segment.Select 按 maxFailover 给出**有序候选尝试列表**（首选 +
其余可用候选），封装进冗余 Route：

- Route.Dial/Bind 依次尝试列表内候选的建连+握手；某候选失败 → 标记失败 → 在
  `failoverTimeout` 预算内尝试下一个，直到成功或列表耗尽。
- 列表耗尽（全部候选不可用/隔离）→ 返回可区分错误
  `ErrNoCandidateAvailable`（errors.Is 可判），绝不退化成底层 dial error，也不会挂到
  超时：failoverTimeout 强制收口。
- 无候选可立即选择（全隔离且未到期）时同样快速返回 ErrNoCandidateAvailable。
- 与现有 Router 重试的关系：冗余段在 Route 内部完成本段切换；服务 handler 的 retries
  保持用户配置不变，不强制改写（仅当 grouped 模式且用户未显式配置时，段内切换已覆盖
  重试需求，Router 仍按既有 retries 语义工作）。

## 4. 重载语义（要求 4、7）

### 4.1 身份与复用

- 段身份：`chain-0` 内按序号 `segment-<i>`；候选身份：段内 `candidate-<i>`，并以
  **规范化地址（去 userinfo）+ connector/dialer 类型**作为内容指纹。
- 每次 `parser.Parse()` + `loader.Load()` 完成后，Manager.Apply(新拓扑声明)：
  1. 先完整构建新 Snapshot（构建失败 → 返回错误，**旧 Snapshot 原封不动继续服务**）。
  2. 段身份相同且候选集合/段策略指纹未变 → 复用旧 candidateRuntime（隔离、计数、探测状态延续）。
  3. 段身份变化或候选集合/策略变化 → 整段重建，状态重置（新集合新状态）。
  4. 原子替换 Snapshot 指针后，关闭被移除段的 node 连接与探测 goroutine（node.Close()）。
- 周期性刷新（-R）走同一 Apply 路径，行为一致。

### 4.2 原子性

- Apply 全程持 Manager 写锁；流量路径读 Snapshot 用 RLock/原子指针，不与构建交错。
- loader.Load 自身的既有行为（含 service 先关后建的历史权衡）不变；Manager 只在
  Load 成功后挂载。若 Load 或 Apply 失败：reloadConfig 返回错误、保留旧拓扑，
  在途请求不中断（已建立连接不受影响），进程不退出。

## 5. 拓扑摘要（要求 8；单一事实源）

- Snapshot 是唯一编排结果：运行时路由读它，`Summary()` 也由它生成，禁止两套拼装。
- 启动与每次成功重载后输出一次（Info 级，固定文本格式，便于 grep/验收）：

```
topology chain chain-0:
  segment segment-0 strategy=random maxFails=2 failTimeout=30s maxFailover=3 failoverTimeout=30s
    candidate candidate-0 role=primary weight=3 addr=127.0.0.1:1080 state=ACTIVE fails=0 isolatedUntil=- threshold=2 isolateFor=30s
    candidate candidate-1 role=backup  weight=1 addr=127.0.0.1:1082 state=ISOLATED fails=2 isolatedUntil=2026-... threshold=2 isolateFor=30s
```

- 地址输出一律经过 userinfo 剥离（凭据遮蔽，硬约束 6）；错误信息同样不得携带凭据。
- 当前状态与计数取自 candidateRuntime 实时值；状态切换日志与摘要共用同一状态机读取。
- 摘要同时以结构化 `TopologySummary` 暴露给进程内调用方（`Manager.Summary()`），
  与运行时路由共用同一 Snapshot，不存在第二套拼装。x 是只读版本化依赖且其 API mux
  不对外暴露挂载点，因此不新增 HTTP 端点；对外可观测面为启动/重载日志块与该结构。

## 6. 配置文件合并（硬约束 2）

- 合并优先级（可复述）：多配置源按 parser 既有左→右 merge；**CLI 生成的 cmdCfg 最后合并**，
  即 CLI 追加在配置文件之后。
- 同名冲突检测（新增，在 Manager.Apply 与启动校验统一执行）：
  - 配置文件中已存在名为 `chain-0` 的 chain 且 CLI 又提供 `-F` → **冲突报错**，
    不静默覆盖/叠加。
  - 同段内候选规范化地址重名 → 报错。
  - 空段（`-F "|"` 或解析后零候选）→ 报错。
  - 悬空引用（候选引用不存在的 resolver/hosts/bypass 等命名对象）→ 由 x 解析期既有
    错误返回，Manager 直接透传为启动/重载失败。
- 摘要中每段标注来源（`source=cli` / `source=file:<name>`），体现合并结果。
- `-O` 导出：grouped 声明被物化为标准 `chains/hops/nodes` 结构（新增字段全部走可选
  metadata key `gost.topology.*`，不改变既有字段名与结构），导出后重启可收敛到同一拓扑。

## 7. 启动前校验清单（非法输入逐项定义）

| 输入 | 期望 |
|---|---|
| weight=0 / 负数 / 非整数 / >10000 | 拒绝，错误含候选序号与合法范围，不含凭据 |
| 空段（无候选） | 拒绝 |
| 段内候选规范化地址重复 | 拒绝（重名候选） |
| grouped 与旧风格 -F 混用、grouped 内出现逗号多主机 | 拒绝（矛盾声明） |
| 段级参数出现在非首个候选 | 拒绝 |
| backup 取值非 bool、duration 非法、maxFails≤0 | 拒绝 |
| CLI chain-0 与配置文件同名 chain | 拒绝（同名冲突） |
| 命名对象悬空引用 | 拒绝（透传 x 解析错误） |

## 8. 兼容性（硬约束 1、四）

- 不含 `|` 的 `-F`：完全不进入 topology.Manager，链路构建、解析、导出与运行时行为
  与现状逐字节一致；不新增任何必填参数。
- 服务声明（-L）与配置文件解析结果不变；config 结构不改字段，只新增可选 metadata key。
- SIGHUP/-R 节奏、退出码、日志级别、多 worker（`--`）语义不变。
- 单候选多段串联的 resolver/hosts/bypass 能力保持：候选 URL 上的这些参数沿用 x
  cmd 解析逻辑（Manager 复用 `cmd.BuildConfigFromCmd` 逐候选解析）。

## 9. 测试（tests/local，纯本机回环，不依赖 Docker/外部服务）

新建 `tests/local/` 包，`go test` 直接跑（既有 tests/e2e 需 Docker，保持不动）：

1. 一正常监听 + 一拒绝连接：同段请求全部成功且只落到可用候选。
2. 权重抽样：固定 N 次请求占比容差校验；backup 候选零流量（覆盖五.2 意图）。
3. 隔离与恢复：连续失败达阈值后隔离期不被选中；恢复监听+隔离期满后自动回归。
4. 重载延续：隔离期内 SIGHUP 等价的 Apply 重载，状态/计数保持、服务不中断；
   换成全新候选集合后状态重置。
5. 导出一致与异常输入：Summary/导出再加载收敛；§7 各非法输入逐一断言错误。
6. `go test -race` 全绿；既有 `go build ./...` 与 `go test ./cmd/...` 全绿。
