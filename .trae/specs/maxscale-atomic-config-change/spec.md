# MaxScale 分代原子配置变更（可恢复的依赖图切换）- 产品需求文档

## Overview

- **Summary**: 把 MaxScale CR 一次 spec 变更中涉及的 servers、filters、monitor、services、listeners 作为同一 `metadata.generation` 下的一张依赖图，以「校验 → 准备 → 收敛 → 切换 → 清理 → 提交」的可恢复事务方式下发到 MaxScale REST API；变更过程在所有副本间原子收敛，失败、重试与 operator 重启都保留上一版可服务图，且只删除该 CR 管辖的对象。
- **Purpose**: 现状按阶段直接改写正在接流量的 REST 配置，后续阶段失败、Pod 不可达或 operator 重启时，已删除/改写的对象无法回滚；status 中分散的 spec hash 可能把半成品视为完成；删除集合来自实时对象索引，可能误删非本 CR 创建的对象；多副本之间也没有「全部具备同一可用图」才切换入口的屏障。
- **Target Users**: 通过 MaxScale CR 管理代理拓扑的平台/SRE 人员；依赖 MaxScale Ready 条件与 status 判断变更是否完成的自动化流水线。

## Goals

- G1: 同一 generation 的依赖图作为一个有清晰阶段记录、可恢复、可幂等续跑的配置变更（plan）执行。
- G2: 变更准备阶段不停止、不删除旧 listener / service / monitor；旧图在切换前持续可服务。
- G3: 新增与就地修改的对象先在所有 MaxScale 副本上准备并收敛，然后才切换入口；切换后再次确认所有副本收敛到同一可用图。
- G4: 清理只删除「本 CR 管辖且新图不再声明」的对象，依赖序删除；MaxScale 中与本 CR 无关的对象永不删除。
- G5: 任一对象操作或多副本同步失败时，保留上一版可服务图，status 记录未完成阶段与原因，Ready 不得为 True；重试/重启不重复执行破坏性操作、不把半成品标为 Ready。
- G6: 连续 spec 更新时，较新 generation 废弃陈旧 plan，陈旧 plan 的删除集合绝不作用于较新关系；旧 plan 已创建的「残留对象」纳入新 plan 的管辖台账后继续收敛。
- G7: 保持现有 suspend、switchover（primaryServer）、未改变配置调谐、初始引导（Init）与对象运行状态（started/stopped）的行为语义不变。

## Non-Goals

- 不追求「listener 改名但新旧端口相同」场景的零中断：同一 MaxScale Pod 上两个 listener 无法绑定同一端口，此类变更在切换阶段存在用户已确认接受的最短切换窗口。
- 不改变 MaxScale config sync（`maxscale_config` 表）机制本身，不改变 switchover 语义、suspend 语义与 CR 删除/finalizer 流程。
- 不新增 CRD kind，不改变 listener 端口不可变等既有 webhook 契约，不调整 MaxScale 前面的 Kubernetes Service 的调和行为。
- 不新增 E2E 测试（遵循仓库当前约定）；凭据轮换、TLS、metrics、connection 等既有阶段不在本次事务编排内。
- 不试图在准备阶段提前暴露新 listener 端口对应的 Kubernetes Service 端口（K8s Service 仍按现有阶段从 spec 调和）；新增端口在准备完成前不可用属可接受的增量现象，旧端口流量不受影响。

## Background & Context

- 当前相位顺序见 `internal/controller/maxscale_controller.go`：`Primary Server → Servers → Filters → Monitor → Monitor State → Services and Listeners → Service State → Listener State → Switchover`。每个 changed 相位用各自 spec 片段 hash（`status.serversSpec/filtersSpec/monitorSpec/servicesSpec`）判断是否执行，写操作只打到单个 healthy Pod（依赖 config sync 传播），删除集合由「期望索引 vs 实时全量索引」的 diff 得出，删除排在新增/改写之前，且只在单个 hash 相位成功后立即更新。
- MaxScale REST 客户端能力：server/filter/monitor/service/listener 均有 `List/Get/Create/Patch/Delete`、service/monitor/listener 有 `Start/Stop`，删除可带 `force=true`；HA 时配置对象经一个写副本同步，运行状态（started/stopped）不同步，需逐 Pod 操作（现有 `forEachPod`）。
- 依赖方向：listener → service → servers/filters；monitor → servers。service 与 listener 当前为 1:1（listener 内联在 service 中），listener 端口在 webhook 层不可变且 spec 内唯一。
- MaxScale 多副本通过 config sync 版本（每个 Pod 的 `maxscale.config_sync.version` 与后端库版本）可观测收敛状态；现有代码已有版本读取与冲突处理。

## Functional Requirements

- **FR-1（代次计划与校验阶段）**: 每次调谐在基础设施与客户端就绪后，对当前 `metadata.generation` 计算整图期望指纹（servers、filters、monitor、services、listeners 的身份、参数与关系的规范化摘要）；当期望指纹不同于 status 中已提交的上一版指纹时，为该 generation 建立唯一 plan 并持久化到 status。建立 plan 后、任何 REST 写操作前，必须完成校验：
  - 静态闭包：名称/地址/端口唯一性（webhook 已覆盖的不重复报错但作为防线）、listener 与 service 一一对应、每个 service.filters 引用都能在 filters 中解析、monitor 关系的 servers 都在 servers 集合内、monitor module 合法。
  - 动态引用可解析：monitor/service 所需密码 Secret、TLS 引用（启用时）、MariaDBRef（存在时）可解析；不可解析时不产生任何 REST 变更。
  - 操作分类：在「管辖台账 ∪ 实时对象」与期望图之间分类 create / in-place update / delete，并标记「新旧 listener 端口冲突、只能在切换阶段创建」的延迟对象。
- **FR-2（准备阶段，非破坏）**: 在不触碰旧入口的前提下准备新图：
  - 新增 servers（含其期望 maintenance 状态）、新增 filters；
  - 新增 services（带完整 servers/filters 关系；无 listener 时不承载流量）；新增 monitor（仅当 monitor 改名时）必须以 stopped 状态创建；
  - 新增 listeners：端口不与任何旧 listener 冲突的立即创建；端口冲突（含同名端口改不了的改名场景）的延迟到切换阶段；
  - 对身份未变的对象执行参数/TLS 等就地 patch；其中会改变路由/拓扑指向的关系变更（同名 monitor 的 servers 关系、既有 service 的 servers 关系）延迟到切换阶段；纯参数类 patch 可在准备阶段执行；
  - 准备阶段禁止任何 delete，禁止 stop/delete 旧 listener、旧 service 与旧 monitor；每个完成的操作在 plan 中逐条记检查点。
- **FR-3（收敛屏障，切换前）**: 多副本（HA）时，在写副本应用的准备变更经 config sync 传播后，必须确认每个副本：config sync 版本与后端库版本一致，且准备产生/改写的对象在该副本上存在并与期望指纹一致；单副本时在本副本做等价校验。任一 Pod 不可达或未收敛时停留在本阶段并重排队，不进入切换。
- **FR-4（切换阶段，入口切换）**: 仅在 FR-3 通过后执行，且所有运行状态操作逐 Pod 进行（状态不经 config sync 传播）：
  - 同名 monitor：下发延迟的 servers 关系变更；改名 monitor：在所有副本停止旧 monitor 后启动新 monitor；
  - 既有 services 的 servers/filters 关系变更在此下发；
  - 端口冲突 listener：先在所有副本停止旧 listener，再由写副本删除旧 listener（及仅因改名而删除的旧 service），创建新 listener，经同步确认后逐副本启动；无冲突的新 listener 此阶段仅需按期望 started/stopped 状态校正；
  - 按各对象的 suspend 期望对齐 monitor/service/listener 运行状态。
- **FR-5（切换后收敛屏障）**: 再次确认所有副本：新入口对象（listeners 与其 service）在每个副本存在、指纹一致、运行状态符合期望且可承载流量（listener 处于 Started 状态，除非显式 suspend）；config sync 版本一致。未通过则不进入清理。
- **FR-6（清理阶段，仅删旧代次管辖对象）**: 删除集合 = 管辖台账中存在但期望图不再声明的对象，按依赖序逐类删除：listeners → services → 旧 monitor → filters → servers；删除带 `force`，404 视为已删除（幂等）；每个删除在 plan 中记检查点，重启/重试不得对同一对象重复发起破坏性删除。
- **FR-7（提交阶段）**: 全部阶段成功后，原子地：用本代次期望图替换 status 中的「上一版已提交图」快照与整体指纹、把管辖台账收敛为期望对象集合、把 plan 标记为 completed。只有提交完成后 Ready 条件才允许依据后端 server 状态变为 True。
- **FR-8（失败记录与重试语义）**: 任一阶段的 REST 操作、引用解析或副本收敛失败时：停止后续阶段，保留上一版可服务图（准备/切换前不删旧对象，清理失败不影响新图已承载流量），在 status 的 plan 中记录阶段、对象类别/名称与错误摘要（不含凭据），Ready=False 并给出区分「进行中/失败」的原因与信息，发出 Event，按退避重排队；重连后从检查点幂等续跑。
- **FR-9（代次栅栏）**: 每次调谐以最新 CR 为准；若 status 中 plan 的 generation 与当前 `metadata.generation` 不同，必须废弃该 plan：不执行其任何待办删除/切换，先把其已完成准备中创建的对象身份并入管辖台账，再基于最新 spec 重新计算 plan 与删除集合。任何删除决策必须能从「当前 spec + 已提交台账」重算，禁止持久化执行一个旧 generation 的删除清单。
- **FR-10（管辖边界）**: 删除只能来自持久化的本 CR 管辖台账（历史上由本 CR 创建、或升级采用时声明且实时存在的对象）与期望图的差集；实时存在但从未被本 CR 声明/采用的对象（人工或其他系统创建）永不删除、不阻止 Ready，也不要求其指纹一致。
- **FR-11（升级采用）**: 部署新逻辑后第一次对存量 CR 调谐时，不进行追溯删除：以「当前 spec 声明 ∩ 实时存在」采用为初始管辖台账与上一版已提交图（实时指纹可获得部分按实时快照），随后差异为空即提交；只有采用完成之后的代次差异才触发事务编排。
- **FR-12（既有行为保持）**:
  - suspend：在配置事务相位之前短路，行为与现状一致；plan 在挂起期间不推进，恢复后从检查点续跑；
  - switchover（`spec.primaryServer`）：仍在配置事务之后按现状执行，配置变更未提交完成时不执行 switchover；
  - 无配置变化（期望指纹 == 已提交指纹、无未完成 plan）：不产生任何 REST 写调用，走原有状态/观测路径；
  - Init 引导：全新 MaxScale 首次引导路径保持现状（逐 Pod 直接创建），引导完成后再由事务逻辑采用并提交基线；
  - monitor/service/listener 的 started/stopped 状态调和仍逐 Pod 执行，suspend 模板语义不变。
- **FR-13（status 与可观测性）**: 以统一的配置变更状态（绑定 generation、阶段、期望指纹、检查点、管辖台账与上一版图、错误摘要）取代分散的 `serversSpec/filtersSpec/monitorSpec/servicesSpec` 决策作用；旧字段保留但不再被读取或写入（向后兼容，不删除字段）。新增集中的 Event reason 常量，阶段推进与失败发出 Event；日志与状态中不得出现密码、口令等凭据。

## Non-Functional Requirements

- **NFR-1（向后兼容）**: 新增 status 字段全部 optional/omitempty；不删除/重命名既有 spec/status 字段、枚举与 kind；不收紧 webhook 校验；升级后既有 CR 无需人工干预即可采用并继续调谐。
- **NFR-2（CRD 体积）**: 台账与检查点仅存对象身份（名称）、小指纹与阶段枚举，规模与 servers/services 数量同阶；`make crd-size` 保持通过（<900KB）。
- **NFR-3（可测试性）**: 图构建、校验、操作分类、端口冲突检测、台账差集、代次栅栏/残留合并等纯逻辑必须有不依赖 Kubernetes/REST 的单元测试；控制器编排提供可注入的 REST 调用边界以便单测/集成测覆盖「准备失败保旧图、重启续跑、新版本废弃旧 plan、外来对象不删」等场景。
- **NFR-4（质量门禁）**: `make gen` 产物一致、`make lint`、`make test` 通过；遵循项目简洁性原则，复用现有 diff/index、client、condition、events、patchStatus 模式，不引入不必要的抽象层。
- **NFR-5（安全）**: 计划与事件只记录对象身份、阶段与错误摘要，严禁记录 monitor/service 密码或含凭据的 SQL/REST 报文；删除类操作保持最少且可解释。

## Constraints

- **Technical**: Go + controller-runtime；REST 操作经现有 `pkg/maxscale/client`；配置对象写单副本 + config sync，状态操作逐 Pod；生成文件必须经 `make gen` 重生成（deepcopy、CRD、helm CRDs、bundles、docs）。
- **Business**: 该控制器管理生产数据库代理，任何半成品入口切换都可能造成连接中断或错误路由；实现必须默认保守（先增后删、失败冻结）。
- **Dependencies**: MaxScale REST 语义（listener 端口占用、force 删除、config sync 版本）、现有 config sync 后端表与 cooperative monitoring。

## Assumptions

- MaxScale 对「同 ID 对象 PATCH 参数」不会销毁其 listener 的既有连接；会改变拓扑的关系（monitor/service 的 servers）统一切换阶段处理以保守保序。
- config sync 传播配置对象的创建/删除/关系，但不传播运行状态，因此切换/状态操作逐 Pod 执行、屏障逐 Pod 校验。
- 新建 monitor 可以 stopped 状态存在且不参与锁竞争；改名 monitor 的双 monitor 共存窗口内旧 monitor 仍在运行、新 monitor stopped。
- 用户已确认：listener 改名但端口相同的场景接受切换阶段的最短中断窗口；filters 纳入同一事务编排。

## Acceptance Criteria

### AC-1: 期望图先校验、失败零写入
- **Type**: `rule`
- **Given**: 一个 Ready 的 MaxScale，spec 变更引用了不存在的 filter，或 monitor/service 密码 Secret 不可解析
- **When**: 新 generation 触发调谐
- **Then**: 不产生任何 MaxScale REST 写/删调用；旧 listeners/services 持续可用；Ready=False，status 配置变更状态停留在校验阶段并记录原因；修复引用后同一 generation 重试可进入准备阶段
- **Pass Condition**: 单测构造不可解析引用断言无写操作且返回校验失败；控制器代码路径中校验先于所有 Create/Patch/Delete
- **Evidence**: 新增纯逻辑/编排单测；代码审查证据

### AC-2: 准备阶段不切断旧入口
- **Type**: `rule`
- **Given**: 一次同时变更 servers、monitor 关系、services、listeners 的 spec 更新
- **When**: plan 处于准备阶段（含中途失败与 operator 重启后续跑）
- **Then**: 旧 listener、旧 service、旧 monitor 均不存在删除或停止操作；只允许新增对象、纯参数就地 patch 与新增 stopped monitor；删除调用数为 0
- **Pass Condition**: 用记录 REST 调用的假客户端跑完整准备阶段及一次中断-续跑，断言无 DELETE、无旧对象 Stop
- **Evidence**: 编排单测

### AC-3: 所有副本收敛后才切换，切换后再次收敛才清理
- **Type**: `rule`
- **Given**: 多副本 MaxScale（config sync 开启）与单副本两种形态
- **When**: 准备完成 / 切换完成
- **Then**: 准备屏障要求每个副本配置版本一致且对象指纹齐备才允许切换；切换屏障要求每个副本新 listener/service 存在、指纹一致且运行状态符合期望才允许删除旧对象；任一副本不可达时重排队且不切换/不清理
- **Pass Condition**: 单测以假多副本客户端分别制造「版本落后」「Pod 不可达」「全部收敛」三种返回，断言相位推进/停留正确；单副本形态断言等价本地校验
- **Evidence**: 编排单测

### AC-4: 失败保留旧图并记录未完成阶段，Ready 不为 True
- **Type**: `rule`
- **Given**: 准备、切换、清理任一阶段注入 REST 失败（server/monitor/service/listener 任一类）
- **When**: 调谐结束于错误
- **Then**: status 记录 plan generation、阶段、失败对象与错误摘要；Ready 条件为 False 且原因表明配置变更受阻；上一版 listener/service 仍可服务（清理阶段失败时新图已承载流量，旧对象残留但不影响）；发出 Event
- **Pass Condition**: 每个阶段一类失败的表驱动单测断言条件、事件与旧图完整性
- **Evidence**: 编排单测 + 条件辅助逻辑审查

### AC-5: 重试与重启不重复删除、不把半成品标 Ready
- **Type**: `rule`
- **Given**: 任一破坏性操作（删除旧对象、冲突 listener 切换序列）成功返回后、检查点落盘前制造重启，以及完成部分删除后注入失败
- **When**: 同一 generation 重新调谐
- **Then**: 已删除对象按 404/检查点识别为已完成，不重复发起破坏性动作；plan 未提交前 Ready 始终 False；从上次检查点继续直至提交
- **Pass Condition**: 单测重放「操作成功/检查点未写」与「404 已删」两种交错，断言调用序列幂等
- **Evidence**: 编排单测

### AC-6: 新 generation 废弃旧 plan，陈旧删除不作用于新关系
- **Type**: `rule`
- **Given**: generation G1 的 plan 已准备部分对象、尚未清理；此时 spec 再次更新为 G2（例如把 G1 准备删除的 server 重新声明、或把 G1 新建的对象改名）
- **When**: 调谐读到 G2
- **Then**: G1 plan 立即作废且其待办删除/切换不执行；G1 已创建对象身份并入 G2 管辖台账；G2 删除集合由「台账 ∩ ¬G2 期望」重新计算；最终实时图等于 G2 期望图且没有 G2 之外的对象被删
- **Pass Condition**: 单测连续两代交错场景断言对象最终集合与调用序列；代码中不存在不经过当前 generation 校验即执行的删除路径
- **Evidence**: 纯逻辑单测 + 代码审查

### AC-7: 只删除本 CR 管辖对象，外来对象保留
- **Type**: `rule`
- **Given**: MaxScale 中存在人工创建的 server/service/listener/filter（不在该 CR spec，也不在台账）
- **When**: 该 CR 发生多次配置变更，包括删除本 CR 自己的同名类对象
- **Then**: 外来对象不被删除、不被 patch、不出现在失败/未就绪原因中；本 CR 对象正常增删
- **Pass Condition**: 单测在实时索引中混入外来对象，跑完整事务后断言外来对象零调用且仍存在
- **Evidence**: 编排单测

### AC-8: suspend、switchover、无变化调谐与首次引导行为保持
- **Type**: `rule`
- **Given**: 分别为：spec.suspend=true 且有未完成 plan；配置 plan 未完成时设置 spec.primaryServer；期望指纹等于已提交指纹；全新空 MaxScale 首次引导
- **When**: 调谐
- **Then**: suspend 时 plan 不推进并按原节奏 requeue，恢复后续跑；plan 完成提交前不发起 switchover，完成后行为同现状；无变化时无 REST 写调用；首次引导沿用现有逐 Pod 创建路径，之后采用并提交基线
- **Pass Condition**: 相位顺序与短路条件在代码/单测中验证；现有 MaxScale 相关测试不回归
- **Evidence**: 单测 + `make test`

### AC-9: 升级采用不追溯删除，存量 CR 平滑收敛
- **Type**: `rule`
- **Given**: 运行旧版本 operator、已配置完成的 MaxScale CR（无新 status）
- **When**: 升级到新逻辑后首次及后续调谐
- **Then**: 首次调谐采用「spec 声明 ∩ 实时存在」为台账与基线、不删除任何实时对象；实时与 spec 一致时提交基线并保持 Ready；之后再改 spec 才产生删除事务
- **Pass Condition**: 单测模拟空台账 + 实时=spec / 实时含外来对象两种初始态
- **Evidence**: 纯逻辑单测

### AC-10: 向后兼容与生成产物一致性
- **Type**: `rule`
- **Given**: 新增 status 字段与事件常量
- **When**: 执行 `make gen`、`make build`、`make lint`、`make test`、`make crd-size`
- **Then**: deepcopy/CRD/helm/bundles/docs 全部重新生成且差异已提交；旧 status hash 字段保留；构建、lint、单测通过；CRD 总量低于 900KB
- **Pass Condition**: 上述命令退出码为 0，工作区无未提交生成差异
- **Evidence**: 命令输出与产物 diff

### AC-11: 事务编排的工程质量
- **Type**: `rubric`
- **Dimension**: 简洁性、可恢复性与代码库一致性（复用 client/diff/index/condition/events/patchStatus；阶段状态机清晰；无跨对象共享内存态；无凭据泄漏）
- **Scale**: 1-5
- **Anchors**: 1 = 引入并行框架或重复实现 REST/状态扩散逻辑、阶段隐式且难恢复；3 = 功能正确但阶段与检查点散落、部分复用；5 = 纯图逻辑与 REST 编排清晰分层、检查点驱动幂等、与现有 MaxScale 代码风格一致且无多余抽象
- **Pass Threshold**: >= 4
- **Evidence**: 独立评审对照本 rubric 打分并给证据

## Open Questions

- 无（端口冲突与 filters 范围两个关键取舍已经用户确认；其余为实现期可决策事项，记入 tasks.md）。
