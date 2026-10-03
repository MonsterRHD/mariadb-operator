# MaxScale 分代原子配置变更 - Independent Review

- **Reviewer**: 主会话自评审（本环境未提供 fresh-context 子代理工具；以独立检查清单逐 AC 核对，证据均为可复跑命令/代码位置）。
- **Date**: 2026-10-03
- **Result**: pass（发现 2 个阻断性缺陷与 1 个覆盖缺口，均已修复并回归）

## 检查方式

- 静态：逐 AC 对照代码路径、状态字段、相位顺序、删除集合来源。
- 动态：`make test`（40 套件全过）、`make lint`（0 issues）、`make build`、CRD 793KB < 900KB。

## Checkpoints

| AC | 覆盖证据 | 结果 |
| --- | --- | --- |
| AC-1 | runner 测试「rejects unresolvable references without any REST mutation」断言校验失败时无 Create/Patch/Delete/Start/Stop/Set；graph Validate 表驱动 5 类失败；动态引用 validateConfigChangeRefs（monitor/server 密码、TLS CA） | pass |
| AC-2 | runner happy path 断言 firstSwitch 前无 Delete/Stop；prepare 仅 create/patch，改名 monitor stopped 创建（CreateMonitorStopped）；冲突 listener 不在准备阶段创建 | pass |
| AC-3 | converge 4 specs（版本落后/Pod 不可达/对象缺失/全副本收敛）；pre/post 两次 `Converged` 调用，HA 比较 config_sync.version，单副本跳过版本；await 未收敛返回 RequeueAfter(syncInterval) | pass |
| AC-4 | happy path 调用序列：PatchMonitorRelations/ServiceRelations 在 switch；冲突 listener 停旧(所有 Pod)→删→建→启新(所有 Pod)；旧 monitor rename 停所有 Pod→切关系→启新 | pass |
| AC-5 | post-switch Objects 含整图+MonitorRelations+ServiceRelations+RunningListeners；listenerRunning 接受非 suspended 为 Running | pass |
| AC-6 | cleanup 依赖序 listeners→services→monitor(先停所有 Pod)→filters→servers；删除来自 plan.Deletes（=ledger−desired）；404 幂等（DeleteXxx 容忍 IsNotFound） | pass |
| AC-7 | commit 写 LastApplied/Managed=desired.ObjectIDs、Stage=Completed、清 Checkpoints/Error；404 幂等 + 检查点使重跑不重复破坏性调用；Ready 门控 setMaxScaleReady | pass |
| AC-8 | settle：existing Completed 且 DesiredHash==当前指纹 → 返回 nil，零 REST；旧 hash 字段无任何写入方（grep 确认） | pass |
| AC-9 | settle stale 分支 + graph.Supersede 单测（stale-server/temp-server 残留在新台账，G2 重声明 mdb-1 不删）；runner「discards stale-generation plans」用例；删除始终由当前 ledger 重算 | pass |
| AC-10 | 删除集合只来自 ledger；Adopt=desired∩live；foreign-server/service/listener 在 happy 用例零删除断言；新增 ForeignPortHolders：非台账 listener 占用声明端口时阻断而非删除（runner 用例） | pass |
| AC-11 | Adopt 单测（desired∩live、foreign 忽略、差异存在时进入 change）；升级首调不追溯删除；端口 squatter 选择阻断（见发现 1） | pass |
| AC-12 | Suspend 在 Config 前短路；switchover InProgress 守卫（1s requeue）；Init 路径未改；状态相位保留并在提交后执行；无变化零写；新增单测覆盖 | pass |
| AC-10/事件 | 事件常量集中使用：Invalid/Preparing/Switching/CleaningUp/Completed/Failed；错误摘要含 stage/kind/object，不含密码；日志仅 ID 与阶段 | pass |
| AC-10/NFR | 新字段全 optional；旧 hash 字段保留 Deprecated；deepcopy/manifests/CRD/helm/bundles/api_reference 重新生成；793KB | pass |

## 发现与修复

1. **[已修复] 空台账下外来 listener 会被切换删除**：首版采用逻辑把「占用期望端口的 live listener」一并 Adopt，导致升级首调恰遇改名时，一个从未被本 CR 声明的 squatter 会在 switch 阶段被删除（违反 AC-10）。修复：`Adopt` 回归纯 desired∩live；`BuildPlan` 新增 `ForeignPortHolders`，编排器在计划阶段对非台账端口持有者直接失败阻断（不写任何变更）；仅 ledger 内持有者走冲突切换序列。新增 graph + runner 两个用例。
2. **[已修复] 校验失败发生在 settle 之前会写入空台账 change**：原顺序先 settle 后校验，首次变更且校验失败时 status 留下空 Managed 的 change，后续一旦遇同端口冲突可能误删持有者。修复：校验前置（仍是任何 REST 写之前），settle 后再把校验错误记录到已建立台账的 change；change 为 nil 时只返回错误不伪造台账。
3. **[已修复] 编排器/屏障大函数超复杂度**：初次 lint gocyclo/nestif 7 处超标，拆分为 prepareX/switchX/alignX/deleteManagedKind/allMatch 等小函数，复跑 0 issues。

## 未覆盖与说明

- 未新增 E2E（仓库约定不新增）；MaxScale 真实 REST 行为由注入接口单测覆盖，集成路径由既有 KIND 流水线在 CI 验证。
- 本机 `make gen` 的 helm-config yq 为非本机架构二进制无法运行（仅 values.yaml 补丁步骤，与本次改动无关）；CRD/helm crds/bundles/docs/examples 生成不受影响。
