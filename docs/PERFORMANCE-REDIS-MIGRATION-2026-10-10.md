# Redis 客户端迁移候选与验收门槛

日期：2026-10-10；性能优化第四十二批。

## 发布状态

- 用户授权：在独立候选分支迁移，真实 Redis 验收前不合入 `eddie`。
- 分支：`codex/redis-context-migration`，起点 `ecf023b`。本批仅后端；前端 `4d186e6`、Agent `7b977e5` 不变。
- **未合入生产构建分支，未部署生产。此记录不表示整体性能优化完成。**

## 实施范围

旧客户端 `github.com/go-redis/redis v6.15.9+incompatible` 迁移至 `github.com/redis/go-redis/v9 v9.22.0`。保留 Go 1.24 / toolchain 1.24.11，不为最新 Redis 客户端顺带升级 Go。依赖整理同步了必需的间接依赖和校验和。

生产策略集中在 `pkg/client/redis.go`：RESP2、关闭身份元数据上报、启用请求 deadline；禁用新增的命令/拨号自动重试，避免写入已发生但响应丢失时被客户端再次执行。保留拨号 5 秒、读写 3 秒、池等待 4 秒、空闲 5 分钟、10×CPU 池大小和原 4 KiB 读写缓冲区，不启用客户端缓存、自动流水线或新索引。

告警列表/精确查询/统计、静默快照、规则索引及回填、认领和恢复 CAS 使用现有调用者 context。JWT Redis 读取传递 HTTP 请求 context；仍未接收 context 的遗留接口明确沿用 background 和客户端 I/O 预算，不能声称全链路全部可取消。

PubSub 在取消时仅关闭自己拥有的订阅连接，不关闭共享 Redis 客户端；处理 channel 关闭，避免解引用空消息。Leader 正常操作使用生命周期 context；退出时使用独立、最多 3 秒的清理 context，原子删除仍只作用于自己持有的锁。

Redis key、JSON、Lua 比较条件、索引默认关闭策略和业务权限不变。v9 新建连接会进行协议协商；测试计数先完成握手，再统计业务查询，取消前零访问的断言仍保留。真实服务器版本、认证、ACL、握手回退仍须验收。

## 取消能力的准确边界

| 场景 | 当前结果 |
|---|---|
| 请求带较短 deadline，Redis 已收到命令但不返回 | 本地 TCP fixture 证明按 deadline 返回错误，失效连接丢弃后可继续查询 |
| 池已满，等待者取消 | 返回取消；占用连接的其他请求不受影响 |
| 无 deadline 的读取已经进入 socket，再执行普通 cancel | **仍等待响应或读超时，不能立即中断**；保留专项测试记录这一限制 |
| 已发送的写入取消/丢失回包 | 无法承诺回滚，客户端不自动重放；现有业务 CAS 重试不改变 |
| 已开始等待订阅确认或消息，订阅取消 | 独立订阅关闭；共享连接池仍可用 |

没有使用每请求 goroutine 提前返回或关闭共享池来伪装取消。取消立即打断所有在途 I/O 仍属待解决问题；不能把本次升级等同于已经实现该目标。

## 本地验证

- `pkg/client/redis_context_test.go`：5 项客户端策略/慢响应/池等待/丢失写回包/普通取消边界测试。
- `internal/cache/redis_deadline_test.go`：告警列表、精确事件、计数、静默和规则全读回退共 5 条真实 TCP 慢响应路径；超时必须为错误而非空成功。
- `pkg/tools/redis_lifecycle_test.go`：订阅确认取消、中文消息投递后取消、取消后只清理自身 Leader 锁。
- 上述 9 个顶层专项连续 10 轮通过；已有 Lua/CAS、索引 WATCH 回填、静默、恢复归档、通知等安全回归包通过。miniredis 是本地协议 fixture，不是真实 Redis 实例。
- 后端安全包集合的 `go vet`、`go build ./...` 通过。没有运行包含外部连接示例的 `pkg/provider/test`。
- 无 Docker/nerdctl、真实 Redis/MySQL 或 Go race 所需 C 工具链；不宣称这些检查通过。

## 真实 Redis 验收入口（尚未执行）

新增 `TestRedisRealAcceptance`，默认跳过。必须连接**专用非生产验收实例**。通过环境安全注入 `WATCHALERT_REDIS_TEST_PASSWORD`，不要把密码放在命令行历史、聊天或仓库。若使用 ACL 用户，另设 `WATCHALERT_REDIS_TEST_USER`。

PowerShell 示例，地址必须替换为已准备的验收实例：

```powershell
$env:WATCHALERT_REDIS_ACCEPTANCE = '1'
$env:WATCHALERT_REDIS_TEST_ADDR = '<验收地址>:6379'
$env:WATCHALERT_REDIS_TEST_DB = '0'
go test ./pkg/client -run '^TestRedisRealAcceptance$' -count=1 -v -timeout 30s
Remove-Item Env:WATCHALERT_REDIS_ACCEPTANCE
Remove-Item Env:WATCHALERT_REDIS_TEST_ADDR
Remove-Item Env:WATCHALERT_REDIS_TEST_DB
```

入口只写每次随机生成的 `w8t:acceptance:<uuid>` key，带 60 秒 TTL，并尽力清理自己的 key；不执行 FLUSHDB、KEYS 或操作业务 key。覆盖认证/数据库选择后的连接、中文 Hash、TTL、Lua CAS 与旧值拒绝、WATCH 冲突、PubSub、预先取消及客户端继续使用。**此入口只是客户端兼容烟测，不替代完整业务验收**；若 ACL 不允许该测试前缀，应在验收实例配置专用权限，不放宽生产 ACL。

合并前还必须完成：

1. 与生产相同 Redis 版本和 ACL 的独立实例，记录版本、认证方式、逻辑库与测试结果；验证 RESP2/HELLO 回退，不能只使用默认无密码实例。
2. 验收后端实际告警认领、静默生效/删除、恢复 CAS、通知去重与故障中心查询；使用模拟通知接收端，不发真实通知。索引如计划启用，单独检查 EVAL、WATCH、MULTI/EXEC、ZSET 权限和多写入者一致性。
3. Linux 容器和 race 检查；故障注入代理验证慢返回、断连、重连、池耗尽及正常查询恢复，不对生产压测。
4. 同负载比较迁移前后的连接数、RSS、CPU、查询延迟/失败率；不能从小样本推导生产收益。
5. 明确接受并记录普通 cancel 的 socket 限制，或另行实现并验证解决方案；没有验收记录不得合入 `eddie`。

回退：候选分支未发布时继续使用 `eddie` 基线即可。未来验收发布后，保留旧后端镜像用于回退；本批不修改存储格式，不清 Redis 数据、不重建索引。

## 选型依据

对照本地固定版本源码 `options.go`、`internal/pool/conn.go` 和 `internal/pool/pool.go`；参阅 [v9.22.0 官方发布说明](https://github.com/redis/go-redis/releases/tag/v9.22.0) 和 [官方生产使用说明](https://redis.io/docs/latest/develop/clients/go/produsage/)。显式固定重试、超时与缓冲策略，避免默认值变化被误当作本次性能优化效果。
