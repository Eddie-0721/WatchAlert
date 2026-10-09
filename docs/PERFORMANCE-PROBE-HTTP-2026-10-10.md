# HTTP 拨测连接释放

日期：2026-10-10。后端基线 `091e6a8`，开发分支 `codex/probe-http-lifecycle`。前端及独立 Agent 未改动。

## 问题与最小修复

`pkg/provider/probe_http.go` 的 `executeHTTPProbe` 每次创建独立 `http.Transport`，未设置空闲超时，也未关闭其空闲连接。关闭 `response.Body` 不等于销毁连接池：204、零长度响应以及重定向中间响应均可能留下可复用连接，但后续拨测使用另一连接池，无法复用它们。

该入口同时由定时拨测和即时拨测使用。本地服务复现连续 10 次 HTTP/HTTPS 拨测完成后，分别仍有 10 条连接；跨服务器重定向成功和跳转到不支持的协议失败也遗留原服务器连接。测试服务清理负责关闭这些连接，不访问真实拨测目标。

修复为创建客户端后立即登记 `defer client.CloseIdleConnections()`。成功路径先关闭响应体、再关闭私有池；请求创建失败和重定向失败同样经过清理。没有把拨测改为跨轮连接复用，因此握手/建连仍计入原本的延迟。未改变 TLS 校验、代理、超时、重定向、GET/POST、自定义请求头或指标含义。

## 验证

- 3 个顶层测试（含 HTTP、HTTPS、重定向成功/失败子场景），修复前均因残留连接失败，修复后连续 10 次通过。
- 服务端实际连接计数确认 10 次拨测仍创建 10 个独立连接，完成后归零，而非只断言代码调用了 close。
- POST 请求体、默认 Content-Type/User-Agent、自定义请求头和标签保持；503 仍按既有语义输出 `probe_http_success=1`、状态码 503，未擅自改变可达性的业务定义。
- 后端配置、cmd、API、internal、alert、client、provider、cloudwatch、tools、medium、agenttoken、secretbox、OIDC 的非缓存测试通过。未运行 `pkg/provider/test` 外部服务示例。
- 同一包集合的 `go vet` 与 `go build ./...` 均通过；没有运行生产压测或访问真实通知/拨测目标。

CI 原包清单遗漏 `alert/consumer`、`alert/mute`、`pkg/client`、`pkg/medium` 等已有性能回归，本批将其纳入默认检查，明确使用 120 秒包测试期限及非缓存执行。保留旧 Agent 的兼容检查，不借此删除仍被引用的目录。远端 CI 结果与本地通过需分别确认。

## 发布与剩余边界

只需后端新镜像，无接口、配置、依赖、数据库或 Redis 格式变化。未部署生产。回滚本批仅需前一镜像，但会恢复该连接累积问题。

本批仅保证已结束的 HTTP 拨测释放私有连接池，不声称解决正在阻塞的拨测。代码仍有以下独立待办：

1. `alert/probe/service.go` 的停止 context 未传入协议探测器；remote write 使用服务根 context。停止旧规则后仍可能等待旧探测完成并继续写指标。
2. 定时入口未调用现有 `ValidateProbeRule`，非正评估周期可能令 `time.NewTicker` panic；HTTP timeout 非正时无总超时。应另批统一创建/修改/启动/即时拨测的验证，不能只在页面限制输入。
3. 拨测没有全进程并发上限。先用本地慢目标验证任务生命周期和取消，再决定适当预算，不能将告警评估的 32 槽误认为已覆盖拨测。

上述是代码确认的边界，尚未宣称生产发生积压、panic 或错误指标。ICMP 涉及权限和网络环境，不能用 HTTP fixture 冒充其集成验收。
