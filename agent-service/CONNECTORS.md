# 外部系统 Connector 约定

WatchAlert Copilot 不允许模型、浏览器或插件直接请求外部系统。未来接入
CMDB、Kubernetes、日志平台或工单系统时，必须新增一个由 Go Tool Gateway
托管的具名 Tool，而不是把 URL、Token 或任意 HTTP 请求参数交给模型。

每个 Connector Tool 必须满足：

1. 在 Go 中注册固定的 Tool 名称、输入结构和最大输出量；禁止通用 URL / SQL / Shell Tool。
2. Gateway 从服务端 Secret 读取凭证；Agent 服务与浏览器都不能读取凭证。
3. 按运行令牌中的租户、用户、数据源和环境范围校验；Connector 不能信任模型参数中的租户或环境。
4. 默认只读。任何写操作都必须生成待确认预览，由 Go 后端以用户当前权限重新校验后执行。
5. 每次调用写入 `w8t_agent_tool_calls` 审计记录，并对敏感字段做脱敏。

推荐的后续 Tool 命名：`cmdb.service_get`、`kubernetes.pod_list`、
`logs.search`、`ticket.propose_create`。这些名称只是接口契约预留，当前
版本没有启用任何外部 Connector。
