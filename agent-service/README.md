# WatchAlert Agent Service

这是隔离的单 Agent 服务。它不连接 WatchAlert 数据库，也不保存 Prometheus、Kubernetes 或其他外部系统凭证。

必须通过环境变量配置：

```text
WATCHALERT_AGENT_INTERNAL_TOKEN=<与 Go 配置 Agent.internalToken 相同的随机密钥>
WATCHALERT_TOOL_GATEWAY_URL=http://w8t-service:9001
OPENAI_API_KEY=<模型供应商密钥>
OPENAI_BASE_URL=<可选，OpenAI-compatible 地址>
AGENT_MODEL=<支持 Tool Calling 的模型名>
```

构建时默认使用清华 PyPI 镜像；如使用公司镜像：

```bash
docker build --build-arg PIP_INDEX_URL=https://<your-pypi-mirror>/simple -t watchalert-agent:dev .
```

服务仅应暴露给 WatchAlert Go 后端所在的内部网络，不应向公网暴露 8080。

`POST /v1/runs/stream` 使用 SSE 输出 `status`、`delta`、`done` 事件。浏览器仍只
访问 Go BFF；模型密钥、Tool Gateway 令牌和 Tool 原始结果均不会下发到浏览器。

Copilot 的数据源和环境范围由 WatchAlert 系统设置中的 `agentConfig.scope`
控制，范围会签入短期运行令牌并由 Go Tool Gateway 强制校验。
