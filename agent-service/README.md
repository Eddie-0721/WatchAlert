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
