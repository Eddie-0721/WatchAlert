# Agent 元数据读取的字段投影

日期：2026-10-10。后端基线 `21713fc`，仅后端变更。

## 问题与实现

`prometheus.datasources` 原来读取全部数据源字段，包括 HTTP、Auth、云厂商配置、KubeConfig 等，随后只返回 id/name/type/labels/description/enabled。`prometheus.rule_query` 先读取完整规则，实际只返回规则名、数据源引用、PromQL 和阈值规则。

- 数据源仓库新增 `ListSummariesContext`，SQL 只选择上述 6 个展示字段，仍按租户和 Prometheus 类型查询。明确拒绝空/null/undefined 租户；返回的局部模型不能用于实际连接。
- Agent 仍按授权 datasourceIds 过滤结果，不把数据源描述性环境标签当作共享连接的访问控制。启用 true/false/nil 的展示行为不变。
- PromQL 工具只选择 rule_id、rule_name、datasource_id_list、prometheus_config。保留租户限定、缺失规则错误及 10 秒数据库预算；不使用数据库特有 JSON 提取语法。
- 真正查询 Prometheus 时仍使用 `GetForTenantContext` 读取和校验完整连接；完整规则详情仍读取完整模型。前端、接口字段、权限范围、模型配置均未改。
- 未使用配置损坏不再使纯元数据读取失败；实际连接/完整详情仍能暴露对应错误。这是取消无关解码的直接结果，不表示连接已通过健康检查。

## 对照测量

Windows / Go 1.24.11 / Ryzen 7 7700 / 内存 SQLite，三轮各一秒。20/200 个合成 Prometheus 数据源，每个包含相同两项标签、测试 URL、128 字节测试密码及 Header；未填充巨大的 KubeConfig 等无关字段。比较仍保留的完整列表入口与新摘要入口，不计初始化数据，不包含 HTTP/模型。

| 数据源数 | 原完整读取耗时 | 摘要读取耗时 | 原临时分配/次 | 摘要临时分配/次 |
|---|---:|---:|---:|---:|
| 20 | 0.199–0.201 ms | 0.062–0.066 ms | 122.6 KB | 40.3 KB |
| 200 | 1.854–1.880 ms | 0.521–0.529 ms | 1.341 MB | 0.524 MB |

累计临时分配降低约 61%–67%，不是进程常驻内存或生产总响应时间下降比例。数据源摘要仍读取当前租户所有 Prometheus 元数据，然后按权限 ID 过滤；没有在本批引入分页、索引、跨请求缓存或结果截断。

## 验证

- 摘要与完整查询所需字段逐项一致，SQL 不包含凭据/连接列；两租户及不同数据源类型不混入。
- Auth 故意置为无效 JSON 后，摘要仍可读，实际连接查询报错；不是把错误数据源标成健康。
- PromQL 工具结果与完整规则的同字段构造结果一致；无关 Loki JSON 损坏不阻止 PromQL 读取，完整详情仍报错。
- 授权 ID 过滤、空结果、启用 true/false/nil、共享连接描述标签不改变授权语义。
- 空规则 ID、跨租户规则、取消、池满时 30ms 请求截止；新入口专项与相关取消测试连续 10 次通过。
- 后端相关包回归、静态检查及构建通过。

```powershell
go test ./internal/repo ./internal/services -run '^Test(DatasourceSummary|AgentRulePromQLProjection|AgentDatasourceSummaryContract|AgentToolReadContexts)' -count=10 -timeout 90s
go test ./internal/repo -run '^$' -bench '^BenchmarkDatasourceMetadataRead$' -benchmem -benchtime=1s -count=3
```

## 发布与范围

仅需后端镜像，无配置、依赖、表结构或协议变更；可回滚前版本。前端、独立 Agent 未改，未运行新增浏览器验收、生产部署或真实模型调用。生产 SQL 计划、容量、审计序列化、Redis 在途取消和其他总体待办仍未完成。
