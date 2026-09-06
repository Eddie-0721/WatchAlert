package services

import (
	"fmt"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
	"strings"
	"watchAlert/internal/models"
	"watchAlert/internal/types"
	"watchAlert/pkg/agenttoken"
)

// A run may only retain access shared by its original token and today's policy.
func restrictAgentClaims(claims agenttoken.Claims, scope types.AgentScope) (agenttoken.Claims, error) {
	intersect := func(old, current []string) ([]string, error) {
		if len(old) == 0 {
			return current, nil
		}
		if len(current) == 0 {
			return old, nil
		}
		var result []string
		for _, value := range old {
			if containsTool(current, value) {
				result = append(result, value)
			}
		}
		if len(result) == 0 {
			return nil, fmt.Errorf("Copilot 数据范围已变更，请重新发起分析")
		}
		return result, nil
	}
	var err error
	claims.DatasourceIds, err = intersect(claims.DatasourceIds, scope.DatasourceIds)
	if err != nil {
		return claims, err
	}
	if len(scope.Environments) > 0 {
		if scope.EnvironmentLabelKey == "" {
			return claims, fmt.Errorf("环境范围缺少 Label 配置")
		}
		if len(claims.Environments) > 0 && claims.EnvironmentLabelKey != scope.EnvironmentLabelKey {
			return claims, fmt.Errorf("环境 Label 配置已变更，请重新发起分析")
		}
		claims.EnvironmentLabelKey = scope.EnvironmentLabelKey
	}
	claims.Environments, err = intersect(claims.Environments, scope.Environments)
	return claims, err
}

// Do not infer query isolation from labels attached to a datasource connection.
// Every selector, including those nested in joins and subqueries, must carry an
// explicit allowed environment equality. Regex/negative-only scopes fail closed.
func validateAgentPromQL(query string, claims agenttoken.Claims) error {
	expr, err := parser.ParseExpr(query)
	if err != nil {
		return fmt.Errorf("PromQL 校验失败: %w", err)
	}
	if len(claims.Environments) == 0 {
		return nil
	}
	if claims.EnvironmentLabelKey == "" {
		return fmt.Errorf("环境范围缺少 Label 配置")
	}
	return parser.Walk(agentScopeVisitor{claims: claims}, expr, nil)
}

type agentScopeVisitor struct{ claims agenttoken.Claims }

func (v agentScopeVisitor) Visit(node parser.Node, _ []parser.Node) (parser.Visitor, error) {
	if selector, ok := node.(*parser.VectorSelector); ok {
		allowed := false
		for _, matcher := range selector.LabelMatchers {
			if matcher.Name == v.claims.EnvironmentLabelKey && matcher.Type == labels.MatchEqual && matcher.Value != "" && containsTool(v.claims.Environments, matcher.Value) {
				allowed = true
			}
		}
		if !allowed {
			return nil, fmt.Errorf("每个指标选择器必须包含允许环境的精确条件 %s=\"环境值\"", v.claims.EnvironmentLabelKey)
		}
	}
	return v, nil
}

func validateAgentWritePolicy(config models.AgentConfig, capabilities types.AgentCapabilities, tool string) error {
	if !config.GetEnable() || !capabilities.Enabled || !containsTool(capabilities.AllowedTools, tool) {
		return fmt.Errorf("Copilot 已停用或当前用户不具备该操作权限")
	}
	if (config.AllowProductionWrite == nil || !*config.AllowProductionWrite) && agentScopeContainsProduction(capabilities.Scope) {
		return fmt.Errorf("当前范围包含生产或未明确限定为非生产环境，需管理员允许生产写操作")
	}
	return nil
}

func isExplicitNonProduction(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "dev", "development", "test", "testing", "sit", "uat", "qa", "staging", "stage", "pre", "preprod", "preproduction", "sandbox", "local", "开发", "测试", "预发布":
		return true
	default:
		return false
	}
}
