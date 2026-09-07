package models

import (
	"fmt"
	"regexp"
)

// Compile once per scan. Preview and notification suppression share semantics.
func CompileSilenceMatchers(matchers []SilenceLabel) (func(map[string]interface{}) bool, error) {
	if len(matchers) == 0 || len(matchers) > 50 {
		return nil, fmt.Errorf("静默需要 1 至 50 个匹配条件")
	}
	tests := make([]func(map[string]interface{}) bool, 0, len(matchers))
	for _, matcher := range matchers {
		m := matcher
		if m.Key == "" || m.Value == "" {
			return nil, fmt.Errorf("Label 名称和匹配值不能为空")
		}
		var re *regexp.Regexp
		switch m.Operator {
		case "=", "==", "!=":
		case "=~", "!~":
			var err error
			re, err = regexp.Compile(m.Value)
			if err != nil {
				return nil, fmt.Errorf("Label %s 的正则无效", m.Key)
			}
		default:
			return nil, fmt.Errorf("不支持的匹配操作符")
		}
		tests = append(tests, func(values map[string]interface{}) bool {
			value, ok := values[m.Key].(string)
			if !ok {
				return false
			}
			switch m.Operator {
			case "=", "==":
				return value == m.Value
			case "!=":
				return value != m.Value
			case "=~":
				return re.MatchString(value)
			case "!~":
				return !re.MatchString(value)
			}
			return false
		})
	}
	return func(values map[string]interface{}) bool {
		for _, test := range tests {
			if !test(values) {
				return false
			}
		}
		return true
	}, nil
}
