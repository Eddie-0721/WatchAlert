package config

import (
	"github.com/spf13/viper"
	"strings"
	"testing"
)

func TestRuleEventIndexConfigurationIsOptIn(t *testing.T) {
	for _, tc := range []struct {
		yaml    string
		enabled bool
	}{{"Evaluation: {}", false}, {"Evaluation:\n  ruleEventIndex: true", true}} {
		v := viper.New()
		v.SetConfigType("yaml")
		if err := v.ReadConfig(strings.NewReader(tc.yaml)); err != nil {
			t.Fatal(err)
		}
		var app App
		if err := v.Unmarshal(&app); err != nil || app.Evaluation.RuleEventIndex != tc.enabled {
			t.Fatal(app.Evaluation, err)
		}
	}
	v := viper.New()
	v.SetConfigFile("config.yaml")
	if err := v.ReadInConfig(); err != nil {
		t.Fatal("repository sample YAML invalid", err)
	}
}
