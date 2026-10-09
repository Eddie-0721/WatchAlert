package config

import (
	"github.com/spf13/viper"
	"strings"
	"testing"
)

func TestProbeConcurrencyConfiguration(t *testing.T) {
	for _, tc := range []struct {
		yaml string
		want int
	}{{"Probing: {}", 0}, {"Probing:\n  maxConcurrentRuns: 12", 12}} {
		v := viper.New()
		v.SetConfigType("yaml")
		if err := v.ReadConfig(strings.NewReader(tc.yaml)); err != nil {
			t.Fatal(err)
		}
		var app App
		if err := v.Unmarshal(&app); err != nil || app.Probing.MaxConcurrentRuns != tc.want {
			t.Fatal(app.Probing, err)
		}
	}
}
