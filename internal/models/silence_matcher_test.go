package models

import "testing"

func TestSilenceMatchersFailClosed(t *testing.T) {
	for _, labels := range [][]SilenceLabel{nil, {{Key: "env", Operator: "??", Value: "prod"}}, {{Key: "env", Operator: "=~", Value: "["}}, {{Key: "", Operator: "==", Value: "prod"}}} {
		if _, err := CompileSilenceMatchers(labels); err == nil {
			t.Fatalf("invalid matcher accepted: %#v", labels)
		}
	}
	for _, op := range []string{"=", "==", "!=", "=~", "!~"} {
		match, err := CompileSilenceMatchers([]SilenceLabel{{Key: "env", Operator: op, Value: "prod"}})
		if err != nil {
			t.Fatal(err)
		}
		if match(map[string]interface{}{}) || match(map[string]interface{}{"env": 1}) {
			t.Fatal("missing/non-string labels must not widen scope")
		}
		expected := op != "!=" && op != "!~"
		if match(map[string]interface{}{"env": "prod"}) != expected {
			t.Fatalf("wrong operator %s", op)
		}
	}
	match, _ := CompileSilenceMatchers([]SilenceLabel{{Key: "env", Operator: "==", Value: "prod"}, {Key: "service", Operator: "=~", Value: "^pay"}})
	if match(map[string]interface{}{"env": "prod", "service": "orders"}) || !match(map[string]interface{}{"env": "prod", "service": "payment"}) {
		t.Fatal("all conditions must match")
	}
}
