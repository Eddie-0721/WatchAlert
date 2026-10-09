package mute

import "watchAlert/internal/models"

type silenceMatch = func(map[string]interface{}) bool

type compiledSilence struct {
	labels []models.SilenceLabel
	match  silenceMatch
}

type silenceAnchor struct{ key, value string }

// Each rule is AND, the snapshot is OR. A positive equality condition is a
// necessary (not sufficient) condition for a rule to match. Index one such
// condition and still evaluate the full rule for every candidate. Never index
// regex or negative operators: missing/non-string labels are not matches.
// The immutable index lives only for this snapshot, never across consumer ticks.
func indexSnapshot(rules []compiledSilence) silenceMatch {
	linear := func(labels map[string]interface{}) bool {
		for _, rule := range rules {
			if rule.match(labels) {
				return true
			}
		}
		return false
	}
	// Small sets rarely amortize an index's preparation and lookup costs.
	if len(rules) <= 8 {
		return linear
	}
	frequencies := make(map[silenceAnchor]int)
	for _, rule := range rules {
		for _, condition := range rule.labels {
			if condition.Operator == "=" || condition.Operator == "==" {
				frequencies[silenceAnchor{condition.Key, condition.Value}]++
			}
		}
	}
	if len(frequencies) == 0 {
		return linear
	}
	index := make(map[string]map[string][]silenceMatch)
	var fallback []silenceMatch
	for _, rule := range rules {
		var anchor silenceAnchor
		least := 0
		for _, condition := range rule.labels {
			if condition.Operator != "=" && condition.Operator != "==" {
				continue
			}
			candidate := silenceAnchor{condition.Key, condition.Value}
			if count := frequencies[candidate]; least == 0 || count < least {
				anchor, least = candidate, count
			}
		}
		if least == 0 {
			fallback = append(fallback, rule.match)
			continue
		}
		if index[anchor.key] == nil {
			index[anchor.key] = make(map[string][]silenceMatch)
		}
		index[anchor.key][anchor.value] = append(index[anchor.key][anchor.value], rule.match)
	}
	return func(labels map[string]interface{}) bool {
		for _, match := range fallback {
			if match(labels) {
				return true
			}
		}
		// Use the smaller set of keys; a center with many unique anchor names
		// must not force every event to scan them all.
		if len(index) <= len(labels) {
			for key, values := range index {
				value, ok := labels[key].(string)
				if !ok {
					continue
				}
				for _, match := range values[value] {
					if match(labels) {
						return true
					}
				}
			}
		} else {
			for key, raw := range labels {
				value, ok := raw.(string)
				if !ok {
					continue
				}
				for _, match := range index[key][value] {
					if match(labels) {
						return true
					}
				}
			}
		}
		return false
	}
}
