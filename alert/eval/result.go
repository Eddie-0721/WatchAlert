package eval

// A successful empty result is different from missing or incomplete evidence.
// Until datasource-aware fingerprints are migrated, any incomplete datasource
// blocks recovery for the whole rule. Valid positive events may still be raised.
type evaluationResult struct {
	Fingerprints []string
	Status       string
	Reason       string
}

func completeEvaluation(fingerprints []string) evaluationResult {
	return evaluationResult{Fingerprints: fingerprints, Status: "complete"}
}

func failedEvaluation(reason string) evaluationResult {
	return evaluationResult{Status: "failed", Reason: reason}
}

func truncatedEvaluation(fingerprints []string) evaluationResult {
	return evaluationResult{Fingerprints: fingerprints, Status: "truncated", Reason: "incomplete_result"}
}

func combineEvaluations(results []evaluationResult) evaluationResult {
	combined := completeEvaluation(nil)
	if len(results) == 0 {
		return failedEvaluation("no_datasources")
	}
	for _, result := range results {
		combined.Fingerprints = append(combined.Fingerprints, result.Fingerprints...)
		if result.Status != "complete" {
			combined.Status = "failed"
			combined.Reason = "incomplete_datasource_evaluation"
		}
	}
	return combined
}
