package cloudwatch

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	cw "github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"math"
	"testing"
	"time"
)

func TestMetricDataCompleteness(t *testing.T) {
	for _, mode := range []string{"complete", "empty", "nil", "partial", "next_page", "message", "length", "nan", "inf"} {
		t.Run(mode, func(t *testing.T) {
			output := &cw.GetMetricDataOutput{MetricDataResults: []types.MetricDataResult{{StatusCode: types.StatusCodeComplete, Values: []float64{1}, Timestamps: []time.Time{time.Now()}}}}
			switch mode {
			case "empty":
				output.MetricDataResults = nil
			case "nil":
				output = nil
			case "partial":
				output.MetricDataResults[0].StatusCode = types.StatusCodePartialData
			case "next_page":
				output.NextToken = aws.String("next")
			case "message":
				output.Messages = []types.MessageData{{Code: aws.String("warning")}}
			case "length":
				output.MetricDataResults[0].Timestamps = nil
			case "nan":
				output.MetricDataResults[0].Values[0] = math.NaN()
			case "inf":
				output.MetricDataResults[0].Values[0] = math.Inf(1)
			}
			_, values, err := completeMetricData(output)
			if (err == nil) != (mode == "complete" || mode == "empty") {
				t.Fatal("unexpected result", err)
			}
			if mode == "complete" && len(values) != 1 {
				t.Fatal("lost samples")
			}
		})
	}
}
