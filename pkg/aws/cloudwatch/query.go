package cloudwatch

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
)

func MetricDataQuery(client *cloudwatch.Client, query CloudWatchQuery) ([]time.Time, []float64, error) {
	input := &cloudwatch.GetMetricDataInput{
		MetricDataQueries: []types.MetricDataQuery{
			{
				Id: aws.String("query"),
				MetricStat: &types.MetricStat{
					Metric: &types.Metric{
						Dimensions: []types.Dimension{
							{
								Name:  aws.String(query.Dimension),
								Value: aws.String(query.Endpoint),
							},
						},
						MetricName: aws.String(query.MetricName),
						Namespace:  aws.String(query.Namespace),
					},
					Stat:   aws.String(query.Statistic),
					Period: aws.Int32(query.Period),
				},
			},
		},
		StartTime: aws.Time(query.Form),
		EndTime:   aws.Time(query.To),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	output, err := client.GetMetricData(ctx, input)
	if err != nil {
		return nil, nil, err
	}
	return completeMetricData(output)
}

func completeMetricData(output *cloudwatch.GetMetricDataOutput) ([]time.Time, []float64, error) {
	if output == nil || aws.ToString(output.NextToken) != "" || len(output.Messages) > 0 {
		return nil, nil, fmt.Errorf("CloudWatch result incomplete")
	}

	var times []time.Time
	var values []float64
	for _, result := range output.MetricDataResults {
		if result.StatusCode != types.StatusCodeComplete || len(result.Messages) > 0 || len(result.Values) != len(result.Timestamps) {
			return nil, nil, fmt.Errorf("CloudWatch metric result incomplete")
		}
		for k, value := range result.Values {
			if math.IsNaN(value) || math.IsInf(value, 0) {
				return nil, nil, fmt.Errorf("CloudWatch metric contains invalid samples")
			}
			times = append(times, result.Timestamps[k])
			values = append(values, value)
		}
	}

	return times, values, nil
}
