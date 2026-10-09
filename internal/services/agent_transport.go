package services

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"watchAlert/internal/types"
)

const (
	maxAgentResponseBytes = 2 << 20
	maxAgentStreamBytes   = 8 << 20
)

func readAgentResponseBody(reader io.Reader) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(reader, maxAgentResponseBytes+1))
	if err != nil {
		return nil, err
	}
	if len(body) > maxAgentResponseBytes {
		return nil, fmt.Errorf("Copilot Agent 响应超过 2 MiB 限制，请缩小分析范围")
	}
	return body, nil
}

// Bound both an assembled event and the complete wire stream. Scanner's line
// limit alone does not constrain multi-line events or endless small frames.
func readAgentEventStream(ctx context.Context, reader io.Reader, emit func(types.AgentStreamEvent)) (types.AgentRunResponse, error) {
	var result types.AgentRunResponse
	if err := ctx.Err(); err != nil {
		return result, err
	}
	limited := &io.LimitedReader{R: reader, N: maxAgentStreamBytes + 1}
	scanner := bufio.NewScanner(limited)
	scanner.Buffer(make([]byte, 4096), maxAgentResponseBytes)
	eventType := "message"
	// Normal Agent frames have one data line: retain it directly and allocate
	// the joining buffer only when another nonempty fragment arrives.
	firstData := ""
	var data strings.Builder
	dispatch := func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		eventData := firstData
		if data.Len() > 0 {
			eventData = data.String()
		}
		if eventData == "" {
			return nil
		}
		var event types.AgentStreamEvent
		if err := json.Unmarshal([]byte(eventData), &event); err != nil {
			return fmt.Errorf("解析 Copilot 流事件失败: %w", err)
		}
		event.Type = eventType
		switch eventType {
		case "delta", "status":
			emit(event)
		case "done":
			result.Content, result.Evidence = event.Content, event.Evidence
			emit(event)
		case "error":
			return fmt.Errorf("Copilot Agent 运行失败: %s", event.Message)
		}
		return nil
	}
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if limited.N == 0 {
			return result, fmt.Errorf("Copilot Agent 流响应超过 8 MiB 限制，请缩小分析范围")
		}
		line := scanner.Text()
		if line == "" {
			if err := dispatch(); err != nil {
				return result, err
			}
			eventType = "message"
			firstData = ""
			data.Reset()
			continue
		}
		if strings.HasPrefix(line, "event:") {
			eventType = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		} else if strings.HasPrefix(line, "data:") {
			part := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			length := len(firstData)
			if data.Len() > 0 {
				length = data.Len()
			}
			if len(part) > maxAgentResponseBytes-length {
				return result, fmt.Errorf("Copilot Agent 流事件超过 2 MiB 限制，请缩小分析范围")
			}
			if part == "" {
				continue
			}
			if firstData == "" {
				firstData = part
				continue
			}
			// Preserve the existing internal protocol's concatenation semantics;
			// Builder avoids copying all previous fragments on each data line.
			if data.Len() == 0 {
				data.WriteString(firstData)
			}
			data.WriteString(part)
		}
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if limited.N == 0 {
		return result, fmt.Errorf("Copilot Agent 流响应超过 8 MiB 限制，请缩小分析范围")
	}
	if err := scanner.Err(); err != nil {
		return result, err
	}
	if err := dispatch(); err != nil {
		return result, err
	}
	if result.Content == "" {
		return result, fmt.Errorf("Copilot Agent 流服务提前结束")
	}
	return result, nil
}
