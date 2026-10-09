package services

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
	"watchAlert/config"
	"watchAlert/internal/types"
)

func agentTransportServer(t testing.TB, handler http.HandlerFunc) {
	t.Helper()
	server := httptest.NewServer(handler)
	original := config.Application.Agent
	config.Application.Agent.URL = server.URL
	config.Application.Agent.InternalToken = "test-only-token"
	t.Cleanup(func() { config.Application.Agent = original; server.Close() })
}

func TestAgentTransportNormalEventsAndFailures(t *testing.T) {
	for _, tail := range []string{"\n\n", ""} {
		stream := ": keepalive\n\nevent: status\ndata: {\"message\":\"分析中\"}\n\n" +
			"event: delta\ndata: {\"delta\":\"告警\"}\n\n" +
			"event: done\ndata: {\"content\":\"分析完成\",\ndata: \"evidence\":\"[]\"}" + tail
		var emitted []types.AgentStreamEvent
		result, err := readAgentEventStream(context.Background(), strings.NewReader(stream), func(event types.AgentStreamEvent) { emitted = append(emitted, event) })
		if err != nil || result.Content != "分析完成" || result.Evidence != "[]" || len(emitted) != 3 || emitted[0].Type != "status" || emitted[1].Delta != "告警" || emitted[2].Type != "done" {
			t.Fatal("normal stream changed", result, emitted, err)
		}
	}
	for _, stream := range []string{
		"event: error\ndata: {\"message\":\"failed\"}\n\n",
		"event: done\ndata: invalid\n\n",
		"event: delta\ndata: {\"delta\":\"partial\"}\n\n",
		"event: done\ndata: {\"content\":\"\"}\n\n",
		"data: " + strings.Repeat("x", maxAgentResponseBytes),
	} {
		if _, err := readAgentEventStream(context.Background(), strings.NewReader(stream), func(types.AgentStreamEvent) {}); err == nil {
			t.Fatal("failed/partial/oversized-line stream accepted")
		}
	}
}

func TestAgentTransportCompleteStreamBudget(t *testing.T) {
	// Individually small comments/events cannot bypass the total wire budget.
	for _, frame := range []string{": keepalive\n\n", "event: status\ndata: {}\n\n"} {
		stream := strings.Repeat(frame, maxAgentStreamBytes/len(frame)+1) + "event: done\ndata: {\"content\":\"done\"}\n\n"
		done := false
		_, err := readAgentEventStream(context.Background(), strings.NewReader(stream), func(event types.AgentStreamEvent) { done = done || event.Type == "done" })
		if err == nil || !strings.Contains(err.Error(), "8 MiB") || done {
			t.Fatal("small frames bypassed total budget", err, done)
		}
	}
	for _, size := range []int{maxAgentStreamBytes, maxAgentStreamBytes + 1} {
		done := "event: done\ndata: {\"content\":\"done\"}\n\n"
		remaining := size - len(done)
		stream := done + strings.Repeat(":\n", remaining/2) + strings.Repeat(":", remaining%2)
		result, err := readAgentEventStream(context.Background(), strings.NewReader(stream), func(types.AgentStreamEvent) {})
		if size == maxAgentStreamBytes && (err != nil || result.Content != "done") {
			t.Fatal("exact wire boundary was rejected", err)
		}
		if size > maxAgentStreamBytes && (err == nil || !strings.Contains(err.Error(), "8 MiB")) {
			t.Fatal("trailing bytes after done bypassed total budget", err)
		}
	}
}

func TestAgentTransportAssembledEventBoundary(t *testing.T) {
	// The JSON envelope counts toward the event budget, not only content.
	size := maxAgentResponseBytes - len(`{"content":""}`)
	result, err := readAgentEventStream(context.Background(), strings.NewReader(agentMultilineFixture(size, 256)), func(types.AgentStreamEvent) {})
	if err != nil || len(result.Content) != size {
		t.Fatal("exact assembled event boundary was rejected", err)
	}
}

func TestAgentTransportNormalHTTPResponse(t *testing.T) {
	agentTransportServer(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-WatchAlert-Agent-Token") != "test-only-token" || r.Method != http.MethodPost || r.URL.Path != "/v1/runs" {
			t.Error("internal transport contract changed")
		}
		fmt.Fprint(w, `{"content":"正常结果","evidence":"[]"}`)
	})
	result, err := callAgentService(context.Background(), types.AgentRunRequest{})
	if err != nil || result.Content != "正常结果" || result.Evidence != "[]" {
		t.Fatal(result, err)
	}
}

func TestAgentTransportResponseBodyBoundary(t *testing.T) {
	for _, n := range []int{0, maxAgentResponseBytes - 1, maxAgentResponseBytes, maxAgentResponseBytes + 1} {
		body, err := readAgentResponseBody(strings.NewReader(strings.Repeat("x", n)))
		if n <= maxAgentResponseBytes {
			if err != nil || len(body) != n {
				t.Fatal("in-budget body changed", n, err)
			}
		} else if err == nil || body != nil {
			t.Fatal("truncated body exposed as valid", n, err)
		}
	}
}

type failOnRead struct{ t *testing.T }

func (r failOnRead) Read([]byte) (int, error) {
	r.t.Fatal("canceled request read data")
	return 0, io.EOF
}

func TestAgentTransportCanceledBeforeAndDuringBufferedStream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := readAgentEventStream(ctx, failOnRead{t}, func(types.AgentStreamEvent) { t.Fatal("unexpected event") }); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	count := 0
	_, err := readAgentEventStream(ctx, strings.NewReader("event: delta\ndata: {\"delta\":\"partial\"}\n\nevent: done\ndata: {\"content\":\"done\"}\n\n"), func(types.AgentStreamEvent) { count++; cancel() })
	if !errors.Is(err, context.Canceled) || count != 1 {
		t.Fatal("buffered events kept running after cancellation", count, err)
	}
}

func TestAgentTransportHTTPDisconnectCancelsUpstream(t *testing.T) {
	started, closed := make(chan struct{}), make(chan struct{})
	agentTransportServer(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "event: status\ndata: {}\n\n")
		w.(http.Flusher).Flush()
		close(started)
		<-r.Context().Done()
		close(closed)
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan error, 1)
	go func() {
		_, err := callAgentServiceStream(ctx, types.AgentRunRequest{}, func(types.AgentStreamEvent) {})
		finished <- err
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("server did not start")
	}
	cancel()
	select {
	case err := <-finished:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("client kept running")
	}
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("upstream request not canceled")
	}
}

// Valid internal multi-line fixture, deliberately split inside the JSON string
// to preserve and measure the existing concatenation contract, not change it.
func agentMultilineFixture(size, fragment int) string {
	var stream strings.Builder
	stream.WriteString("event: done\ndata: {\"content\":\"")
	for i := 0; i < size; i += fragment {
		stream.WriteString("\ndata: ")
		stream.WriteString(strings.Repeat("x", min(fragment, size-i)))
	}
	stream.WriteString("\ndata: \"}\n\n")
	return stream.String()
}

func BenchmarkAgentStreamAssembly(b *testing.B) {
	stream := agentMultilineFixture(256<<10, 256)
	for _, legacy := range []bool{true, false} {
		b.Run(fmt.Sprintf("legacy=%t", legacy), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if legacy {
					// Old parser's repeated immutable concatenation, then decode.
					scanner := bufio.NewScanner(strings.NewReader(stream))
					scanner.Buffer(make([]byte, 4096), 2<<20)
					data := ""
					for scanner.Scan() {
						line := scanner.Text()
						if strings.HasPrefix(line, "data:") {
							data += strings.TrimSpace(strings.TrimPrefix(line, "data:"))
						}
					}
					var event types.AgentStreamEvent
					if err := json.Unmarshal([]byte(data), &event); err != nil || len(event.Content) != 256<<10 {
						b.Fatal(err)
					}
				} else {
					result, err := readAgentEventStream(context.Background(), strings.NewReader(stream), func(types.AgentStreamEvent) {})
					if err != nil || len(result.Content) != 256<<10 {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

func TestAgentTransportRejectsOversizedMultilineEvent(t *testing.T) {
	const size = 2<<20 + 100
	agentTransportServer(t, func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "event: done")
		fmt.Fprintln(w, `data: {"content":"`)
		for i := 0; i < size/100; i++ {
			fmt.Fprintln(w, "data: "+strings.Repeat("x", 100))
		}
		fmt.Fprint(w, "data: \"}\n\n")
	})
	done := false
	_, err := callAgentServiceStream(context.Background(), types.AgentRunRequest{}, func(event types.AgentStreamEvent) { done = done || event.Type == "done" })
	if err == nil || done {
		t.Fatal("oversized assembled event was accepted as done", err)
	}
}

func TestAgentTransportRejectsOversizedJSONBody(t *testing.T) {
	agentTransportServer(t, func(w http.ResponseWriter, r *http.Request) {
		// Previously the first 2 MiB was valid JSON + whitespace, hiding the
		// excess rather than reporting that the response had been truncated.
		fmt.Fprint(w, `{"content":"answer"}`+strings.Repeat(" ", 2<<20))
	})
	if _, err := callAgentService(context.Background(), types.AgentRunRequest{}); err == nil {
		t.Fatal("oversized response accepted after silent truncation")
	}
}
