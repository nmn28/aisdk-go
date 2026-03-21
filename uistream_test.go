package aisdk

import (
	"bytes"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestUIStreamPartJSON verifies each v6 event type produces correct JSON.
func TestUIStreamPartJSON(t *testing.T) {
	tests := []struct {
		name     string
		part     UIMessageStreamPart
		wantType string
		wantJSON string
	}{
		{
			name:     "start",
			part:     UIStartPart{MessageID: "msg_123"},
			wantType: "start",
			wantJSON: `{"type":"start","messageId":"msg_123"}`,
		},
		{
			name:     "finish",
			part:     UIFinishPart{FinishReason: FinishReasonStop},
			wantType: "finish",
			wantJSON: `{"type":"finish","finishReason":"stop"}`,
		},
		{
			name:     "text-start",
			part:     UITextStartPart{ID: "text_1"},
			wantType: "text-start",
			wantJSON: `{"type":"text-start","id":"text_1"}`,
		},
		{
			name:     "text-delta",
			part:     UITextDeltaPart{ID: "text_1", Delta: "Hello "},
			wantType: "text-delta",
			wantJSON: `{"type":"text-delta","id":"text_1","delta":"Hello "}`,
		},
		{
			name:     "text-end",
			part:     UITextEndPart{ID: "text_1"},
			wantType: "text-end",
			wantJSON: `{"type":"text-end","id":"text_1"}`,
		},
		{
			name:     "reasoning-start",
			part:     UIReasoningStartPart{ID: "r_1"},
			wantType: "reasoning-start",
			wantJSON: `{"type":"reasoning-start","id":"r_1"}`,
		},
		{
			name:     "reasoning-delta",
			part:     UIReasoningDeltaPart{ID: "r_1", Delta: "thinking..."},
			wantType: "reasoning-delta",
			wantJSON: `{"type":"reasoning-delta","id":"r_1","delta":"thinking..."}`,
		},
		{
			name:     "reasoning-end",
			part:     UIReasoningEndPart{ID: "r_1"},
			wantType: "reasoning-end",
			wantJSON: `{"type":"reasoning-end","id":"r_1"}`,
		},
		{
			name:     "tool-input-start",
			part:     UIToolInputStartPart{ToolCallID: "call_1", ToolName: "search"},
			wantType: "tool-input-start",
			wantJSON: `{"type":"tool-input-start","toolCallId":"call_1","toolName":"search"}`,
		},
		{
			name:     "tool-input-delta",
			part:     UIToolInputDeltaPart{ToolCallID: "call_1", InputTextDelta: `{"q":`},
			wantType: "tool-input-delta",
			wantJSON: `{"type":"tool-input-delta","toolCallId":"call_1","inputTextDelta":"{\"q\":"}`,
		},
		{
			name:     "tool-input-available",
			part:     UIToolInputAvailablePart{ToolCallID: "call_1", ToolName: "search", Input: map[string]any{"q": "test"}},
			wantType: "tool-input-available",
			wantJSON: `{"type":"tool-input-available","toolCallId":"call_1","toolName":"search","input":{"q":"test"}}`,
		},
		{
			name:     "tool-output-available",
			part:     UIToolOutputAvailablePart{ToolCallID: "call_1", Output: map[string]any{"result": "sunny"}},
			wantType: "tool-output-available",
			wantJSON: `{"type":"tool-output-available","toolCallId":"call_1","output":{"result":"sunny"}}`,
		},
		{
			name:     "source-url",
			part:     UISourceURLPart{SourceID: "src_1", URL: "https://example.com", Title: "Example"},
			wantType: "source-url",
			wantJSON: `{"type":"source-url","sourceId":"src_1","url":"https://example.com","title":"Example"}`,
		},
		{
			name:     "file",
			part:     UIFilePart{URL: "https://s3.example.com/file.pdf", MediaType: "application/pdf"},
			wantType: "file",
			wantJSON: `{"type":"file","url":"https://s3.example.com/file.pdf","mediaType":"application/pdf"}`,
		},
		{
			name:     "start-step",
			part:     UIStartStepPart{},
			wantType: "start-step",
			wantJSON: `{"type":"start-step"}`,
		},
		{
			name:     "error",
			part:     UIErrorPart{ErrorText: "rate limited"},
			wantType: "error",
			wantJSON: `{"type":"error","errorText":"rate limited"}`,
		},
		{
			name:     "done",
			part:     UIDonePart{},
			wantType: "[DONE]",
			wantJSON: `[DONE]`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.wantType, tt.part.UIStreamType())

			data, err := tt.part.UIStreamJSON()
			require.NoError(t, err)
			if tt.wantType == "[DONE]" {
				// [DONE] is not valid JSON — compare as raw string
				assert.Equal(t, tt.wantJSON, string(data))
			} else {
				assert.JSONEq(t, tt.wantJSON, string(data))
			}
		})
	}
}

// TestPipeUIMessageStream verifies SSE framing: "data: {json}\n\n"
func TestPipeUIMessageStream(t *testing.T) {
	stream := UIMessageStream(func(yield func(UIMessageStreamPart, error) bool) {
		yield(UIStartPart{MessageID: "msg_1"}, nil)
		yield(UITextStartPart{ID: "t_1"}, nil)
		yield(UITextDeltaPart{ID: "t_1", Delta: "Hi"}, nil)
		yield(UITextEndPart{ID: "t_1"}, nil)
		yield(UIFinishPart{FinishReason: FinishReasonStop}, nil)
		yield(UIDonePart{}, nil)
	})

	var buf bytes.Buffer
	err := stream.Pipe(&buf)
	require.NoError(t, err)

	output := buf.String()
	lines := strings.Split(strings.TrimSpace(output), "\n\n")

	require.Len(t, lines, 6)
	assert.Contains(t, lines[0], `data: {"type":"start"`)
	assert.Contains(t, lines[1], `data: {"type":"text-start"`)
	assert.Contains(t, lines[2], `data: {"type":"text-delta"`)
	assert.Contains(t, lines[3], `data: {"type":"text-end"`)
	assert.Contains(t, lines[4], `data: {"type":"finish"`)
	assert.Equal(t, `data: [DONE]`, lines[5])
}

// TestDataStreamToUIMessageStream_TextOnly verifies basic text conversion.
func TestDataStreamToUIMessageStream_TextOnly(t *testing.T) {
	ds := DataStream(func(yield func(DataStreamPart, error) bool) {
		yield(TextStreamPart{Content: "Hello "}, nil)
		yield(TextStreamPart{Content: "world"}, nil)
		yield(FinishMessageStreamPart{FinishReason: FinishReasonStop}, nil)
	})

	var events []string
	uiStream := DataStreamToUIMessageStream(ds, "msg_test")
	for part, err := range uiStream {
		require.NoError(t, err)
		events = append(events, part.UIStreamType())
	}

	assert.Equal(t, []string{
		"start",
		"text-start",
		"text-delta",    // "Hello "
		"text-delta",    // "world"
		"text-end",
		"finish",
		"[DONE]",
	}, events)
}

// TestDataStreamToUIMessageStream_Reasoning verifies reasoning blocks.
func TestDataStreamToUIMessageStream_Reasoning(t *testing.T) {
	ds := DataStream(func(yield func(DataStreamPart, error) bool) {
		yield(ReasoningStreamPart{Content: "Let me think..."}, nil)
		yield(ReasoningStreamPart{Content: " about this"}, nil)
		yield(TextStreamPart{Content: "The answer is 42"}, nil)
		yield(FinishMessageStreamPart{FinishReason: FinishReasonStop}, nil)
	})

	var events []string
	uiStream := DataStreamToUIMessageStream(ds, "msg_r")
	for part, err := range uiStream {
		require.NoError(t, err)
		events = append(events, part.UIStreamType())
	}

	assert.Equal(t, []string{
		"start",
		"reasoning-start",
		"reasoning-delta", // "Let me think..."
		"reasoning-delta", // " about this"
		"reasoning-end",   // Close reasoning before text
		"text-start",
		"text-delta",      // "The answer is 42"
		"text-end",
		"finish",
		"[DONE]",
	}, events)
}

// TestDataStreamToUIMessageStream_ToolCalling verifies tool call conversion.
func TestDataStreamToUIMessageStream_ToolCalling(t *testing.T) {
	ds := DataStream(func(yield func(DataStreamPart, error) bool) {
		yield(TextStreamPart{Content: "Let me search"}, nil)
		yield(ToolCallStartStreamPart{ToolCallID: "call_1", ToolName: "web_search"}, nil)
		yield(ToolCallDeltaStreamPart{ToolCallID: "call_1", ArgsTextDelta: `{"q"`}, nil)
		yield(ToolCallDeltaStreamPart{ToolCallID: "call_1", ArgsTextDelta: `:"test"}`}, nil)
		yield(ToolCallStreamPart{ToolCallID: "call_1", ToolName: "web_search", Args: map[string]any{"q": "test"}}, nil)
		yield(ToolResultStreamPart{ToolCallID: "call_1", Result: map[string]any{"data": "found"}}, nil)
		yield(TextStreamPart{Content: "I found it"}, nil)
		yield(FinishMessageStreamPart{FinishReason: FinishReasonStop}, nil)
	})

	var events []string
	var eventParts []UIMessageStreamPart
	uiStream := DataStreamToUIMessageStream(ds, "msg_tool")
	for part, err := range uiStream {
		require.NoError(t, err)
		events = append(events, part.UIStreamType())
		eventParts = append(eventParts, part)
	}

	assert.Equal(t, []string{
		"start",
		"text-start",
		"text-delta",           // "Let me search"
		"text-end",             // Close text before tool call
		"tool-input-start",
		"tool-input-delta",     // {"q"
		"tool-input-delta",     // :"test"}
		"tool-input-available", // Complete args
		"tool-output-available",
		"text-start",           // New text block
		"text-delta",           // "I found it"
		"text-end",
		"finish",
		"[DONE]",
	}, events)

	// Verify tool input mapping
	toolStart := eventParts[4].(UIToolInputStartPart)
	assert.Equal(t, "call_1", toolStart.ToolCallID)
	assert.Equal(t, "web_search", toolStart.ToolName)

	toolAvail := eventParts[7].(UIToolInputAvailablePart)
	assert.Equal(t, map[string]any{"q": "test"}, toolAvail.Input)

	toolOutput := eventParts[8].(UIToolOutputAvailablePart)
	assert.Equal(t, map[string]any{"data": "found"}, toolOutput.Output)
}

// TestDataStreamToUIMessageStream_Error verifies error handling.
func TestDataStreamToUIMessageStream_Error(t *testing.T) {
	ds := DataStream(func(yield func(DataStreamPart, error) bool) {
		yield(TextStreamPart{Content: "Starting"}, nil)
		yield(ErrorStreamPart{Content: "rate limited"}, nil)
	})

	var events []string
	uiStream := DataStreamToUIMessageStream(ds, "msg_err")
	for part, err := range uiStream {
		require.NoError(t, err)
		events = append(events, part.UIStreamType())
	}

	assert.Equal(t, []string{
		"start",
		"text-start",
		"text-delta",  // "Starting"
		"text-end",    // Close before error
		"error",       // "rate limited"
		"[DONE]",      // Terminate
	}, events)
}

// TestWriteUIMessageStreamHeaders verifies required HTTP headers.
func TestWriteUIMessageStreamHeaders(t *testing.T) {
	w := httptest.NewRecorder()
	WriteUIMessageStreamHeaders(w)

	assert.Equal(t, "text/event-stream", w.Header().Get("Content-Type"))
	assert.Equal(t, "v1", w.Header().Get("x-vercel-ai-ui-message-stream"))
	assert.Equal(t, "no-cache, no-transform", w.Header().Get("Cache-Control"))
	assert.Equal(t, "no", w.Header().Get("X-Accel-Buffering"))
	assert.Equal(t, 200, w.Code)
}

// TestWriteEndureStreamHeaders verifies custom Endure headers.
func TestWriteEndureStreamHeaders(t *testing.T) {
	w := httptest.NewRecorder()
	WriteEndureStreamHeaders(w, EndureStreamOptions{
		ThreadID:  "thread_abc",
		StreamID:  "stream_xyz",
		MessageID: "msg_123",
	})

	assert.Equal(t, "text/event-stream", w.Header().Get("Content-Type"))
	assert.Equal(t, "v1", w.Header().Get("x-vercel-ai-ui-message-stream"))
	assert.Equal(t, "thread_abc", w.Header().Get("X-Thread-Id"))
	assert.Equal(t, "stream_xyz", w.Header().Get("X-Stream-Id"))
	assert.Equal(t, "msg_123", w.Header().Get("X-Message-Id"))
}

// TestWithTitleInjection verifies title is injected before finish/done.
func TestWithTitleInjection(t *testing.T) {
	stream := UIMessageStream(func(yield func(UIMessageStreamPart, error) bool) {
		yield(UIStartPart{MessageID: "msg_1"}, nil)
		yield(UITextStartPart{ID: "t_1"}, nil)
		yield(UITextDeltaPart{ID: "t_1", Delta: "Hello"}, nil)
		yield(UITextEndPart{ID: "t_1"}, nil)
		yield(UIFinishPart{FinishReason: FinishReasonStop}, nil)
		yield(UIDonePart{}, nil)
	})

	withTitle := WithTitleInjection(stream, func() (string, string) {
		return "Chat about weather", "thread_abc"
	})

	var events []string
	for part, err := range withTitle {
		require.NoError(t, err)
		events = append(events, part.UIStreamType())
	}

	assert.Equal(t, []string{
		"start",
		"text-start",
		"text-delta",
		"text-end",
		"endure-title", // Title injected before finish
		"finish",
		"[DONE]",
	}, events)
}

// TestUITitlePartFormatRaw verifies backwards-compatible t: format.
func TestUITitlePartFormatRaw(t *testing.T) {
	tp := UITitlePart{Title: "My Chat", ThreadID: "abc123"}
	raw := tp.FormatTitleEvent()
	assert.Equal(t, `t:{"title":"My Chat","threadId":"abc123"}`+"\n", raw)
}

// TestPipeEndureStream_FullIntegration is an end-to-end test simulating
// what endureapigo would do.
func TestPipeEndureStream_FullIntegration(t *testing.T) {
	ds := DataStream(func(yield func(DataStreamPart, error) bool) {
		yield(StartStepStreamPart{MessageID: "msg_1"}, nil)
		yield(TextStreamPart{Content: "The weather is "}, nil)
		yield(TextStreamPart{Content: "sunny"}, nil)
		yield(FinishStepStreamPart{
			FinishReason: FinishReasonStop,
			Usage:        Usage{PromptTokens: ptr(int64(10)), CompletionTokens: ptr(int64(5))},
		}, nil)
		yield(FinishMessageStreamPart{
			FinishReason: FinishReasonStop,
			Usage:        Usage{PromptTokens: ptr(int64(10)), CompletionTokens: ptr(int64(5))},
		}, nil)
	})

	w := httptest.NewRecorder()
	err := PipeEndureStream(w, ds, EndureStreamOptions{
		ThreadID:  "thread_123",
		StreamID:  "stream_456",
		MessageID: "msg_1",
	}, func() (string, string) {
		return "Weather chat", "thread_123"
	})
	require.NoError(t, err)

	// Verify headers
	assert.Equal(t, "text/event-stream", w.Header().Get("Content-Type"))
	assert.Equal(t, "v1", w.Header().Get("x-vercel-ai-ui-message-stream"))
	assert.Equal(t, "thread_123", w.Header().Get("X-Thread-Id"))
	assert.Equal(t, "stream_456", w.Header().Get("X-Stream-Id"))

	// Verify SSE body
	body := w.Body.String()
	lines := strings.Split(strings.TrimSpace(body), "\n\n")

	// Should have: start, start-step, text-start, text-delta x2, text-end, finish-step, endure-title, finish, [DONE]
	assert.GreaterOrEqual(t, len(lines), 8)
	assert.Contains(t, lines[0], `"type":"start"`)
	assert.Contains(t, body, `"type":"text-delta"`)
	assert.Contains(t, body, `"type":"text-start"`)
	assert.Contains(t, body, `"type":"text-end"`)
	assert.Contains(t, body, `"type":"endure-title"`)
	assert.Contains(t, body, `"Weather chat"`)
	assert.True(t, strings.HasSuffix(strings.TrimSpace(body), "data: [DONE]"))
}

func ptr[T any](v T) *T {
	return &v
}
