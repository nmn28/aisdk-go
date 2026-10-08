package aisdk_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/option"
	"github.com/anthropics/anthropic-sdk-go/packages/ssestream"
	"github.com/nmn28/aisdk-go"
	"github.com/stretchr/testify/require"
)

func TestAnthropicToDataStream(t *testing.T) {
	t.Parallel()

	// anthropicResponses are hardcoded responses from the Anthropic API Stream endpoint.
	anthropicResponses := `event: message_start
data: {"type":"message_start","message":{"id":"msg_01LHXQM4FBxykQGT7N1a7kJ7","type":"message","role":"assistant","model":"claude-3-5-sonnet-20241022","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":408,"cache_creation_input_tokens":0,"cache_read_input_tokens":0,"output_tokens":1}}        }

event: content_block_start
data: {"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}      }

event: ping
data: {"type": "ping"}

event: content_block_delta
data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"I"}    }

event: content_block_delta
data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"'ll help you print 'hello world' to the console"}              }

event: content_block_delta
data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":" using the print function."}      }

event: content_block_stop
data: {"type":"content_block_stop","index":0  }

event: content_block_start
data: {"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"toolu_01RA76iwg1LbKuDjJnc6ym45","name":"print","input":{}}            }

event: content_block_delta
data: {"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":""}    }

event: content_block_delta
data: {"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"message\""}    }

event: content_block_delta
data: {"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":": \"hel"}   }

event: content_block_delta
data: {"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"lo worl"}}

event: content_block_delta
data: {"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"d\"}"}       }

event: content_block_stop
data: {"type":"content_block_stop","index":1         }

event: message_delta
data: {"type":"message_delta","delta":{"stop_reason":"tool_use","stop_sequence":null},"usage":{"output_tokens":71}             }

event: message_stop
data: {"type":"message_stop" }`

	decoder := ssestream.NewDecoder(&http.Response{
		Body: io.NopCloser(strings.NewReader(anthropicResponses)),
	})
	typedStream := ssestream.NewStream[anthropic.MessageStreamEventUnion](decoder, nil)

	var acc aisdk.DataStreamAccumulator
	stream := aisdk.AnthropicToDataStream(typedStream)
	stream = stream.WithToolCalling(func(toolCall aisdk.ToolCall) any {
		return map[string]any{"message": "Message printed to the console"}
	})
	stream = stream.WithAccumulator(&acc)
	for _, err := range stream {
		require.NoError(t, err)
	}

	expectedMessages := []aisdk.Message{
		{
			ID:      "msg_01LHXQM4FBxykQGT7N1a7kJ7",
			Role:    "assistant",
			Content: "I'll help you print 'hello world' to the console using the print function.",
			Parts: []aisdk.Part{{
				Type: aisdk.PartTypeStepStart,
			}, {
				Type: aisdk.PartTypeText,
				Text: "I'll help you print 'hello world' to the console using the print function.",
			}, {
				Type: aisdk.PartTypeToolInvocation,
				ToolInvocation: &aisdk.ToolInvocation{
					State:      aisdk.ToolInvocationStateResult,
					ToolCallID: "toolu_01RA76iwg1LbKuDjJnc6ym45",
					ToolName:   "print",
					Args:       map[string]any{"message": "hello world"},
					Result:     map[string]any{"message": "Message printed to the console"},
				},
			}},
		},
	}

	require.EqualExportedValues(t, expectedMessages, acc.Messages())

	// --- Add conversion back check ---
	anthropicMsgs, systemPrompts, err := aisdk.MessagesToAnthropic(acc.Messages())
	require.NoError(t, err)

	// Verify the converted Anthropic messages match expectations
	// We now expect TWO messages: assistant (text + tool_use) and user (tool_result)
	require.Empty(t, systemPrompts)
	require.Len(t, anthropicMsgs, 2)

	// Check Assistant Message (Call)
	assistantMsg := anthropicMsgs[0]
	require.Equal(t, anthropic.MessageParamRoleAssistant, assistantMsg.Role)
	require.Len(t, assistantMsg.Content, 2) // Text block + ToolUse block

	// Check Text Content Block
	textBlock := assistantMsg.Content[0].OfText
	require.NotNil(t, textBlock)
	require.Equal(t, "I'll help you print 'hello world' to the console using the print function.", textBlock.Text)

	// Check Tool Use Content Block
	toolUseBlock := assistantMsg.Content[1].OfToolUse
	require.NotNil(t, toolUseBlock)
	require.Equal(t, "toolu_01RA76iwg1LbKuDjJnc6ym45", toolUseBlock.ID)
	require.Equal(t, "print", toolUseBlock.Name)
	require.JSONEq(t, `{"message": "hello world"}`, string(toolUseBlock.Input.(json.RawMessage)))

	// Check User Message (Result) - This message is now generated by the first conversion
	userMsg := anthropicMsgs[1]
	require.Equal(t, anthropic.MessageParamRoleUser, userMsg.Role)
	require.Len(t, userMsg.Content, 1) // ToolResult block

	toolResultBlock := userMsg.Content[0].OfToolResult
	require.NotNil(t, toolResultBlock)
	require.Equal(t, "toolu_01RA76iwg1LbKuDjJnc6ym45", toolResultBlock.ToolUseID)
	require.Len(t, toolResultBlock.Content, 1)
	require.NotNil(t, toolResultBlock.Content[0].OfText)
	require.JSONEq(t, `{"message":"Message printed to the console"}`, toolResultBlock.Content[0].OfText.Text)

	// --- Second conversion check (using expectedMessages) ---
	// This part should remain the same, as it also expects 2 messages now.
	anthropicMsgsWithResult, systemPromptsWithResult, err := aisdk.MessagesToAnthropic(expectedMessages)
	require.NoError(t, err)
	require.Empty(t, systemPromptsWithResult)
	require.Len(t, anthropicMsgsWithResult, 2) // Expect assistant call + user result

	// Check Assistant Message (unchanged from above)
	assistantMsgWithResult := anthropicMsgsWithResult[0]
	require.Equal(t, anthropic.MessageParamRoleAssistant, assistantMsgWithResult.Role)
	require.Len(t, assistantMsgWithResult.Content, 2) // Text block + ToolUse block (same as before)

	// Check User Message (Tool Result)
	userMsgWithResult := anthropicMsgsWithResult[1]
	require.Equal(t, anthropic.MessageParamRoleUser, userMsgWithResult.Role)
	require.Len(t, userMsgWithResult.Content, 1) // ToolResult block

	toolResultBlockWithResult := userMsgWithResult.Content[0].OfToolResult
	require.NotNil(t, toolResultBlockWithResult)
	require.Equal(t, "toolu_01RA76iwg1LbKuDjJnc6ym45", toolResultBlockWithResult.ToolUseID)
	require.Len(t, toolResultBlockWithResult.Content, 1)
	require.NotNil(t, toolResultBlockWithResult.Content[0].OfText)
	require.JSONEq(t, `{"message":"Message printed to the console"}`, toolResultBlockWithResult.Content[0].OfText.Text)
}

// TestAnthropicThinkingRoundTrip verifies that signed thinking blocks survive
// the accumulator → MessagesToAnthropic round-trip. When Claude calls a tool
// during extended thinking, round 2 must include the signed thinking block from
// round 1 byte-identical, or Anthropic rejects the request.
func TestAnthropicThinkingRoundTrip(t *testing.T) {
	t.Parallel()

	// Simulate a round-1 SSE stream: thinking → signature → tool_use
	const thinkingText = "Let me analyze this step by step. The user wants to know about muscle soreness."
	const thinkingSignature = "EqoBCkgIARABGAIiQLTMusa0a0sYmKf4RE97VYjCc8PCh3JO+QAAKE0d9bqVWrl0X4OaSThRXSOxDcN7beFS" // fake but representative
	const toolCallID = "toolu_01ABC123"
	const toolName = "search_pubmed"

	anthropicResponses := `event: message_start
data: {"type":"message_start","message":{"id":"msg_thinking_01","type":"message","role":"assistant","model":"claude-opus-4-20250514","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":500,"cache_creation_input_tokens":0,"cache_read_input_tokens":0,"output_tokens":1}}}

event: content_block_start
data: {"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}

event: content_block_delta
data: {"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"Let me analyze this step by step. "}}

event: content_block_delta
data: {"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"The user wants to know about muscle soreness."}}

event: content_block_delta
data: {"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"` + thinkingSignature + `"}}

event: content_block_stop
data: {"type":"content_block_stop","index":0}

event: content_block_start
data: {"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"` + toolCallID + `","name":"` + toolName + `","input":{}}}

event: content_block_delta
data: {"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":""}}

event: content_block_delta
data: {"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"query\": \"delayed onset muscle soreness\"}"}}

event: content_block_stop
data: {"type":"content_block_stop","index":1}

event: message_delta
data: {"type":"message_delta","delta":{"stop_reason":"tool_use","stop_sequence":null},"usage":{"output_tokens":150}}

event: message_stop
data: {"type":"message_stop"}`

	decoder := ssestream.NewDecoder(&http.Response{
		Body: io.NopCloser(strings.NewReader(anthropicResponses)),
	})
	typedStream := ssestream.NewStream[anthropic.MessageStreamEventUnion](decoder, nil)

	var acc aisdk.DataStreamAccumulator
	stream := aisdk.AnthropicToDataStream(typedStream)
	stream = stream.WithToolCalling(func(toolCall aisdk.ToolCall) any {
		return map[string]any{"results": []string{"DOMS is caused by eccentric exercise"}}
	})
	stream = stream.WithAccumulator(&acc)
	for _, err := range stream {
		require.NoError(t, err)
	}

	// Verify the accumulator captured reasoning with details
	msgs := acc.Messages()
	require.Len(t, msgs, 1, "expected 1 accumulated message")
	msg := msgs[0]
	require.Equal(t, "assistant", msg.Role)

	// Find the reasoning part
	var reasoningPart *aisdk.Part
	for i := range msg.Parts {
		if msg.Parts[i].Type == aisdk.PartTypeReasoning {
			reasoningPart = &msg.Parts[i]
			break
		}
	}
	require.NotNil(t, reasoningPart, "expected a reasoning part in accumulated message")
	require.Equal(t, thinkingText, reasoningPart.Reasoning)
	require.Len(t, reasoningPart.Details, 1, "expected 1 reasoning detail (signed thinking block)")
	require.Equal(t, "text", reasoningPart.Details[0].Type)
	require.Equal(t, thinkingText, reasoningPart.Details[0].Text)
	require.Equal(t, thinkingSignature, reasoningPart.Details[0].Signature, "signature must be byte-identical")

	// ---- ROUND-TRIP: Feed accumulated messages back through MessagesToAnthropic ----
	// This simulates building the round-2 request after tool execution.
	anthropicMsgs, _, err := aisdk.MessagesToAnthropic(msgs)
	require.NoError(t, err)

	// We expect: assistant message (thinking + tool_use) then user message (tool_result)
	require.Len(t, anthropicMsgs, 2, "expected assistant + user messages for round-2")

	assistantMsg := anthropicMsgs[0]
	require.Equal(t, anthropic.MessageParamRoleAssistant, assistantMsg.Role)

	// The assistant content should have: thinking block, tool_use block
	// (step-start and text parts are skipped if empty)
	var foundThinking bool
	var foundToolUse bool
	for _, block := range assistantMsg.Content {
		if block.OfThinking != nil {
			foundThinking = true
			require.Equal(t, thinkingText, block.OfThinking.Thinking, "thinking text must be byte-identical in round-2 request")
			require.Equal(t, thinkingSignature, block.OfThinking.Signature, "thinking signature must be byte-identical in round-2 request")
		}
		if block.OfToolUse != nil {
			foundToolUse = true
			require.Equal(t, toolCallID, block.OfToolUse.ID)
			require.Equal(t, toolName, block.OfToolUse.Name)
		}
	}
	require.True(t, foundThinking, "round-2 request must contain the signed thinking block")
	require.True(t, foundToolUse, "round-2 request must contain the tool_use block")

	// Verify the tool result is in the user message
	userMsg := anthropicMsgs[1]
	require.Equal(t, anthropic.MessageParamRoleUser, userMsg.Role)
	require.Len(t, userMsg.Content, 1)
	require.NotNil(t, userMsg.Content[0].OfToolResult)
	require.Equal(t, toolCallID, userMsg.Content[0].OfToolResult.ToolUseID)

	// Verify the thinking block serializes correctly to JSON (what actually goes over the wire)
	for _, block := range assistantMsg.Content {
		if block.OfThinking != nil {
			j, err := json.Marshal(block)
			require.NoError(t, err)
			var raw map[string]any
			require.NoError(t, json.Unmarshal(j, &raw))
			require.Equal(t, thinkingText, raw["thinking"], "JSON wire format: thinking text")
			require.Equal(t, thinkingSignature, raw["signature"], "JSON wire format: signature")
		}
	}

	t.Log("PASS: Signed thinking blocks survive accumulator → MessagesToAnthropic round-trip")
}

// TestAnthropicRedactedThinkingRoundTrip verifies that redacted thinking blocks
// also survive the round-trip.
func TestAnthropicRedactedThinkingRoundTrip(t *testing.T) {
	t.Parallel()

	const redactedData = "EqoBCkgIARABGAIiQLTMusa0a0sY..."

	anthropicResponses := `event: message_start
data: {"type":"message_start","message":{"id":"msg_redacted_01","type":"message","role":"assistant","model":"claude-opus-4-20250514","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":100,"cache_creation_input_tokens":0,"cache_read_input_tokens":0,"output_tokens":1}}}

event: content_block_start
data: {"type":"content_block_start","index":0,"content_block":{"type":"redacted_thinking","data":"` + redactedData + `"}}

event: content_block_stop
data: {"type":"content_block_stop","index":0}

event: content_block_start
data: {"type":"content_block_start","index":1,"content_block":{"type":"text","text":""}}

event: content_block_delta
data: {"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"Here is my answer."}}

event: content_block_stop
data: {"type":"content_block_stop","index":1}

event: message_delta
data: {"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":20}}

event: message_stop
data: {"type":"message_stop"}`

	decoder := ssestream.NewDecoder(&http.Response{
		Body: io.NopCloser(strings.NewReader(anthropicResponses)),
	})
	typedStream := ssestream.NewStream[anthropic.MessageStreamEventUnion](decoder, nil)

	var acc aisdk.DataStreamAccumulator
	stream := aisdk.AnthropicToDataStream(typedStream)
	stream = stream.WithAccumulator(&acc)
	for _, err := range stream {
		require.NoError(t, err)
	}

	msgs := acc.Messages()
	require.Len(t, msgs, 1)

	// Find reasoning part with redacted detail
	var reasoningPart *aisdk.Part
	for i := range msgs[0].Parts {
		if msgs[0].Parts[i].Type == aisdk.PartTypeReasoning {
			reasoningPart = &msgs[0].Parts[i]
			break
		}
	}
	require.NotNil(t, reasoningPart)
	require.Len(t, reasoningPart.Details, 1)
	require.Equal(t, "redacted", reasoningPart.Details[0].Type)
	require.Equal(t, redactedData, reasoningPart.Details[0].Data)

	// Round-trip through MessagesToAnthropic
	anthropicMsgs, _, err := aisdk.MessagesToAnthropic(msgs)
	require.NoError(t, err)
	require.Len(t, anthropicMsgs, 1)

	var foundRedacted bool
	for _, block := range anthropicMsgs[0].Content {
		if block.OfRedactedThinking != nil {
			foundRedacted = true
			require.Equal(t, redactedData, block.OfRedactedThinking.Data, "redacted data must be byte-identical")
		}
	}
	require.True(t, foundRedacted, "round-2 request must contain the redacted thinking block")
	t.Log("PASS: Redacted thinking blocks survive round-trip")
}

// TestMessagesToAnthropic_ContentFallback verifies that when Parts is empty
// but Content is non-empty, MessagesToAnthropic creates a text part from Content.
func TestMessagesToAnthropic_ContentFallback(t *testing.T) {
	t.Parallel()

	messages, system, err := aisdk.MessagesToAnthropic([]aisdk.Message{
		{
			Role:    "system",
			Content: "You are helpful.",
			// Parts intentionally nil
		},
		{
			Role:    "user",
			Content: "hello",
			// Parts intentionally nil
		},
	})
	require.NoError(t, err)
	require.Len(t, system, 1)
	require.Equal(t, "You are helpful.", system[0].Text)
	require.Len(t, messages, 1)
	require.Equal(t, anthropic.MessageParamRoleUser, messages[0].Role)
	require.Len(t, messages[0].Content, 1)
	require.NotNil(t, messages[0].Content[0].OfText)
	require.Equal(t, "hello", messages[0].Content[0].OfText.Text)
}

// TestMessagesToOpenAI_ContentFallback verifies that when Parts is empty
// but Content is non-empty, MessagesToOpenAI creates a text part from Content.
func TestMessagesToOpenAI_ContentFallback(t *testing.T) {
	t.Parallel()

	messages, err := aisdk.MessagesToOpenAI([]aisdk.Message{
		{
			Role:    "system",
			Content: "You are helpful.",
		},
		{
			Role:    "user",
			Content: "hello",
		},
	})
	require.NoError(t, err)
	require.Len(t, messages, 2)

	// System message — openai.SystemMessage wraps the content in param.Opt
	require.NotNil(t, messages[0].OfSystem)
	require.Equal(t, "You are helpful.", messages[0].OfSystem.Content.OfString.Value)

	// User message — should have synthesized a text part
	require.NotNil(t, messages[1].OfUser)
	require.Len(t, messages[1].OfUser.Content.OfArrayOfContentParts, 1)
	require.NotNil(t, messages[1].OfUser.Content.OfArrayOfContentParts[0].OfText)
	require.Equal(t, "hello", messages[1].OfUser.Content.OfArrayOfContentParts[0].OfText.Text)
}

// TestMessagesToGoogle_ContentFallback verifies that when Parts is empty
// but Content is non-empty, MessagesToGoogle creates a text part from Content.
func TestMessagesToGoogle_ContentFallback(t *testing.T) {
	t.Parallel()

	contents, err := aisdk.MessagesToGoogle([]aisdk.Message{
		{
			Role:    "user",
			Content: "hello",
		},
	})
	require.NoError(t, err)
	require.Len(t, contents, 1)
	require.Equal(t, "user", contents[0].Role)
	require.Len(t, contents[0].Parts, 1)
	require.Equal(t, "hello", contents[0].Parts[0].Text)
}

func TestMessagesToAnthropic_Live(t *testing.T) {
	t.Parallel()
	apiKey := os.Getenv("ANTHROPIC_API_KEY")
	if apiKey == "" {
		t.Skip("ANTHROPIC_API_KEY is not set")
	}
	ctx := context.Background()
	client := anthropic.NewClient(option.WithAPIKey(apiKey))

	// Ensure messages are converted correctly.
	prompt := "use the 'print' tool to print 'Hello, world!' and then show the result"
	messages, systemPrompts, err := aisdk.MessagesToAnthropic([]aisdk.Message{
		{
			Role: "system",
			Parts: []aisdk.Part{
				{Text: "You are a helpful assistant.", Type: aisdk.PartTypeText},
			},
		},
		{
			Role: "user", Parts: []aisdk.Part{
				{Text: prompt, Type: aisdk.PartTypeText},
			},
		},
	})
	require.Len(t, messages, 1)
	require.Len(t, systemPrompts, 1)
	require.Len(t, messages[0].Content, 1)
	require.NotNil(t, messages[0].Content[0].OfText)
	require.Equal(t, messages[0].Content[0].OfText.Text, prompt)
	require.NoError(t, err)

	stream := client.Messages.NewStreaming(ctx, anthropic.MessageNewParams{
		Messages:  messages,
		Model:     anthropic.ModelClaudeSonnet4_5,
		System:    systemPrompts,
		MaxTokens: 10,
	})
	require.NoError(t, err)

	dataStream := aisdk.AnthropicToDataStream(stream)
	var streamErr error
	dataStream(func(part aisdk.DataStreamPart, err error) bool {
		if err != nil {
			streamErr = err
			return false
		}
		return true
	})
	require.NoError(t, streamErr)
}

// TestServerToolUseNoDeltaLeak verifies that InputJSONDelta events for server
// tools (e.g., web_search) are NOT emitted as ToolCallDeltaStreamPart. Without
// this, downstream tool-calling wrappers would try to process them as regular
// tool calls and hit the "empty tool name" error because server tools are
// skipped in ToolCallStartStreamPart handling.
func TestServerToolUseNoDeltaLeak(t *testing.T) {
	t.Parallel()

	// Simulated Anthropic stream with a server_tool_use block (web_search).
	// The server_tool_use block has an empty Name field (known SDK bug).
	serverToolStream := `event: message_start
data: {"type":"message_start","message":{"id":"msg_srv01","type":"message","role":"assistant","model":"claude-sonnet-4-6","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":100,"cache_creation_input_tokens":0,"cache_read_input_tokens":0,"output_tokens":1}}}

event: content_block_start
data: {"type":"content_block_start","index":0,"content_block":{"type":"server_tool_use","id":"srvtoolu_01ABC","name":"","input":{}}}

event: content_block_delta
data: {"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"query\":"}}

event: content_block_delta
data: {"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"\"test\"}"}}

event: content_block_stop
data: {"type":"content_block_stop","index":0}

event: content_block_start
data: {"type":"content_block_start","index":1,"content_block":{"type":"web_search_tool_result","tool_use_id":"srvtoolu_01ABC","content":[{"type":"web_search_result","url":"https://example.com","title":"Example","encrypted_content":"enc123","page_age":"2d"}]}}

event: content_block_stop
data: {"type":"content_block_stop","index":1}

event: content_block_start
data: {"type":"content_block_start","index":2,"content_block":{"type":"text","text":""}}

event: content_block_delta
data: {"type":"content_block_delta","index":2,"delta":{"type":"text_delta","text":"Here is the answer."}}

event: content_block_stop
data: {"type":"content_block_stop","index":2}

event: message_delta
data: {"type":"message_delta","delta":{"stop_reason":"end_turn","stop_sequence":null},"usage":{"output_tokens":50}}

event: message_stop
data: {"type":"message_stop"}`

	decoder := ssestream.NewDecoder(&http.Response{
		Body: io.NopCloser(strings.NewReader(serverToolStream)),
	})
	typedStream := ssestream.NewStream[anthropic.MessageStreamEventUnion](decoder, nil)

	var parts []aisdk.DataStreamPart
	stream := aisdk.AnthropicToDataStream(typedStream)
	for part, err := range stream {
		require.NoError(t, err)
		parts = append(parts, part)
	}

	// Verify: no ToolCallDeltaStreamPart should appear (server tool deltas are suppressed).
	for _, part := range parts {
		_, isDelta := part.(aisdk.ToolCallDeltaStreamPart)
		require.False(t, isDelta, "ToolCallDeltaStreamPart should not be emitted for server tools")
	}

	// Verify: ToolCallStartStreamPart for the server tool should have IsServerTool=true
	// and a non-empty name (fallback to "web_search").
	var foundStart bool
	for _, part := range parts {
		if start, ok := part.(aisdk.ToolCallStartStreamPart); ok {
			foundStart = true
			require.True(t, start.IsServerTool)
			require.Equal(t, "web_search", start.ToolName)
			require.Equal(t, "srvtoolu_01ABC", start.ToolCallID)
		}
	}
	require.True(t, foundStart, "expected ToolCallStartStreamPart for server tool")

	// Verify: ToolCallStreamPart emitted at content_block_stop with the query args.
	var foundToolCall bool
	for _, part := range parts {
		if tc, ok := part.(aisdk.ToolCallStreamPart); ok {
			foundToolCall = true
			require.True(t, tc.IsServerTool)
			require.Equal(t, "web_search", tc.ToolName)
			require.Equal(t, "srvtoolu_01ABC", tc.ToolCallID)
			require.Equal(t, "test", tc.Args["query"])
		}
	}
	require.True(t, foundToolCall, "expected ToolCallStreamPart with server tool query")

	// Verify: WebSearchResultStreamPart should be present.
	var foundResult bool
	for _, part := range parts {
		if ws, ok := part.(aisdk.WebSearchResultStreamPart); ok {
			foundResult = true
			require.Equal(t, "srvtoolu_01ABC", ws.ToolCallID)
			require.Len(t, ws.Results, 1)
			require.Equal(t, "https://example.com", ws.Results[0].URL)
		}
	}
	require.True(t, foundResult, "expected WebSearchResultStreamPart")
}
