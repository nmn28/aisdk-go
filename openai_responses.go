package aisdk

import (
	"encoding/json"
	"fmt"

	"github.com/openai/openai-go/packages/param"
	"github.com/openai/openai-go/packages/ssestream"
	"github.com/openai/openai-go/responses"
)

// ToolsToOpenAIResponses converts the tool format to OpenAI Responses API format.
func ToolsToOpenAIResponses(tools []Tool) []responses.ToolUnionParam {
	out := make([]responses.ToolUnionParam, 0, len(tools))
	for _, tool := range tools {
		var schemaParams map[string]any
		if tool.Schema.Properties != nil {
			schemaParams = map[string]any{
				"type":       "object",
				"properties": tool.Schema.Properties,
			}
			if len(tool.Schema.Required) > 0 {
				schemaParams["required"] = tool.Schema.Required
			}
		}
		out = append(out, responses.ToolUnionParam{
			OfFunction: &responses.FunctionToolParam{
				Name:        tool.Name,
				Description: param.NewOpt(tool.Description),
				Parameters:  schemaParams,
			},
		})
	}
	return out
}

// MessagesToOpenAIResponses converts internal messages to the Responses API input format.
// Returns the input items and the system instruction (if any).
func MessagesToOpenAIResponses(messages []Message) (responses.ResponseInputParam, string, error) {
	var items responses.ResponseInputParam
	var systemInstruction string

	for _, message := range messages {
		// Fallback: if Parts is empty but Content is non-empty, synthesize a text part.
		parts := message.Parts
		if len(parts) == 0 && message.Content != "" {
			parts = []Part{{Type: PartTypeText, Text: message.Content}}
		}

		switch message.Role {
		case "system":
			// System messages become the instruction parameter.
			if len(parts) > 0 && parts[0].Type == PartTypeText {
				systemInstruction = parts[0].Text
			} else {
				systemInstruction = message.Content
			}

		case "user":
			for _, part := range parts {
				switch part.Type {
				case PartTypeText:
					items = append(items, responses.ResponseInputItemUnionParam{
						OfMessage: &responses.EasyInputMessageParam{
							Role: responses.EasyInputMessageRoleUser,
							Content: responses.EasyInputMessageContentUnionParam{
								OfString: param.NewOpt(part.Text),
							},
						},
					})
				}
			}

		case "assistant":
			for _, part := range parts {
				switch part.Type {
				case PartTypeText:
					items = append(items, responses.ResponseInputItemUnionParam{
						OfMessage: &responses.EasyInputMessageParam{
							Role: responses.EasyInputMessageRoleAssistant,
							Content: responses.EasyInputMessageContentUnionParam{
								OfString: param.NewOpt(part.Text),
							},
						},
					})

				case PartTypeToolInvocation:
					if part.ToolInvocation == nil {
						return nil, "", fmt.Errorf("assistant message part has type tool-invocation but nil ToolInvocation field (ID: %s)", message.ID)
					}
					argsJSON, err := json.Marshal(part.ToolInvocation.Args)
					if err != nil {
						return nil, "", fmt.Errorf("marshalling tool input for call %s: %w", part.ToolInvocation.ToolCallID, err)
					}

					items = append(items, responses.ResponseInputItemParamOfFunctionCall(
						string(argsJSON),
						part.ToolInvocation.ToolCallID,
						part.ToolInvocation.ToolName,
					))

					if part.ToolInvocation.State == ToolInvocationStateResult {
						resultJSON, err := json.Marshal(part.ToolInvocation.Result)
						if err != nil {
							return nil, "", fmt.Errorf("marshalling tool result for call %s: %w", part.ToolInvocation.ToolCallID, err)
						}
						items = append(items, responses.ResponseInputItemParamOfFunctionCallOutput(
							part.ToolInvocation.ToolCallID,
							string(resultJSON),
						))
					}
				}
			}
		}
	}

	return items, systemInstruction, nil
}

// OpenAIResponsesToDataStream pipes an OpenAI Responses API stream to a DataStream.
// It emits reasoning summaries as ReasoningStreamPart (type 'g') when available.
func OpenAIResponsesToDataStream(stream *ssestream.Stream[responses.ResponseStreamEventUnion]) DataStream {
	return func(yield func(DataStreamPart, error) bool) {
		var (
			currentToolCallID string
			currentToolName   string
			finalReason       FinishReason = FinishReasonUnknown
			finalUsage        Usage
			emittedStart      bool
		)

		// Track tool call IDs from output_item.added so we can map
		// function_call_arguments.delta to the right tool call.
		toolCallIDs := make(map[int64]struct {
			callID string
			name   string
		})

		for stream.Next() {
			chunk := stream.Current()
			event := chunk.AsAny()

			switch ev := event.(type) {
			case responses.ResponseCreatedEvent:
				if !emittedStart {
					emittedStart = true
					if !yield(StartStepStreamPart{MessageID: ev.Response.ID}, nil) {
						return
					}
				}

			case responses.ResponseOutputItemAddedEvent:
				// Track function calls by output index
				switch item := ev.Item.AsAny().(type) {
				case responses.ResponseFunctionToolCall:
					callID := item.CallID
					if callID == "" {
						callID = item.ID
					}
					toolCallIDs[ev.OutputIndex] = struct {
						callID string
						name   string
					}{callID, item.Name}
					currentToolCallID = callID
					currentToolName = item.Name
					if !yield(ToolCallStartStreamPart{
						ToolCallID: callID,
						ToolName:   item.Name,
					}, nil) {
						return
					}
				}

			case responses.ResponseTextDeltaEvent:
				if !yield(TextStreamPart{Content: ev.Delta}, nil) {
					return
				}

			case responses.ResponseReasoningSummaryTextDeltaEvent:
				// Emit reasoning summaries as ReasoningStreamPart
				if !yield(ReasoningStreamPart{Content: ev.Delta}, nil) {
					return
				}

			case responses.ResponseFunctionCallArgumentsDeltaEvent:
				id := currentToolCallID
				if tc, ok := toolCallIDs[ev.OutputIndex]; ok {
					id = tc.callID
				}
				if !yield(ToolCallDeltaStreamPart{
					ToolCallID:    id,
					ArgsTextDelta: ev.Delta,
				}, nil) {
					return
				}

			case responses.ResponseFunctionCallArgumentsDoneEvent:
				// Arguments are complete — emit the full tool call so WithToolCalling can execute it.
				id := currentToolCallID
				name := currentToolName
				if tc, ok := toolCallIDs[ev.OutputIndex]; ok {
					id = tc.callID
					name = tc.name
				}
				var args map[string]any
				if err := json.Unmarshal([]byte(ev.Arguments), &args); err != nil {
					args = map[string]any{}
				}
				if !yield(ToolCallStreamPart{
					ToolCallID: id,
					ToolName:   name,
					Args:       args,
				}, nil) {
					return
				}

			case responses.ResponseCompletedEvent:
				resp := ev.Response

				// Determine finish reason from output items
				hasToolCalls := false
				for _, item := range resp.Output {
					if _, ok := item.AsAny().(responses.ResponseFunctionToolCall); ok {
						hasToolCalls = true
						break
					}
				}
				if hasToolCalls {
					finalReason = FinishReasonToolCalls
				} else {
					finalReason = FinishReasonStop
				}

				// Extract usage
				if resp.Usage.InputTokens > 0 {
					tokens := int64(resp.Usage.InputTokens)
					finalUsage.PromptTokens = &tokens
				}
				if resp.Usage.OutputTokens > 0 {
					tokens := int64(resp.Usage.OutputTokens)
					finalUsage.CompletionTokens = &tokens
				}

				if !yield(FinishStepStreamPart{
					FinishReason: finalReason,
					Usage:        finalUsage,
					IsContinued:  false,
				}, nil) {
					return
				}

				if !yield(FinishMessageStreamPart{
					FinishReason: finalReason,
					Usage:        finalUsage,
				}, nil) {
					return
				}

			case responses.ResponseFailedEvent:
				yield(ErrorStreamPart{Content: "OpenAI Responses API request failed"}, nil)
				return

			case responses.ResponseIncompleteEvent:
				finalReason = FinishReasonLength
				if !yield(FinishStepStreamPart{
					FinishReason: finalReason,
					Usage:        finalUsage,
				}, nil) {
					return
				}
				yield(FinishMessageStreamPart{
					FinishReason: finalReason,
					Usage:        finalUsage,
				}, nil)
				return
			}
		}

		if err := stream.Err(); err != nil {
			yield(nil, fmt.Errorf("openai responses stream error: %w", err))
			return
		}

		// Safety: if we never got a completed event
		if finalReason == FinishReasonUnknown {
			yield(FinishMessageStreamPart{
				FinishReason: FinishReasonStop,
				Usage:        finalUsage,
			}, nil)
		}

		_ = currentToolName
	}
}
