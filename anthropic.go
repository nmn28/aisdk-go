package aisdk

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/anthropics/anthropic-sdk-go"
	"github.com/anthropics/anthropic-sdk-go/packages/ssestream"
)

// ToolsToAnthropic converts client tools to Anthropic's API format.
func ToolsToAnthropic(tools []Tool) []anthropic.ToolUnionParam {
	anthropicTools := []anthropic.ToolUnionParam{}
	for _, tool := range tools {
		properties := tool.Schema.Properties
		if properties == nil {
			properties = make(map[string]interface{})
		}
		inputSchema := anthropic.ToolInputSchemaParam{
			Properties: properties,
		}
		if len(tool.Schema.Required) > 0 {
			if inputSchema.ExtraFields == nil {
				inputSchema.ExtraFields = make(map[string]interface{})
			}
			inputSchema.ExtraFields["required"] = tool.Schema.Required
		}

		anthropicTools = append(anthropicTools, anthropic.ToolUnionParam{
			OfTool: &anthropic.ToolParam{
				Name:        tool.Name,
				Description: anthropic.String(tool.Description),
				InputSchema: inputSchema,
			},
		})
	}
	return anthropicTools
}

// ServerToolsToAnthropic converts server tool definitions to Anthropic's API format.
func ServerToolsToAnthropic(serverTools []ServerTool) []anthropic.ToolUnionParam {
	anthropicTools := []anthropic.ToolUnionParam{}
	for _, st := range serverTools {
		switch st.Type {
		case "web_search_20250305":
			param := &anthropic.WebSearchTool20250305Param{}
			if st.MaxUses != nil {
				param.MaxUses = anthropic.Opt(*st.MaxUses)
			}
			if len(st.AllowedDomains) > 0 {
				param.AllowedDomains = st.AllowedDomains
			}
			if len(st.BlockedDomains) > 0 {
				param.BlockedDomains = st.BlockedDomains
			}
			if st.UserLocation != nil {
				param.UserLocation = anthropic.WebSearchTool20250305UserLocationParam{
					City:     anthropic.Opt(st.UserLocation.City),
					Region:   anthropic.Opt(st.UserLocation.Region),
					Country:  anthropic.Opt(st.UserLocation.Country),
					Timezone: anthropic.Opt(st.UserLocation.Timezone),
				}
			}
			anthropicTools = append(anthropicTools, anthropic.ToolUnionParam{
				OfWebSearchTool20250305: param,
			})
		}
	}
	return anthropicTools
}

// MessagesToAnthropic converts internal message format to Anthropic's API format.
// It extracts system messages into a separate slice of TextBlockParams and groups
// consecutive user/tool and assistant messages according to Anthropic's rules.
// It handles the case where a single assistant message part contains both the
// tool call and its result, splitting them into the required assistant tool_use
// and user tool_result blocks.
func MessagesToAnthropic(messages []Message) ([]anthropic.MessageParam, []anthropic.TextBlockParam, error) {
	anthropicMessages := []anthropic.MessageParam{}

	var systemPrompt []anthropic.TextBlockParam

	for _, message := range messages {
		role := anthropic.MessageParamRoleAssistant
		content := []anthropic.ContentBlockParamUnion{}

		switch message.Role {
		case "system":
			if len(systemPrompt) > 0 {
				return nil, nil, fmt.Errorf("multiple system messages found")
			}
			for _, part := range message.Parts {
				if part.Type == PartTypeText && part.Text != "" {
					systemPrompt = append(systemPrompt, anthropic.TextBlockParam{
						Text: part.Text,
					})
				}
			}
			break
		case "assistant":
			for _, part := range message.Parts {
				switch part.Type {
				case PartTypeText:
					textParam := &anthropic.TextBlockParam{
						Text: part.Text,
					}
					// Forward citations with encrypted_index for multi-turn
					if len(part.Citations) > 0 {
						citations := make([]anthropic.TextCitationParamUnion, 0, len(part.Citations))
						for _, c := range part.Citations {
							if c.Type == "web_search_result_location" && c.EncryptedIndex != "" {
								citations = append(citations, anthropic.TextCitationParamUnion{
									OfWebSearchResultLocation: &anthropic.CitationWebSearchResultLocationParam{
										CitedText:      c.CitedText,
										EncryptedIndex: c.EncryptedIndex,
										Title:          anthropic.Opt(c.Title),
										URL:            c.URL,
									},
								})
							}
						}
						if len(citations) > 0 {
							textParam.Citations = citations
						}
					}
					content = append(content, anthropic.ContentBlockParamUnion{
						OfText: textParam,
					})
				case PartTypeToolInvocation:
					if part.ToolInvocation == nil {
						return nil, nil, fmt.Errorf("assistant message part has type tool-invocation but nil ToolInvocation field (ID: %s)", message.ID)
					}
					argsJSON, err := json.Marshal(part.ToolInvocation.Args)
					if err != nil {
						return nil, nil, fmt.Errorf("marshalling tool input for call %s: %w", part.ToolInvocation.ToolCallID, err)
					}
					content = append(content, anthropic.ContentBlockParamUnion{
						OfToolUse: &anthropic.ToolUseBlockParam{
							ID:    part.ToolInvocation.ToolCallID,
							Input: json.RawMessage(argsJSON),
							Name:  part.ToolInvocation.ToolName,
						},
					})

					if part.ToolInvocation.State != ToolInvocationStateResult {
						continue
					}

					anthropicMessages = append(anthropicMessages, anthropic.MessageParam{
						Role:    role,
						Content: content,
					})
					content = nil

					resultContent := []anthropic.ToolResultBlockParamContentUnion{}
					resultParts, err := toolResultToParts(part.ToolInvocation.Result)
					if err != nil {
						return nil, nil, fmt.Errorf("failed to convert tool call result to parts: %w", err)
					}
					for _, resultPart := range resultParts {
						switch resultPart.Type {
						case PartTypeText:
							resultContent = append(resultContent, anthropic.ToolResultBlockParamContentUnion{
								OfText: &anthropic.TextBlockParam{Text: resultPart.Text},
							})
						case PartTypeFile:
							resultContent = append(resultContent, anthropic.ToolResultBlockParamContentUnion{
								OfImage: &anthropic.ImageBlockParam{
									Source: anthropic.ImageBlockParamSourceUnion{
										OfBase64: &anthropic.Base64ImageSourceParam{
											Data:      base64.StdEncoding.EncodeToString(resultPart.Data),
											MediaType: anthropic.Base64ImageSourceMediaType(resultPart.MimeType),
										},
									},
								},
							})
						}
					}

					anthropicMessages = append(anthropicMessages, anthropic.MessageParam{
						Role: anthropic.MessageParamRoleUser,
						Content: []anthropic.ContentBlockParamUnion{
							{
								OfToolResult: &anthropic.ToolResultBlockParam{
									ToolUseID: part.ToolInvocation.ToolCallID,
									Content:   resultContent,
								},
							},
						},
					})
					content = nil

				case PartTypeServerToolUse:
					// Round-trip server_tool_use blocks for multi-turn
					inputJSON, _ := json.Marshal(part.ServerToolInput)
					content = append(content, anthropic.ContentBlockParamUnion{
						OfServerToolUse: &anthropic.ServerToolUseBlockParam{
							ID:    part.ServerToolUseID,
							Input: json.RawMessage(inputJSON),
						},
					})

				case PartTypeWebSearchResult:
					// Round-trip web_search_tool_result blocks with encrypted content
					if part.WebSearchError != "" {
						// Error result
						content = append(content, anthropic.ContentBlockParamUnion{
							OfWebSearchToolResult: &anthropic.WebSearchToolResultBlockParam{
								ToolUseID: part.WebSearchToolUseID,
								Content: anthropic.WebSearchToolResultBlockParamContentUnion{
									OfRequestWebSearchToolResultError: &anthropic.WebSearchToolRequestErrorParam{
										ErrorCode: anthropic.WebSearchToolRequestErrorErrorCode(part.WebSearchError),
									},
								},
							},
						})
					} else {
						// Success result with encrypted content
						resultParams := make([]anthropic.WebSearchResultBlockParam, len(part.WebSearchResults))
						for i, r := range part.WebSearchResults {
							resultParams[i] = anthropic.WebSearchResultBlockParam{
								EncryptedContent: r.EncryptedContent,
								Title:            r.Title,
								URL:              r.URL,
							}
							if r.PageAge != "" {
								resultParams[i].PageAge = anthropic.Opt(r.PageAge)
							}
						}
						content = append(content, anthropic.ContentBlockParamUnion{
							OfWebSearchToolResult: &anthropic.WebSearchToolResultBlockParam{
								ToolUseID: part.WebSearchToolUseID,
								Content: anthropic.WebSearchToolResultBlockParamContentUnion{
									OfWebSearchToolResultBlockItem: resultParams,
								},
							},
						})
					}
				}
			}
		case "user":
			role = anthropic.MessageParamRoleUser
			for _, part := range message.Parts {
				switch part.Type {
				case PartTypeText:
					content = append(content, anthropic.ContentBlockParamUnion{
						OfText: &anthropic.TextBlockParam{Text: part.Text},
					})
				case PartTypeFile:
					content = append(content, anthropic.ContentBlockParamUnion{
						OfImage: &anthropic.ImageBlockParam{
							Source: anthropic.ImageBlockParamSourceUnion{
								OfBase64: &anthropic.Base64ImageSourceParam{
									Data:      base64.StdEncoding.EncodeToString(part.Data),
									MediaType: anthropic.Base64ImageSourceMediaType(part.MimeType),
								},
							},
						},
					})
				case PartTypeToolInvocation:
					return nil, nil, fmt.Errorf("user message part has type tool-invocation (ID: %s)", message.ID)
				}
			}
		default:
			return nil, nil, fmt.Errorf("unsupported message role encountered: %s", message.Role)
		}

		if len(message.Attachments) > 0 {
			for _, attachment := range message.Attachments {
				// URLs typically have the mime prefixing as a URL.
				parts := strings.SplitN(attachment.URL, ",", 2)
				if len(parts) != 2 {
					return nil, nil, fmt.Errorf("invalid attachment URL: %s", attachment.URL)
				}
				content = append(content, anthropic.ContentBlockParamUnion{
					OfImage: &anthropic.ImageBlockParam{
						Source: anthropic.ImageBlockParamSourceUnion{
							OfBase64: &anthropic.Base64ImageSourceParam{
								Data:      parts[1],
								MediaType: anthropic.Base64ImageSourceMediaType(attachment.ContentType),
							},
						},
					},
				})
			}
		}
		if len(content) > 0 {
			anthropicMessages = append(anthropicMessages, anthropic.MessageParam{
				Role:    role,
				Content: content,
			})
			content = nil
		}
	}

	return anthropicMessages, systemPrompt, nil
}

// AnthropicToDataStream pipes an Anthropic stream to a DataStream.
func AnthropicToDataStream(stream *ssestream.Stream[anthropic.MessageStreamEventUnion]) DataStream {
	return func(yield func(DataStreamPart, error) bool) {
		var lastChunk *anthropic.MessageStreamEventUnion
		var finalReason FinishReason = FinishReasonUnknown
		var finalUsage Usage
		var currentToolCall struct {
			ID           string
			Args         string
			IsServerTool bool
		}
		// Track which tool call IDs are server tools so we skip their deltas in WithToolCalling
		serverToolIDs := make(map[string]bool)

		for stream.Next() {
			chunk := stream.Current()
			lastChunk = &chunk

			event := chunk.AsAny()
			switch event := event.(type) {
			case anthropic.MessageStartEvent:
				if !yield(StartStepStreamPart{
					MessageID: event.Message.ID,
				}, nil) {
					return
				}

			case anthropic.ContentBlockDeltaEvent:
				switch delta := event.Delta.AsAny().(type) {
				case anthropic.TextDelta:
					if !yield(TextStreamPart{Content: delta.Text}, nil) {
						return
					}
				case anthropic.InputJSONDelta:
					// Accumulate the arguments for the current tool call
					currentToolCall.Args += delta.PartialJSON
					if !yield(ToolCallDeltaStreamPart{
						ToolCallID:    currentToolCall.ID,
						ArgsTextDelta: delta.PartialJSON,
					}, nil) {
						return
					}
				case anthropic.ThinkingDelta:
					if !yield(ReasoningStreamPart{Content: delta.Thinking}, nil) {
						return
					}
				case anthropic.CitationsDelta:
					// Inline citation from web search results
					if citation, ok := delta.Citation.AsAny().(anthropic.CitationsWebSearchResultLocation); ok {
						if !yield(SourceStreamPart{
							SourceType:     "web_search_result_location",
							ID:             fmt.Sprintf("cite_%d", event.Index),
							URL:            citation.URL,
							Title:          citation.Title,
							CitedText:      citation.CitedText,
							EncryptedIndex: citation.EncryptedIndex,
						}, nil) {
							return
						}
					}
				}

			case anthropic.ContentBlockStartEvent:
				switch block := event.ContentBlock.AsAny().(type) {
				case anthropic.ToolUseBlock:
					currentToolCall.ID = block.ID
					currentToolCall.Args = ""
					currentToolCall.IsServerTool = false

					if !yield(ToolCallStartStreamPart{
						ToolCallID: block.ID,
						ToolName:   block.Name,
					}, nil) {
						return
					}

				case anthropic.ServerToolUseBlock:
					// Server-side tool invocation (e.g., web_search)
					currentToolCall.ID = block.ID
					currentToolCall.Args = ""
					currentToolCall.IsServerTool = true
					serverToolIDs[block.ID] = true

					if !yield(ToolCallStartStreamPart{
						ToolCallID:   block.ID,
						ToolName:     string(block.Name),
						IsServerTool: true,
					}, nil) {
						return
					}

				case anthropic.WebSearchToolResultBlock:
					// Server-side search results — arrives as a single block
					results := block.Content.AsWebSearchResultBlockArray()
					if len(results) > 0 {
						wsResults := make([]WebSearchResult, len(results))
						for i, r := range results {
							wsResults[i] = WebSearchResult{
								URL:              r.URL,
								Title:            r.Title,
								EncryptedContent: r.EncryptedContent,
								PageAge:          r.PageAge,
							}
						}
						if !yield(WebSearchResultStreamPart{
							ToolCallID: block.ToolUseID,
							Results:    wsResults,
						}, nil) {
							return
						}
					} else {
						// Check for error
						errResult := block.Content.AsResponseWebSearchToolResultError()
						if string(errResult.ErrorCode) != "" {
							if !yield(WebSearchResultStreamPart{
								ToolCallID: block.ToolUseID,
								Error:      string(errResult.ErrorCode),
							}, nil) {
								return
							}
						}
					}
				}

			case anthropic.MessageDeltaEvent:
				switch event.Delta.StopReason {
				case "tool_use":
					finalReason = FinishReasonToolCalls
				case "pause_turn":
					finalReason = FinishReasonPauseTurn
				}
				if event.Usage.OutputTokens != 0 {
					tokens := event.Usage.OutputTokens
					finalUsage.CompletionTokens = &tokens
				}

				// Reset current tool call
				currentToolCall = struct {
					ID           string
					Args         string
					IsServerTool bool
				}{}

			case anthropic.MessageStopEvent:
				// Determine final reason if not already set
				if finalReason == FinishReasonUnknown {
					finalReason = FinishReasonStop
				}

				// Send final finish step
				if !yield(FinishStepStreamPart{
					FinishReason: finalReason,
					Usage:        finalUsage,
					IsContinued:  finalReason == FinishReasonPauseTurn,
				}, nil) {
					return
				}

				// Send final finish message
				if !yield(FinishMessageStreamPart{
					FinishReason: finalReason,
					Usage:        finalUsage,
				}, nil) {
					return
				}
			}
		}

		// Handle any errors from the stream
		if err := stream.Err(); err != nil {
			yield(nil, fmt.Errorf("anthropic stream error: %w", err))
			return
		}

		// If we didn't get a message stop event (e.g., stream ended abruptly),
		// send a final finish message based on the last known state.
		if lastChunk == nil || lastChunk.Type != "message_stop" {
			if finalReason == FinishReasonUnknown {
				finalReason = FinishReasonError
			}

			yield(FinishMessageStreamPart{
				FinishReason: finalReason,
				Usage:        finalUsage,
			}, nil)
		}
	}
}
