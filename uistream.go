package aisdk

import (
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"net/http"

	"github.com/google/uuid"
)

// UIMessageStreamPart represents a part of the AI SDK v6 UI Message Stream protocol.
// Unlike DataStreamPart (v4's "0:text\n" format), this uses SSE with JSON events:
//
//	data: {"type":"text-delta","id":"...","delta":"Hello"}
//
// See: https://ai-sdk.dev/docs/ai-sdk-ui/stream-protocol
type UIMessageStreamPart interface {
	// UIStreamJSON returns the JSON object for this event (without the "data: " prefix).
	UIStreamJSON() ([]byte, error)
	// UIStreamType returns the event type string (e.g., "text-delta", "tool-input-start").
	UIStreamType() string
}

// --- Flow Control Events ---

// UIStartPart signals the start of a message stream.
type UIStartPart struct {
	MessageID string `json:"messageId"`
}

func (p UIStartPart) UIStreamType() string { return "start" }
func (p UIStartPart) UIStreamJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type      string `json:"type"`
		MessageID string `json:"messageId"`
	}{"start", p.MessageID})
}

// UIFinishPart signals the end of message generation.
// AI SDK v6 strict schema: only type + finishReason + messageMetadata allowed.
type UIFinishPart struct {
	FinishReason FinishReason `json:"finishReason,omitempty"`
}

func (p UIFinishPart) UIStreamType() string { return "finish" }
func (p UIFinishPart) UIStreamJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type         string       `json:"type"`
		FinishReason FinishReason `json:"finishReason,omitempty"`
	}{"finish", p.FinishReason})
}

// --- Text Streaming Events ---

// UITextStartPart opens a text block.
type UITextStartPart struct {
	ID string `json:"id"`
}

func (p UITextStartPart) UIStreamType() string { return "text-start" }
func (p UITextStartPart) UIStreamJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type string `json:"type"`
		ID   string `json:"id"`
	}{"text-start", p.ID})
}

// UITextDeltaPart streams a text chunk.
type UITextDeltaPart struct {
	ID    string `json:"id"`
	Delta string `json:"delta"`
}

func (p UITextDeltaPart) UIStreamType() string { return "text-delta" }
func (p UITextDeltaPart) UIStreamJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type  string `json:"type"`
		ID    string `json:"id"`
		Delta string `json:"delta"`
	}{"text-delta", p.ID, p.Delta})
}

// UITextEndPart closes a text block.
type UITextEndPart struct {
	ID string `json:"id"`
}

func (p UITextEndPart) UIStreamType() string { return "text-end" }
func (p UITextEndPart) UIStreamJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type string `json:"type"`
		ID   string `json:"id"`
	}{"text-end", p.ID})
}

// --- Reasoning Streaming Events ---

// UIReasoningStartPart opens a reasoning/thinking block.
type UIReasoningStartPart struct {
	ID string `json:"id"`
}

func (p UIReasoningStartPart) UIStreamType() string { return "reasoning-start" }
func (p UIReasoningStartPart) UIStreamJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type string `json:"type"`
		ID   string `json:"id"`
	}{"reasoning-start", p.ID})
}

// UIReasoningDeltaPart streams a reasoning text chunk.
type UIReasoningDeltaPart struct {
	ID    string `json:"id"`
	Delta string `json:"delta"`
}

func (p UIReasoningDeltaPart) UIStreamType() string { return "reasoning-delta" }
func (p UIReasoningDeltaPart) UIStreamJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type  string `json:"type"`
		ID    string `json:"id"`
		Delta string `json:"delta"`
	}{"reasoning-delta", p.ID, p.Delta})
}

// UIReasoningEndPart closes a reasoning block.
type UIReasoningEndPart struct {
	ID string `json:"id"`
}

func (p UIReasoningEndPart) UIStreamType() string { return "reasoning-end" }
func (p UIReasoningEndPart) UIStreamJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type string `json:"type"`
		ID   string `json:"id"`
	}{"reasoning-end", p.ID})
}

// --- Tool Execution Events ---

// UIToolInputStartPart signals the beginning of a tool invocation.
type UIToolInputStartPart struct {
	ToolCallID string `json:"toolCallId"`
	ToolName   string `json:"toolName"`
}

func (p UIToolInputStartPart) UIStreamType() string { return "tool-input-start" }
func (p UIToolInputStartPart) UIStreamJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type       string `json:"type"`
		ToolCallID string `json:"toolCallId"`
		ToolName   string `json:"toolName"`
	}{"tool-input-start", p.ToolCallID, p.ToolName})
}

// UIToolInputDeltaPart streams a chunk of tool input arguments.
type UIToolInputDeltaPart struct {
	ToolCallID     string `json:"toolCallId"`
	InputTextDelta string `json:"inputTextDelta"`
}

func (p UIToolInputDeltaPart) UIStreamType() string { return "tool-input-delta" }
func (p UIToolInputDeltaPart) UIStreamJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type           string `json:"type"`
		ToolCallID     string `json:"toolCallId"`
		InputTextDelta string `json:"inputTextDelta"`
	}{"tool-input-delta", p.ToolCallID, p.InputTextDelta})
}

// UIToolInputAvailablePart signals that complete tool input is ready.
type UIToolInputAvailablePart struct {
	ToolCallID string `json:"toolCallId"`
	ToolName   string `json:"toolName"`
	Input      any    `json:"input"`
}

func (p UIToolInputAvailablePart) UIStreamType() string { return "tool-input-available" }
func (p UIToolInputAvailablePart) UIStreamJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type       string `json:"type"`
		ToolCallID string `json:"toolCallId"`
		ToolName   string `json:"toolName"`
		Input      any    `json:"input"`
	}{"tool-input-available", p.ToolCallID, p.ToolName, p.Input})
}

// UIToolOutputAvailablePart delivers the result of a tool execution.
type UIToolOutputAvailablePart struct {
	ToolCallID string `json:"toolCallId"`
	Output     any    `json:"output"`
}

func (p UIToolOutputAvailablePart) UIStreamType() string { return "tool-output-available" }
func (p UIToolOutputAvailablePart) UIStreamJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type       string `json:"type"`
		ToolCallID string `json:"toolCallId"`
		Output     any    `json:"output"`
	}{"tool-output-available", p.ToolCallID, p.Output})
}

// --- Source Events ---

// UISourceURLPart provides a URL reference.
type UISourceURLPart struct {
	SourceID string `json:"sourceId"`
	URL      string `json:"url"`
	Title    string `json:"title,omitempty"`
}

func (p UISourceURLPart) UIStreamType() string { return "source-url" }
func (p UISourceURLPart) UIStreamJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type     string `json:"type"`
		SourceID string `json:"sourceId"`
		URL      string `json:"url"`
		Title    string `json:"title,omitempty"`
	}{"source-url", p.SourceID, p.URL, p.Title})
}

// --- File Events ---

// UIFilePart streams a file reference.
type UIFilePart struct {
	URL       string `json:"url"`
	MediaType string `json:"mediaType"`
}

func (p UIFilePart) UIStreamType() string { return "file" }
func (p UIFilePart) UIStreamJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type      string `json:"type"`
		URL       string `json:"url"`
		MediaType string `json:"mediaType"`
	}{"file", p.URL, p.MediaType})
}

// --- Step Events ---

// UIStartStepPart marks the beginning of a processing step.
type UIStartStepPart struct{}

func (p UIStartStepPart) UIStreamType() string { return "start-step" }
func (p UIStartStepPart) UIStreamJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type string `json:"type"`
	}{"start-step"})
}

// UIFinishStepPart marks the end of a processing step.
// AI SDK v6 strict schema: only { type: "finish-step" } allowed — no extra fields.
type UIFinishStepPart struct {
	// FinishReason and Usage are retained internally for the accumulator
	// but NOT serialized to the wire format.
	FinishReason FinishReason `json:"-"`
	Usage        *Usage       `json:"-"`
	IsContinued  bool         `json:"-"`
}

func (p UIFinishStepPart) UIStreamType() string { return "finish-step" }
func (p UIFinishStepPart) UIStreamJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type string `json:"type"`
	}{"finish-step"})
}

// --- Error Events ---

// UIErrorPart delivers an error to the client.
type UIErrorPart struct {
	ErrorText string `json:"errorText"`
}

func (p UIErrorPart) UIStreamType() string { return "error" }
func (p UIErrorPart) UIStreamJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type      string `json:"type"`
		ErrorText string `json:"errorText"`
	}{"error", p.ErrorText})
}

// --- Done Marker ---

// UIDonePart is the terminal [DONE] marker.
type UIDonePart struct{}

func (p UIDonePart) UIStreamType() string { return "[DONE]" }
func (p UIDonePart) UIStreamJSON() ([]byte, error) {
	return []byte("[DONE]"), nil
}

// UIMessageStream is a sequence of UIMessageStreamParts.
type UIMessageStream iter.Seq2[UIMessageStreamPart, error]

// WriteUIMessageStreamHeaders sets the required HTTP headers for the v6 UI Message Stream protocol.
func WriteUIMessageStreamHeaders(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache, no-transform")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("x-vercel-ai-ui-message-stream", "v1")
	w.WriteHeader(http.StatusOK)
}

// PipeUIMessageStream writes a UIMessageStream to a writer in SSE format.
// Each event is written as: "data: {json}\n\n"
// The stream terminates with: "data: [DONE]\n\n"
func (s UIMessageStream) Pipe(w io.Writer) error {
	flusher, ok := w.(http.Flusher)
	if !ok {
		flusher = nil
	}

	var pipeErr error
	s(func(part UIMessageStreamPart, err error) bool {
		if err != nil {
			pipeErr = err
			return false
		}

		data, err := part.UIStreamJSON()
		if err != nil {
			pipeErr = fmt.Errorf("failed to marshal %s event: %w", part.UIStreamType(), err)
			return false
		}

		_, err = fmt.Fprintf(w, "data: %s\n\n", data)
		if err != nil {
			pipeErr = err
			return false
		}
		if flusher != nil {
			flusher.Flush()
		}
		return true
	})
	return pipeErr
}

// DataStreamToUIMessageStream converts a v4 DataStream into a v6 UIMessageStream.
// It tracks text/reasoning block lifecycle (start/delta/end) and maps tool call
// events to their v6 equivalents.
func DataStreamToUIMessageStream(ds DataStream, messageID string) UIMessageStream {
	return func(yield func(UIMessageStreamPart, error) bool) {
		if messageID == "" {
			messageID = uuid.New().String()
		}

		// Emit start event
		if !yield(UIStartPart{MessageID: messageID}, nil) {
			return
		}

		// State tracking for text/reasoning block lifecycle
		var (
			textBlockID      string // "" means no active text block
			reasoningBlockID string // "" means no active reasoning block
			textPartCounter  int
		)

		genPartID := func(prefix string) string {
			textPartCounter++
			return fmt.Sprintf("%s_%d", prefix, textPartCounter)
		}

		// Helper to close active blocks
		closeTextBlock := func() bool {
			if textBlockID != "" {
				id := textBlockID
				textBlockID = ""
				return yield(UITextEndPart{ID: id}, nil)
			}
			return true
		}

		closeReasoningBlock := func() bool {
			if reasoningBlockID != "" {
				id := reasoningBlockID
				reasoningBlockID = ""
				return yield(UIReasoningEndPart{ID: id}, nil)
			}
			return true
		}

		closeAllBlocks := func() bool {
			if !closeTextBlock() {
				return false
			}
			return closeReasoningBlock()
		}

		var lastFinishReason FinishReason

		for part, err := range ds {
			if err != nil {
				yield(nil, err)
				return
			}

			switch p := part.(type) {
			case TextStreamPart:
				// Close reasoning if switching to text
				if reasoningBlockID != "" {
					if !closeReasoningBlock() {
						return
					}
				}
				// Open text block if needed
				if textBlockID == "" {
					textBlockID = genPartID("text")
					if !yield(UITextStartPart{ID: textBlockID}, nil) {
						return
					}
				}
				if !yield(UITextDeltaPart{ID: textBlockID, Delta: p.Content}, nil) {
					return
				}

			case ReasoningStreamPart:
				// Close text if switching to reasoning
				if textBlockID != "" {
					if !closeTextBlock() {
						return
					}
				}
				// Open reasoning block if needed
				if reasoningBlockID == "" {
					reasoningBlockID = genPartID("reasoning")
					if !yield(UIReasoningStartPart{ID: reasoningBlockID}, nil) {
						return
					}
				}
				if !yield(UIReasoningDeltaPart{ID: reasoningBlockID, Delta: p.Content}, nil) {
					return
				}

			case ToolCallStartStreamPart:
				if !closeAllBlocks() {
					return
				}
				if !yield(UIToolInputStartPart{
					ToolCallID: p.ToolCallID,
					ToolName:   p.ToolName,
				}, nil) {
					return
				}

			case ToolCallDeltaStreamPart:
				if !yield(UIToolInputDeltaPart{
					ToolCallID:     p.ToolCallID,
					InputTextDelta: p.ArgsTextDelta,
				}, nil) {
					return
				}

			case ToolCallStreamPart:
				if !closeAllBlocks() {
					return
				}
				if !yield(UIToolInputAvailablePart{
					ToolCallID: p.ToolCallID,
					ToolName:   p.ToolName,
					Input:      p.Args,
				}, nil) {
					return
				}

			case ToolResultStreamPart:
				if !yield(UIToolOutputAvailablePart{
					ToolCallID: p.ToolCallID,
					Output:     p.Result,
				}, nil) {
					return
				}

			case SourceStreamPart:
				if !yield(UISourceURLPart{
					SourceID: p.ID,
					URL:      p.URL,
					Title:    p.Title,
				}, nil) {
					return
				}

			case FileStreamPart:
				// v4 FileStreamPart has raw bytes, v6 expects URL.
				// Encode as data URI for compatibility.
				dataURI := fmt.Sprintf("data:%s;base64,", p.MimeType)
				if !yield(UIFilePart{
					URL:       dataURI, // Caller should provide URL-based files when possible
					MediaType: p.MimeType,
				}, nil) {
					return
				}

			case StartStepStreamPart:
				if !closeAllBlocks() {
					return
				}
				if !yield(UIStartStepPart{}, nil) {
					return
				}

			case FinishStepStreamPart:
				if !closeAllBlocks() {
					return
				}
				lastFinishReason = p.FinishReason
				if !yield(UIFinishStepPart{
					FinishReason: p.FinishReason,
					Usage:        &p.Usage,
					IsContinued:  p.IsContinued,
				}, nil) {
					return
				}

			case FinishMessageStreamPart:
				if !closeAllBlocks() {
					return
				}
				lastFinishReason = p.FinishReason
				// Don't emit finish here — we emit it after the loop
				// to ensure it's always the last event before [DONE]

			case ErrorStreamPart:
				closeAllBlocks()
				if !yield(UIErrorPart{ErrorText: p.Content}, nil) {
					return
				}
				// Terminate on error
				yield(UIDonePart{}, nil)
				return

			case WebSearchResultStreamPart:
				// Emit search results as a tool output (reuse tool-output-available)
				if !yield(UIToolOutputAvailablePart{
					ToolCallID: p.ToolCallID,
					Output: map[string]any{
						"type":    "web_search_results",
						"results": p.Results,
						"error":   p.Error,
					},
				}, nil) {
					return
				}

			case RedactedReasoningStreamPart, ReasoningSignatureStreamPart:
				// No v6 equivalent yet — skip silently

			default:
				// Unknown part type — skip
			}
		}

		// Close any remaining open blocks
		if !closeAllBlocks() {
			return
		}

		// Emit finish
		if !yield(UIFinishPart{
			FinishReason: lastFinishReason,
		}, nil) {
			return
		}

		// Emit [DONE]
		yield(UIDonePart{}, nil)
	}
}
