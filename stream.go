package aisdk

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"iter"
	"net/http"
	"strings"
	"time"
)

// Chat is the structure sent from `useChat` to the server.
// This can be extended if you'd like to send additional data with `body`.
type Chat struct {
	ID       string    `json:"id"`
	Messages []Message `json:"messages"`
}

// DataStream is a stream of DataStreamParts.
type DataStream iter.Seq2[DataStreamPart, error]

// WithToolCalling passes tool calls to the handleToolCall function.
func (s DataStream) WithToolCalling(handleToolCall func(toolCall ToolCall) any) DataStream {
	return func(yield func(DataStreamPart, error) bool) {
		// Track partial tool calls by ID
		partialToolCalls := make(map[string]struct {
			text     string
			step     int
			toolName string
		})

		// Track current step
		step := 0

		// Process a complete tool call
		processToolCall := func(id string, name string, args map[string]any) bool {
			if !yield(ToolCallStreamPart{
				ToolCallID: id,
				ToolName:   name,
				Args:       args,
			}, nil) {
				return false
			}

			// Call the handler and get the result
			result := handleToolCall(ToolCall{
				ID:   id,
				Name: name,
				Args: args,
			})

			// Yield the result
			return yield(ToolResultStreamPart{
				ToolCallID: id,
				Result:     result,
			}, nil)
		}

		// Process a tool call delta
		processDelta := func(id string, delta string) bool {
			partialCall := partialToolCalls[id]
			partialCall.text += delta
			partialToolCalls[id] = partialCall

			// Try to parse the partial JSON
			var args map[string]any
			if err := json.Unmarshal([]byte(partialCall.text), &args); err == nil {
				// Successfully parsed complete args, process the call
				if !processToolCall(id, partialCall.toolName, args) {
					return false
				}
				delete(partialToolCalls, id)
			}

			return true
		}

		for part, err := range s {
			if err != nil {
				yield(nil, err)
				return
			}

			if !yield(part, nil) {
				return
			}

			switch p := part.(type) {
			case StartStepStreamPart:
				// Keep track of step for tool calls
				step++

			case ToolCallStartStreamPart:
				// Skip server tool calls — they are handled by the API
				if p.IsServerTool {
					continue
				}
				// Initialize a new partial tool call
				partialToolCalls[p.ToolCallID] = struct {
					text     string
					step     int
					toolName string
				}{
					text:     "",
					step:     step,
					toolName: p.ToolName,
				}

			case ToolCallDeltaStreamPart:
				if !processDelta(p.ToolCallID, p.ArgsTextDelta) {
					return
				}

			case ToolCallStreamPart:
				// Skip server tool calls — they are handled by the API
				if p.IsServerTool {
					continue
				}
				if !processToolCall(p.ToolCallID, p.ToolName, p.Args) {
					return
				}
				delete(partialToolCalls, p.ToolCallID)

			case FinishStepStreamPart:
				// Clean up any remaining partial tool calls
				for id := range partialToolCalls {
					delete(partialToolCalls, id)
				}
			}
		}
	}
}

// WithAccumulator passes parts to the accumulator which aggregates them into a single message.
func (s DataStream) WithAccumulator(accumulator *DataStreamAccumulator) DataStream {
	return func(yield func(DataStreamPart, error) bool) {
		for part, err := range s {
			if err != nil {
				yield(nil, err)
				return
			}
			err = accumulator.Push(part)
			if err != nil {
				yield(nil, err)
				return
			}
			if !yield(part, nil) {
				return
			}
		}
	}
}

// Pipe iterates over the DataStream and writes the parts to the writer.
func (s DataStream) Pipe(w io.Writer) error {
	flusher, ok := w.(http.Flusher)
	if !ok {
		flusher = nil
	}

	var pipeErr error
	s(func(part DataStreamPart, err error) bool {
		if err != nil {
			pipeErr = err
			return false
		}
		formatted, err := part.Format()
		if err != nil {
			pipeErr = err
			return false
		}
		if formatted == "" {
			return true // Internal-only part (e.g. ReasoningStartStreamPart)
		}
		_, err = fmt.Fprint(w, formatted)
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

// DataStreamPart represents a part of the Vercel AI SDK data stream.
type DataStreamPart interface {
	Format() (string, error)
	TypeID() byte
}

// TextStreamPart corresponds to TYPE_ID '0'.
type TextStreamPart struct {
	Content string
}

func (p TextStreamPart) TypeID() byte { return '0' }
func (p TextStreamPart) Format() (string, error) {
	jsonContent, err := json.Marshal(p.Content)
	if err != nil {
		return "", fmt.Errorf("failed to marshal text content: %w", err)
	}
	return fmt.Sprintf("%c:%s\n", p.TypeID(), string(jsonContent)), nil
}

// ReasoningStreamPart corresponds to TYPE_ID 'g'.
type ReasoningStreamPart struct {
	Content string
}

func (p ReasoningStreamPart) TypeID() byte { return 'g' }
func (p ReasoningStreamPart) Format() (string, error) {
	jsonContent, err := json.Marshal(p.Content)
	if err != nil {
		return "", fmt.Errorf("failed to marshal reasoning content: %w", err)
	}
	return fmt.Sprintf("%c:%s\n", p.TypeID(), string(jsonContent)), nil
}

// ReasoningStartStreamPart is an internal-only marker emitted when a ThinkingBlock starts.
// It has no wire format (TypeID 0, empty Format) — it exists so downstream taps can detect
// the start of a reasoning block before the first ThinkingDelta arrives.
// ID is the block identifier assigned by the stream producer; if empty, downstream
// consumers (DataStreamToUIMessageStream, accumulator) generate one.
type ReasoningStartStreamPart struct {
	ID string
}

func (p ReasoningStartStreamPart) TypeID() byte              { return 0 }
func (p ReasoningStartStreamPart) Format() (string, error)   { return "", nil }

// RedactedReasoningStreamPart corresponds to TYPE_ID 'i'.
type RedactedReasoningStreamPart struct {
	Data string `json:"data"`
}

func (p RedactedReasoningStreamPart) TypeID() byte { return 'i' }
func (p RedactedReasoningStreamPart) Format() (string, error) {
	return formatJSONPart(p)
}

// ReasoningSignatureStreamPart corresponds to TYPE_ID 'j'.
type ReasoningSignatureStreamPart struct {
	Signature string `json:"signature"`
}

func (p ReasoningSignatureStreamPart) TypeID() byte { return 'j' }
func (p ReasoningSignatureStreamPart) Format() (string, error) {
	return formatJSONPart(p)
}

// SourceStreamPart corresponds to TYPE_ID 'h'.
type SourceStreamPart struct {
	SourceType     string `json:"sourceType"`
	ID             string `json:"id"`
	URL            string `json:"url"`
	Title          string `json:"title"`
	CitedText      string `json:"citedText,omitempty"`
	EncryptedIndex string `json:"encryptedIndex,omitempty"`
}

func (p SourceStreamPart) TypeID() byte { return 'h' }
func (p SourceStreamPart) Format() (string, error) {
	return formatJSONPart(p)
}

// FileStreamPart corresponds to TYPE_ID 'k'.
type FileStreamPart struct {
	Data     []byte `json:"data"`
	MimeType string `json:"mimeType"`
}

func (p FileStreamPart) TypeID() byte { return 'k' }
func (p FileStreamPart) Format() (string, error) {
	return formatJSONPart(p)
}

// DataStreamDataPart corresponds to TYPE_ID '2'.
type DataStreamDataPart struct {
	Content []any
}

func (p DataStreamDataPart) TypeID() byte { return '2' }
func (p DataStreamDataPart) Format() (string, error) {
	jsonContent, err := json.Marshal(p.Content)
	if err != nil {
		return "", fmt.Errorf("failed to marshal data content: %w", err)
	}
	return fmt.Sprintf("%c:%s\n", p.TypeID(), string(jsonContent)), nil
}

// MessageAnnotationStreamPart corresponds to TYPE_ID '8'.
type MessageAnnotationStreamPart struct {
	Content []any
}

func (p MessageAnnotationStreamPart) TypeID() byte { return '8' }
func (p MessageAnnotationStreamPart) Format() (string, error) {
	jsonContent, err := json.Marshal(p.Content)
	if err != nil {
		return "", fmt.Errorf("failed to marshal annotation content: %w", err)
	}
	return fmt.Sprintf("%c:%s\n", p.TypeID(), string(jsonContent)), nil
}

// WebSearchResultStreamPart is emitted when Anthropic's server-side web search returns results.
// This is NOT a standard Vercel AI SDK part — it's an Endure extension.
// It uses TYPE_ID 'w' (custom).
type WebSearchResultStreamPart struct {
	ToolCallID string            `json:"toolCallId"`
	Results    []WebSearchResult `json:"results,omitempty"`
	Error      string            `json:"error,omitempty"`
}

func (p WebSearchResultStreamPart) TypeID() byte { return 'w' }
func (p WebSearchResultStreamPart) Format() (string, error) {
	return formatJSONPart(p)
}

// ErrorStreamPart corresponds to TYPE_ID '3'.
type ErrorStreamPart struct {
	Content string
}

func (p ErrorStreamPart) TypeID() byte { return '3' }
func (p ErrorStreamPart) Format() (string, error) {
	jsonContent, err := json.Marshal(p.Content)
	if err != nil {
		return "", fmt.Errorf("failed to marshal error content: %w", err)
	}
	return fmt.Sprintf("%c:%s\n", p.TypeID(), string(jsonContent)), nil
}

// ToolCall represents a tool call *request*.
type ToolCall struct {
	ID   string         `json:"id"`
	Name string         `json:"name"`
	Args map[string]any `json:"args"`
}

type ToolCallResult interface {
	Part | []Part | any
}

// ToolCallStartStreamPart corresponds to TYPE_ID 'b'.
type ToolCallStartStreamPart struct {
	ToolCallID       string `json:"toolCallId"`
	ToolName         string `json:"toolName"`
	IsServerTool     bool   `json:"isServerTool,omitempty"` // True for Anthropic server tools (web_search)
	ThoughtSignature []byte `json:"thoughtSignature,omitempty"` // Gemini 3.x reasoning signature
}

func (p ToolCallStartStreamPart) TypeID() byte { return 'b' }
func (p ToolCallStartStreamPart) Format() (string, error) {
	return formatJSONPart(p)
}

// ToolCallDeltaStreamPart corresponds to TYPE_ID 'c'.
type ToolCallDeltaStreamPart struct {
	ToolCallID    string `json:"toolCallId"`
	ArgsTextDelta string `json:"argsTextDelta"`
}

func (p ToolCallDeltaStreamPart) TypeID() byte { return 'c' }
func (p ToolCallDeltaStreamPart) Format() (string, error) {
	return formatJSONPart(p)
}

// ToolCallStreamPart corresponds to TYPE_ID '9'.
type ToolCallStreamPart struct {
	ToolCallID   string         `json:"toolCallId"`
	ToolName     string         `json:"toolName"`
	Args         map[string]any `json:"args"`
	IsServerTool bool           `json:"isServerTool,omitempty"` // True for Anthropic server tools (web_search)
}

func (p ToolCallStreamPart) TypeID() byte { return '9' }
func (p ToolCallStreamPart) Format() (string, error) {
	return formatJSONPart(p)
}

// ToolResultStreamPart corresponds to TYPE_ID 'a'.
type ToolResultStreamPart struct {
	ToolCallID string `json:"toolCallId"`
	Result     any    `json:"result"`
}

func (p ToolResultStreamPart) TypeID() byte { return 'a' }
func (p ToolResultStreamPart) Format() (string, error) {
	return formatJSONPart(p)
}

// StartStepStreamPart corresponds to TYPE_ID 'f'.
type StartStepStreamPart struct {
	MessageID string `json:"messageId"`
}

func (p StartStepStreamPart) TypeID() byte { return 'f' }
func (p StartStepStreamPart) Format() (string, error) {
	return formatJSONPart(p)
}

// FinishReason defines the possible reasons for finishing a step or message.
type FinishReason string

const (
	FinishReasonStop          FinishReason = "stop"
	FinishReasonLength        FinishReason = "length"
	FinishReasonContentFilter FinishReason = "content-filter"
	FinishReasonToolCalls     FinishReason = "tool-calls"
	FinishReasonPauseTurn     FinishReason = "pause-turn"
	FinishReasonError         FinishReason = "error"
	FinishReasonOther         FinishReason = "other"
	FinishReasonUnknown       FinishReason = "unknown"
)

// Usage details the token usage.
type Usage struct {
	PromptTokens             *int64 `json:"promptTokens"`
	CompletionTokens         *int64 `json:"completionTokens"`
	CacheReadInputTokens     *int64 `json:"cacheReadInputTokens,omitempty"`
	CacheCreationInputTokens *int64 `json:"cacheCreationInputTokens,omitempty"`
}

// FinishStepStreamPart corresponds to TYPE_ID 'e'.
type FinishStepStreamPart struct {
	FinishReason FinishReason `json:"finishReason"`
	Usage        Usage        `json:"usage"`
	IsContinued  bool         `json:"isContinued"`
}

func (p FinishStepStreamPart) TypeID() byte { return 'e' }
func (p FinishStepStreamPart) Format() (string, error) {
	return formatJSONPart(p)
}

// FinishMessageStreamPart corresponds to TYPE_ID 'd'.
type FinishMessageStreamPart struct {
	FinishReason FinishReason `json:"finishReason"`
	Usage        Usage        `json:"usage"`
}

func (p FinishMessageStreamPart) TypeID() byte { return 'd' }
func (p FinishMessageStreamPart) Format() (string, error) {
	return formatJSONPart(p)
}

func formatJSONPart(part DataStreamPart) (string, error) {
	jsonData, err := json.Marshal(part)
	if err != nil {
		return "", fmt.Errorf("failed to marshal part type %T: %w", part, err)
	}
	return fmt.Sprintf("%c:%s\n", part.TypeID(), string(jsonData)), nil
}

type Attachment struct {
	Name        string `json:"name,omitempty"`
	ContentType string `json:"contentType,omitempty"`
	URL         string `json:"url"`

	// TextContent holds pre-fetched text for text/* attachments (populated by the server).
	TextContent string `json:"textContent,omitempty"`
	// Bytes holds pre-fetched binary content for non-text attachments (populated by the server).
	// When set, converters use this instead of parsing the URL.
	Bytes []byte `json:"bytes,omitempty"`

	// Kind classifies the attachment origin (e.g. "pasted_text", "file").
	// Populated by the server so clients can render collapse chips on reload.
	Kind string `json:"kind,omitempty"`
	// Chars is the character count of the original text content (pasted_text only).
	Chars int `json:"chars,omitempty"`
	// Lines is the line count of the original text content (pasted_text only).
	Lines int `json:"lines,omitempty"`
}

type Message struct {
	ID          string           `json:"id"`
	CreatedAt   *json.RawMessage `json:"createdAt,omitempty"`
	Content     string           `json:"content"`
	Role        string           `json:"role"`
	Parts       []Part           `json:"parts,omitempty"`
	Annotations []any            `json:"annotations,omitempty"`
	Attachments []Attachment     `json:"experimental_attachments,omitempty"`
}

type PartType string

const (
	PartTypeText              PartType = "text"
	PartTypeReasoning         PartType = "reasoning"
	PartTypeToolInvocation    PartType = "tool-invocation"
	PartTypeSource            PartType = "source"
	PartTypeFile              PartType = "file"
	PartTypeStepStart          PartType = "step-start"
	PartTypeServerToolUse      PartType = "server-tool-use"
	PartTypeWebSearchResult    PartType = "web-search-result"
	PartTypeToolSearchResult   PartType = "tool-search-result"
)

type ReasoningDetail struct {
	Type      string `json:"type"`
	Text      string `json:"text,omitempty"`
	Signature string `json:"signature,omitempty"`
	Data      string `json:"data,omitempty"`
}

type SourceInfo struct {
	URI         string         `json:"uri,omitempty"`
	ContentType string         `json:"contentType,omitempty"`
	Data        string         `json:"data,omitempty"`
	Metadata    map[string]any `json:"metadata,omitempty"`
}

type Part struct {
	Type PartType `json:"type"`

	// ID is the block identifier for reasoning/text parts, or toolCallId for tool-invocation parts.
	// Used to correlate headlines, timestamps, and persisted parts across live stream and reload.
	ID string `json:"id,omitempty"`

	// Type: "text"
	Text     string     `json:"text,omitempty"`
	Citations []Citation `json:"citations,omitempty"` // Inline citations from web search

	// Type: "reasoning"
	Reasoning string            `json:"reasoning,omitempty"`
	Details   []ReasoningDetail `json:"details,omitempty"`

	// Type: "tool-invocation"
	ToolInvocation *ToolInvocation `json:"toolInvocation,omitempty"`

	// Type: "source"
	Source *SourceInfo `json:"source,omitempty"`

	// Type: "file"
	MimeType string `json:"mimeType,omitempty"`
	Data     []byte `json:"data,omitempty"`

	// Type: "server-tool-use" — Anthropic server tool invocation
	ServerToolUseID   string `json:"serverToolUseId,omitempty"`
	ServerToolName    string `json:"serverToolName,omitempty"`
	ServerToolInput   any    `json:"serverToolInput,omitempty"`

	// Type: "web-search-result" — Results from Anthropic native web search
	WebSearchToolUseID string              `json:"webSearchToolUseId,omitempty"`
	WebSearchResults   []WebSearchResult   `json:"webSearchResults,omitempty"`
	WebSearchError     string              `json:"webSearchError,omitempty"`
	RawContentJSON     string              `json:"-"` // Preserves encrypted_content for multi-turn round-trip

	// Type: "tool-search-result" — Results from Anthropic tool search
	ToolSearchToolUseID  string                `json:"toolSearchToolUseId,omitempty"`
	ToolSearchReferences []ToolSearchReference `json:"toolSearchReferences,omitempty"`
	ToolSearchError      string                `json:"toolSearchError,omitempty"`
	ToolSearchErrorMsg   string                `json:"toolSearchErrorMsg,omitempty"`

	// CacheControl signals the Anthropic adapter to set cache_control: ephemeral on this block.
	CacheControl bool `json:"-"`

	// Timing — server wall-clock milliseconds (Unix epoch). Present on reasoning and tool-invocation parts.
	StartedMs int64 `json:"startedMs,omitempty"`
	EndedMs   int64 `json:"endedMs,omitempty"`

	// Type: "step-start" - No additional fields

	isComplete bool `json:"-"` // Internal accumulator tracking
}

// ToolSearchReference represents a tool discovered by Anthropic's tool search.
type ToolSearchReference struct {
	ToolName string `json:"tool_name"`
}

// ToolSearchResultStreamPart is emitted when Anthropic's server-side tool search returns results.
// This is an Endure extension, TYPE_ID 'x' (custom).
type ToolSearchResultStreamPart struct {
	ToolCallID string                `json:"toolCallId"`
	References []ToolSearchReference `json:"references,omitempty"`
	Error      string                `json:"error,omitempty"`
	ErrorMsg   string                `json:"errorMsg,omitempty"`
}

func (p ToolSearchResultStreamPart) TypeID() byte { return 'x' }
func (p ToolSearchResultStreamPart) Format() (string, error) {
	return formatJSONPart(p)
}

// WebSearchResult represents a single search result from Anthropic's native web search.
type WebSearchResult struct {
	URL              string `json:"url"`
	Title            string `json:"title"`
	EncryptedContent string `json:"encrypted_content,omitempty"` // Must be preserved for multi-turn
	PageAge          string `json:"page_age,omitempty"`
}

type Tool struct {
	Name         string `json:"name"`
	Description  string `json:"description"`
	Schema       Schema `json:"parameters"`
	DeferLoading bool   `json:"defer_loading,omitempty"` // Anthropic tool search: defer loading into context
}

// ServerTool defines an Anthropic server-side tool (e.g., web_search).
// These are executed by Anthropic's API, not by the client.
type ServerTool struct {
	Type           string            `json:"type"`                      // e.g. "web_search_20250305"
	Name           string            `json:"name"`                      // e.g. "web_search"
	MaxUses        *int64            `json:"max_uses,omitempty"`        // Max searches per request
	AllowedDomains []string          `json:"allowed_domains,omitempty"` // Mutually exclusive with BlockedDomains
	BlockedDomains []string          `json:"blocked_domains,omitempty"` // Mutually exclusive with AllowedDomains
	UserLocation   *UserLocation     `json:"user_location,omitempty"`   // Approximate user location
}

// UserLocation provides approximate user location for search relevance.
type UserLocation struct {
	Type     string `json:"type"`               // "approximate"
	City     string `json:"city,omitempty"`
	Region   string `json:"region,omitempty"`
	Country  string `json:"country,omitempty"`
	Timezone string `json:"timezone,omitempty"`
}

// Citation represents an inline citation from a web search result.
type Citation struct {
	Type           string `json:"type"`                      // "web_search_result_location"
	URL            string `json:"url"`
	Title          string `json:"title,omitempty"`
	EncryptedIndex string `json:"encrypted_index,omitempty"` // Must be preserved for multi-turn
	CitedText      string `json:"cited_text,omitempty"`
}

type Schema struct {
	Required   []string       `json:"required"`
	Properties map[string]any `json:"properties"`
}

type ToolInvocationState string

const (
	ToolInvocationStateCall        ToolInvocationState = "call"
	ToolInvocationStatePartialCall ToolInvocationState = "partial-call"
	ToolInvocationStateResult      ToolInvocationState = "result"
)

type ToolInvocation struct {
	State      ToolInvocationState `json:"state"`
	Step       *int                `json:"step,omitempty"`
	ToolCallID string              `json:"toolCallId"`
	ToolName   string              `json:"toolName"`
	Args       any                 `json:"args"`
	Result     any                 `json:"result,omitempty"`

	// ThoughtSignature preserves Gemini 3.x's opaque reasoning signature.
	// Must be sent back unchanged with the FunctionCall part on the next turn.
	ThoughtSignature []byte `json:"thoughtSignature,omitempty"`
}

func WriteDataStreamHeaders(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("X-Vercel-AI-Data-Stream", "v1")
	w.WriteHeader(http.StatusOK)
}

// DataStreamAccumulator accumulates DataStreamParts into Messages.
type DataStreamAccumulator struct {
	messages       []Message
	currentMessage *Message
	wipToolCalls   map[string]*Part // Keyed by ToolCallID, points to Part in currentMessage.Parts
	finishReason   FinishReason
	usage          Usage

	// Per-block reasoning tracking: index of the current reasoning Part in currentMessage.Parts.
	// -1 means no active reasoning block.
	currentReasoningIdx int
	reasoningBlockCount int // counter for generating IDs when ReasoningStartStreamPart.ID is empty
}

func (a *DataStreamAccumulator) ensureCurrentMessage() {
	if a.currentMessage == nil {
		a.currentMessage = &Message{
			Role:  "assistant",
			Parts: make([]Part, 0, 5),
		}
		a.wipToolCalls = make(map[string]*Part)
		a.currentReasoningIdx = -1
	}
}

func (a *DataStreamAccumulator) findPart(toolCallID string) *Part {
	if a.currentMessage == nil {
		return nil
	}
	for i := range a.currentMessage.Parts {
		if a.currentMessage.Parts[i].Type == PartTypeToolInvocation &&
			a.currentMessage.Parts[i].ToolInvocation != nil &&
			a.currentMessage.Parts[i].ToolInvocation.ToolCallID == toolCallID {
			return &a.currentMessage.Parts[i]
		}
	}
	return nil
}

func (a *DataStreamAccumulator) Push(part DataStreamPart) error {
	if _, isFinal := part.(FinishMessageStreamPart); !isFinal {
		a.ensureCurrentMessage()
	}

	var currentMsgPtr *Message
	if a.currentMessage != nil {
		currentMsgPtr = a.currentMessage
	}

	switch p := part.(type) {
	case TextStreamPart:
		if currentMsgPtr == nil {
			return fmt.Errorf("cannot add TextStreamPart without an active message")
		}
		// Close any active reasoning block (text after reasoning ends the block)
		if a.currentReasoningIdx >= 0 && a.currentReasoningIdx < len(currentMsgPtr.Parts) {
			rp := &currentMsgPtr.Parts[a.currentReasoningIdx]
			if rp.EndedMs == 0 {
				rp.EndedMs = time.Now().UnixMilli()
			}
			a.currentReasoningIdx = -1
		}
		currentMsgPtr.Content += p.Content
		numParts := len(currentMsgPtr.Parts)
		if numParts > 0 && currentMsgPtr.Parts[numParts-1].Type == PartTypeText {
			currentMsgPtr.Parts[numParts-1].Text += p.Content
		} else {
			currentMsgPtr.Parts = append(currentMsgPtr.Parts, Part{
				Type:      PartTypeText,
				Text:      p.Content,
				StartedMs: time.Now().UnixMilli(),
			})
		}

	case ReasoningStreamPart:
		if currentMsgPtr == nil {
			return fmt.Errorf("cannot add ReasoningStreamPart without an active message")
		}
		// If no active reasoning block, start one (handles providers that emit
		// ReasoningStreamPart without a preceding ReasoningStartStreamPart).
		if a.currentReasoningIdx < 0 || a.currentReasoningIdx >= len(currentMsgPtr.Parts) {
			a.reasoningBlockCount++
			id := fmt.Sprintf("reasoning_%d", a.reasoningBlockCount)
			currentMsgPtr.Parts = append(currentMsgPtr.Parts, Part{
				Type:      PartTypeReasoning,
				ID:        id,
				StartedMs: time.Now().UnixMilli(),
			})
			a.currentReasoningIdx = len(currentMsgPtr.Parts) - 1
		}
		currentMsgPtr.Parts[a.currentReasoningIdx].Reasoning += p.Content

	case FileStreamPart:
		if currentMsgPtr == nil {
			return fmt.Errorf("cannot add FileStreamPart without an active message")
		}
		currentMsgPtr.Parts = append(currentMsgPtr.Parts, Part{
			Type:     PartTypeFile,
			MimeType: p.MimeType,
			Data:     p.Data,
		})

	case SourceStreamPart:
		if currentMsgPtr == nil {
			return fmt.Errorf("cannot add SourceStreamPart without an active message")
		}
		// Attach citation to the most recent text part (inline Perplexity-style)
		citation := Citation{
			Type:           p.SourceType,
			URL:            p.URL,
			Title:          p.Title,
			CitedText:      p.CitedText,
			EncryptedIndex: p.EncryptedIndex,
		}
		attachedToText := false
		for i := len(currentMsgPtr.Parts) - 1; i >= 0; i-- {
			if currentMsgPtr.Parts[i].Type == PartTypeText {
				currentMsgPtr.Parts[i].Citations = append(currentMsgPtr.Parts[i].Citations, citation)
				attachedToText = true
				break
			}
		}
		// Also emit as a standalone source part for backward compatibility
		currentMsgPtr.Parts = append(currentMsgPtr.Parts, Part{
			Type: PartTypeSource,
			Source: &SourceInfo{
				URI:         p.URL,
				ContentType: "",
				Data:        "",
				Metadata:    map[string]any{"id": p.ID, "title": p.Title, "sourceType": p.SourceType, "citedText": p.CitedText},
			},
		})
		_ = attachedToText

	case StartStepStreamPart:
		if currentMsgPtr == nil {
			return fmt.Errorf("StartStepStreamPart received before message initialization")
		}
		if currentMsgPtr.ID == "" {
			currentMsgPtr.ID = p.MessageID
		}
		currentMsgPtr.Parts = append(currentMsgPtr.Parts, Part{Type: PartTypeStepStart})

	case ToolCallStartStreamPart:
		if currentMsgPtr == nil {
			return fmt.Errorf("cannot add ToolCallStartStreamPart without an active message")
		}
		// Close any active reasoning block
		if a.currentReasoningIdx >= 0 && a.currentReasoningIdx < len(currentMsgPtr.Parts) {
			rp := &currentMsgPtr.Parts[a.currentReasoningIdx]
			if rp.EndedMs == 0 {
				rp.EndedMs = time.Now().UnixMilli()
			}
			a.currentReasoningIdx = -1
		}
		// Initialize a new tool call. Args starts as empty string for delta
		// concatenation during streaming; converted to object before API replay.
		newPart := Part{
			Type: PartTypeToolInvocation,
			ToolInvocation: &ToolInvocation{
				State:            ToolInvocationStatePartialCall,
				ToolCallID:       p.ToolCallID,
				ToolName:         p.ToolName,
				Args:             "",
				ThoughtSignature: p.ThoughtSignature,
			},
			StartedMs:  time.Now().UnixMilli(),
			isComplete: false,
		}
		currentMsgPtr.Parts = append(currentMsgPtr.Parts, newPart)
		a.wipToolCalls[p.ToolCallID] = &currentMsgPtr.Parts[len(currentMsgPtr.Parts)-1]

	case ToolCallDeltaStreamPart:
		if currentMsgPtr == nil {
			return fmt.Errorf("cannot add ToolCallDeltaStreamPart without an active message")
		}
		wipCallPart, exists := a.wipToolCalls[p.ToolCallID]
		if exists && wipCallPart.ToolInvocation != nil {
			if argsStr, ok := wipCallPart.ToolInvocation.Args.(string); ok {
				wipCallPart.ToolInvocation.Args = argsStr + p.ArgsTextDelta
			} else {
				return fmt.Errorf("tool call delta received for non-string args (ID: %s)", p.ToolCallID)
			}
		}

	case ToolCallStreamPart:
		if currentMsgPtr == nil {
			return fmt.Errorf("cannot add ToolCallStreamPart without an active message")
		}
		// Update or create tool call
		existingPart := a.findPart(p.ToolCallID)
		if existingPart != nil && existingPart.ToolInvocation != nil {
			existingPart.ToolInvocation.ToolName = p.ToolName
			existingPart.ToolInvocation.Args = p.Args
			existingPart.ToolInvocation.State = ToolInvocationStateCall
			existingPart.isComplete = true
		} else {
			currentMsgPtr.Parts = append(currentMsgPtr.Parts, Part{
				Type: PartTypeToolInvocation,
				ToolInvocation: &ToolInvocation{
					State:      ToolInvocationStateCall,
					ToolCallID: p.ToolCallID,
					ToolName:   p.ToolName,
					Args:       p.Args,
				},
				isComplete: true,
			})
		}
		delete(a.wipToolCalls, p.ToolCallID)

	case ToolResultStreamPart:
		if currentMsgPtr == nil {
			return fmt.Errorf("cannot add ToolResultStreamPart without an active message")
		}
		// Find and update existing tool call with result
		existingPart := a.findPart(p.ToolCallID)
		if existingPart != nil && existingPart.ToolInvocation != nil {
			existingPart.ToolInvocation.State = ToolInvocationStateResult
			existingPart.ToolInvocation.Result = p.Result
			existingPart.EndedMs = time.Now().UnixMilli()
		} else {
			return fmt.Errorf("tool result received for unknown tool call ID: %s", p.ToolCallID)
		}

	case DataStreamDataPart:
		if currentMsgPtr == nil {
			return fmt.Errorf("cannot add DataStreamDataPart without an active message")
		}
		currentMsgPtr.Annotations = append(currentMsgPtr.Annotations, p.Content...)

	case MessageAnnotationStreamPart:
		if currentMsgPtr == nil {
			return fmt.Errorf("cannot add MessageAnnotationStreamPart without an active message")
		}
		currentMsgPtr.Annotations = append(currentMsgPtr.Annotations, p.Content...)

	case FinishStepStreamPart:
		if currentMsgPtr != nil {
			// Close any open reasoning block
			if a.currentReasoningIdx >= 0 && a.currentReasoningIdx < len(currentMsgPtr.Parts) {
				rp := &currentMsgPtr.Parts[a.currentReasoningIdx]
				if rp.EndedMs == 0 {
					rp.EndedMs = time.Now().UnixMilli()
				}
				a.currentReasoningIdx = -1
			}
			// Clean up any remaining WIP tool calls
			for id, wipCallPart := range a.wipToolCalls {
				if !wipCallPart.isComplete && wipCallPart.ToolInvocation != nil {
					if argsStr, ok := wipCallPart.ToolInvocation.Args.(string); ok && argsStr != "" {
						var parsedArgs map[string]any
						if json.Unmarshal([]byte(argsStr), &parsedArgs) == nil {
							wipCallPart.ToolInvocation.Args = parsedArgs
							wipCallPart.ToolInvocation.State = ToolInvocationStateCall
						}
					}
					wipCallPart.isComplete = true
				}
				delete(a.wipToolCalls, id)
			}

			if !p.IsContinued {
				a.messages = append(a.messages, *currentMsgPtr)
				a.currentMessage = nil
				a.wipToolCalls = nil
			}
		}
		a.finishReason = p.FinishReason

	case FinishMessageStreamPart:
		if currentMsgPtr != nil {
			// Clean up any remaining WIP tool calls
			for _, wipCallPart := range a.wipToolCalls {
				if !wipCallPart.isComplete && wipCallPart.ToolInvocation != nil {
					if argsStr, ok := wipCallPart.ToolInvocation.Args.(string); ok && argsStr != "" {
						var parsedArgs map[string]any
						if json.Unmarshal([]byte(argsStr), &parsedArgs) == nil {
							wipCallPart.ToolInvocation.Args = parsedArgs
							wipCallPart.ToolInvocation.State = ToolInvocationStateCall
						}
					}
					wipCallPart.isComplete = true
				}
			}
			a.messages = append(a.messages, *currentMsgPtr)
		}
		a.finishReason = p.FinishReason
		a.currentMessage = nil
		a.wipToolCalls = nil
		a.usage = p.Usage

	case ErrorStreamPart:
		a.finishReason = FinishReasonError
		return fmt.Errorf("error in stream: %s", p.Content)

	case ToolSearchResultStreamPart:
		if currentMsgPtr == nil {
			return fmt.Errorf("cannot add ToolSearchResultStreamPart without an active message")
		}
		part := Part{
			Type:                 PartTypeToolSearchResult,
			ToolSearchToolUseID:  p.ToolCallID,
			ToolSearchReferences: p.References,
			ToolSearchError:      p.Error,
			ToolSearchErrorMsg:   p.ErrorMsg,
		}
		currentMsgPtr.Parts = append(currentMsgPtr.Parts, part)

	case WebSearchResultStreamPart:
		if currentMsgPtr == nil {
			return fmt.Errorf("cannot add WebSearchResultStreamPart without an active message")
		}
		part := Part{
			Type:               PartTypeWebSearchResult,
			WebSearchToolUseID: p.ToolCallID,
			WebSearchResults:   p.Results,
			WebSearchError:     p.Error,
		}
		currentMsgPtr.Parts = append(currentMsgPtr.Parts, part)

	case ReasoningSignatureStreamPart:
		if currentMsgPtr == nil {
			return fmt.Errorf("cannot add ReasoningSignatureStreamPart without an active message")
		}
		// Attach signature to the current (last) reasoning block.
		// The signature marks the end of a thinking block, so we wrap accumulated
		// reasoning text into a detail entry with the signature attached.
		idx := a.currentReasoningIdx
		if idx < 0 {
			// Fallback: find last reasoning part
			for i := len(currentMsgPtr.Parts) - 1; i >= 0; i-- {
				if currentMsgPtr.Parts[i].Type == PartTypeReasoning {
					idx = i
					break
				}
			}
		}
		if idx >= 0 && idx < len(currentMsgPtr.Parts) {
			rp := &currentMsgPtr.Parts[idx]
			rp.Details = append(rp.Details, ReasoningDetail{
				Type:      "text",
				Text:      rp.Reasoning,
				Signature: p.Signature,
			})
			// Close the block — signature marks end
			if rp.EndedMs == 0 {
				rp.EndedMs = time.Now().UnixMilli()
			}
			a.currentReasoningIdx = -1
		}

	case RedactedReasoningStreamPart:
		if currentMsgPtr == nil {
			return fmt.Errorf("cannot add RedactedReasoningStreamPart without an active message")
		}
		// Attach to current reasoning block, or create one if none active.
		idx := a.currentReasoningIdx
		if idx < 0 {
			// No active block — create one for the redacted detail
			a.reasoningBlockCount++
			id := fmt.Sprintf("reasoning_%d", a.reasoningBlockCount)
			currentMsgPtr.Parts = append(currentMsgPtr.Parts, Part{
				Type:      PartTypeReasoning,
				ID:        id,
				StartedMs: time.Now().UnixMilli(),
			})
			idx = len(currentMsgPtr.Parts) - 1
			a.currentReasoningIdx = idx
		}
		currentMsgPtr.Parts[idx].Details = append(currentMsgPtr.Parts[idx].Details, ReasoningDetail{
			Type: "redacted",
			Data: p.Data,
		})

	case ReasoningStartStreamPart:
		if currentMsgPtr == nil {
			break
		}
		// Close previous reasoning block if still open
		if a.currentReasoningIdx >= 0 && a.currentReasoningIdx < len(currentMsgPtr.Parts) {
			rp := &currentMsgPtr.Parts[a.currentReasoningIdx]
			if rp.EndedMs == 0 {
				rp.EndedMs = time.Now().UnixMilli()
			}
		}
		// Start a new reasoning block
		id := p.ID
		if id == "" {
			a.reasoningBlockCount++
			id = fmt.Sprintf("reasoning_%d", a.reasoningBlockCount)
		}
		currentMsgPtr.Parts = append(currentMsgPtr.Parts, Part{
			Type:      PartTypeReasoning,
			ID:        id,
			StartedMs: time.Now().UnixMilli(),
		})
		a.currentReasoningIdx = len(currentMsgPtr.Parts) - 1

	default:
		return fmt.Errorf("unhandled part type: %T", part)
	}

	return nil
}

func (a *DataStreamAccumulator) Messages() []Message {
	return a.messages
}

func (a *DataStreamAccumulator) FinishReason() FinishReason {
	return a.finishReason
}

func (a *DataStreamAccumulator) Usage() Usage {
	return a.usage
}

// attachmentBytes returns the binary content of an attachment.
// Prefers the pre-fetched Bytes field; falls back to parsing a data-URI URL.
func attachmentBytes(a Attachment) ([]byte, error) {
	if len(a.Bytes) > 0 {
		return a.Bytes, nil
	}
	parts := strings.SplitN(a.URL, ",", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("attachment has no Bytes and invalid data URI: %s", a.URL)
	}
	decoded, err := base64.StdEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, fmt.Errorf("failed to decode attachment base64: %w", err)
	}
	return decoded, nil
}

// attachmentBase64 returns base64-encoded content for an attachment.
// Prefers the pre-fetched Bytes field; falls back to parsing a data-URI URL.
func attachmentBase64(a Attachment) (string, error) {
	if len(a.Bytes) > 0 {
		return base64.StdEncoding.EncodeToString(a.Bytes), nil
	}
	parts := strings.SplitN(a.URL, ",", 2)
	if len(parts) != 2 {
		return "", fmt.Errorf("attachment has no Bytes and invalid data URI: %s", a.URL)
	}
	return parts[1], nil
}

func toolResultToParts(result any) ([]Part, error) {
	switch r := result.(type) {
	case []Part:
		return r, nil
	case Part:
		return []Part{r}, nil
	default:
		jsonData, err := json.Marshal(r)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal tool call result: %w", err)
		}
		return []Part{{Type: PartTypeText, Text: string(jsonData)}}, nil
	}
}
