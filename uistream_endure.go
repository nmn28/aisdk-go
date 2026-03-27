package aisdk

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// EndureStreamOptions configures Endure-specific stream behavior.
type EndureStreamOptions struct {
	// ThreadID is returned via X-Thread-Id header. Required.
	ThreadID string

	// StreamID is returned via X-Stream-Id header for durable stream resumption.
	StreamID string

	// MessageID is returned via X-Message-Id header.
	MessageID string

	// Title is injected as a custom "t:" event at the end of the stream,
	// before the finish/[DONE] events. This allows iOS clients to receive
	// the auto-generated title without an extra API call.
	// If empty, no title event is injected.
	Title string

	// StreamMode controls buffering behavior. Set from the x-stream-mode request header.
	// "realtime" = flush immediately (web), "buffered" = buffer first (iOS).
	StreamMode string
}

// UIStatusPart is a transient status indicator sent during streaming so the
// frontend can show "Thinking..." / "Searching..." UI.  It uses the v6
// custom data event convention (type starts with "data-").
//
// Values: "thinking" (initial), "searching" (tool loop continuing).
type UIStatusPart struct {
	Status string
}

func (p UIStatusPart) UIStreamType() string { return "data-status" }
func (p UIStatusPart) UIStreamJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type      string `json:"type"`
		Data      string `json:"data"`
		Transient bool   `json:"transient"`
	}{"data-status", p.Status, true})
}

// UITitlePart is an Endure-specific custom event that injects a thread title
// into the stream. Uses the "t:" prefix for backwards compatibility with the
// existing corapinew protocol.
type UITitlePart struct {
	Title    string `json:"title"`
	ThreadID string `json:"threadId"`
}

func (p UITitlePart) UIStreamType() string { return "data-endure-title" }
func (p UITitlePart) UIStreamJSON() ([]byte, error) {
	return json.Marshal(struct {
		Type     string `json:"type"`
		Title    string `json:"title"`
		ThreadID string `json:"threadId"`
	}{"data-endure-title", p.Title, p.ThreadID})
}

// FormatTitleEvent returns the raw "t:" format used by corapinew for
// backwards-compatible title injection into the SSE stream.
func (p UITitlePart) FormatTitleEvent() string {
	data, _ := json.Marshal(struct {
		Title    string `json:"title"`
		ThreadID string `json:"threadId"`
	}{p.Title, p.ThreadID})
	return fmt.Sprintf("t:%s\n", string(data))
}

// WriteEndureStreamHeaders sets all required HTTP headers for Endure's AI chat streaming.
// This combines the standard v6 UI Message Stream headers with Endure's custom headers.
func WriteEndureStreamHeaders(w http.ResponseWriter, opts EndureStreamOptions) {
	// Standard v6 protocol headers
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache, no-transform")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("x-vercel-ai-ui-message-stream", "v1")

	// Endure custom headers
	if opts.ThreadID != "" {
		w.Header().Set("X-Thread-Id", opts.ThreadID)
	}
	if opts.StreamID != "" {
		w.Header().Set("X-Stream-Id", opts.StreamID)
	}
	if opts.MessageID != "" {
		w.Header().Set("X-Message-Id", opts.MessageID)
	}

	w.WriteHeader(http.StatusOK)
}

// WithTitleInjection wraps a UIMessageStream to inject a title event
// just before the finish/[DONE] events. This matches corapinew's behavior
// in chat-original.ts lines 1159-1188.
//
// The titleFunc is called when the stream reaches the finish event.
// It can block (e.g., waiting for async title generation) without
// affecting the rest of the stream — earlier events are already flushed.
func WithTitleInjection(stream UIMessageStream, titleFunc func() (title string, threadID string)) UIMessageStream {
	if titleFunc == nil {
		return stream
	}

	return func(yield func(UIMessageStreamPart, error) bool) {
		// Buffer the last two events (finish + done) so we can inject title before them
		var pendingFinish *UIFinishPart
		var sawDone bool

		stream(func(part UIMessageStreamPart, err error) bool {
			if err != nil {
				return yield(part, err)
			}

			switch p := part.(type) {
			case UIFinishPart:
				// Hold the finish event — we'll emit it after the title
				pendingFinish = &p
				return true
			case UIDonePart:
				sawDone = true
				return true // Hold — emit after finish
			default:
				return yield(part, nil)
			}
		})

		// Inject title if available
		title, threadID := titleFunc()
		if title != "" {
			yield(UITitlePart{Title: title, ThreadID: threadID}, nil)
		}

		// Emit the held finish event
		if pendingFinish != nil {
			yield(*pendingFinish, nil)
		}

		// Emit [DONE]
		if sawDone {
			yield(UIDonePart{}, nil)
		}
	}
}

// PipeEndureStream is a convenience function that writes a DataStream as a v6
// UI Message Stream with Endure's custom headers and optional title injection.
//
// This is the main integration point for endureapigo handlers:
//
//	func (h *Handler) Chat(w http.ResponseWriter, r *http.Request) {
//	    stream := provider.StreamChat(ctx, messages, tools)
//	    aisdk.PipeEndureStream(w, stream, aisdk.EndureStreamOptions{
//	        ThreadID:  threadID,
//	        StreamID:  streamID,
//	        MessageID: messageID,
//	    }, func() (string, string) {
//	        return generateTitle(messages), threadID
//	    })
//	}
func PipeEndureStream(w http.ResponseWriter, ds DataStream, opts EndureStreamOptions, titleFunc func() (title string, threadID string)) error {
	WriteEndureStreamHeaders(w, opts)

	uiStream := DataStreamToUIMessageStream(ds, opts.MessageID)

	if titleFunc != nil {
		uiStream = WithTitleInjection(uiStream, titleFunc)
	}

	return UIMessageStream(uiStream).Pipe(w)
}

// PipeEndureStreamRaw writes a UIMessageStream with Endure headers but also
// supports raw "t:" title events for backwards compatibility with existing
// iOS clients that parse the corapinew format.
func PipeEndureStreamRaw(w io.Writer, stream UIMessageStream, titleEvent *UITitlePart) error {
	flusher, ok := w.(http.Flusher)
	if !ok {
		flusher = nil
	}

	var pipeErr error
	stream(func(part UIMessageStreamPart, err error) bool {
		if err != nil {
			pipeErr = err
			return false
		}

		// For title events, use the raw "t:" format for iOS compatibility
		if tp, ok := part.(UITitlePart); ok {
			_, writeErr := fmt.Fprint(w, tp.FormatTitleEvent())
			if writeErr != nil {
				pipeErr = writeErr
				return false
			}
			if flusher != nil {
				flusher.Flush()
			}
			return true
		}

		data, marshalErr := part.UIStreamJSON()
		if marshalErr != nil {
			pipeErr = fmt.Errorf("failed to marshal %s event: %w", part.UIStreamType(), marshalErr)
			return false
		}

		_, writeErr := fmt.Fprintf(w, "data: %s\n\n", data)
		if writeErr != nil {
			pipeErr = writeErr
			return false
		}
		if flusher != nil {
			flusher.Flush()
		}
		return true
	})

	return pipeErr
}
