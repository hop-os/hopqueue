package api

import (
	"errors"
	"fmt"
	"net/http"
)

// sseWriter is the small wrapper over http.ResponseWriter every SSE
// handler in this package goes through. It owns the headers,
// flush-after-each-message, and the heartbeat shape.
type sseWriter struct {
	w http.ResponseWriter
	f http.Flusher
}

// newSSE upgrades w to an SSE response. Must be called before any other
// header writes — once we've sent the 200, additional Header().Set()
// calls have no effect.
func newSSE(w http.ResponseWriter) (*sseWriter, error) {
	f, ok := w.(http.Flusher)
	if !ok {
		return nil, errors.New("streaming unsupported")
	}
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	h.Set("Connection", "keep-alive")
	// Disables nginx response buffering when the queue is fronted by a
	// proxy. Without this the proxy holds events until its buffer fills,
	// which destroys the latency story.
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	f.Flush()
	return &sseWriter{w: w, f: f}, nil
}

// send writes one SSE event and flushes. id == 0 omits the id line
// (used for the welcome event whose order doesn't matter for resume).
// data == nil becomes "{}" so consumers always get parseable JSON.
func (s *sseWriter) send(id int64, event string, data []byte) error {
	if id > 0 {
		if _, err := fmt.Fprintf(s.w, "id: %d\n", id); err != nil {
			return err
		}
	}
	if event != "" {
		if _, err := fmt.Fprintf(s.w, "event: %s\n", event); err != nil {
			return err
		}
	}
	if len(data) == 0 {
		data = []byte("{}")
	}
	if _, err := fmt.Fprintf(s.w, "data: %s\n\n", data); err != nil {
		return err
	}
	s.f.Flush()
	return nil
}

// heartbeat writes an SSE comment line. Comments are ignored by clients
// but keep the TCP connection (and any intermediate proxy) alive.
func (s *sseWriter) heartbeat() error {
	if _, err := s.w.Write([]byte(": heartbeat\n\n")); err != nil {
		return err
	}
	s.f.Flush()
	return nil
}
