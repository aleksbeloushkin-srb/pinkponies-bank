package httpapi

import (
	"context"
	"fmt"
	"io"
	"maps"
	"net/http"
	"slices"
	"strings"
	"sync"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/platform/httpserver"
)

// Sink is a fake webhook receiver for development and the end-to-end
// script: the far end of the webhook channel. It counts what it receives
// by Idempotency-Key, so the script can check "delivered once", and it can
// fail on request.
//
// It lives in the notifications process only to keep the local stack
// small; a real receiver is somebody else's service.
type Sink struct {
	ctx    context.Context
	cancel context.CancelFunc

	mu       sync.Mutex
	faults   []string
	calls    int
	accepted map[string]int
}

// Sink faults, consumed one per call.
const (
	SinkFault500  = "500"
	SinkFaultHang = "hang"
)

// NewSink returns a sink with an initial fault queue.
func NewSink(faults []string) (*Sink, error) {
	if err := checkFaults(faults); err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Sink{ctx: ctx, cancel: cancel, faults: slices.Clone(faults), accepted: map[string]int{}}, nil
}

// Close releases hanging requests; the HTTP server calls it when shutdown
// starts.
func (s *Sink) Close() { s.cancel() }

func checkFaults(faults []string) error {
	for _, f := range faults {
		if f != SinkFault500 && f != SinkFaultHang {
			return fmt.Errorf("unknown sink fault %q (want 500 or hang)", f)
		}
	}
	return nil
}

func (s *Sink) register(mux *http.ServeMux) {
	mux.HandleFunc("POST /_sink/webhooks", s.receive)
	mux.HandleFunc("GET /_sink/webhooks", s.show)
	mux.HandleFunc("POST /_sink/control", s.control)
	mux.HandleFunc("DELETE /_sink/control", s.reset)
}

func (s *Sink) receive(w http.ResponseWriter, r *http.Request) {
	_, _ = io.Copy(io.Discard, io.LimitReader(r.Body, 64<<10))
	s.mu.Lock()
	s.calls++
	var fault string
	if len(s.faults) > 0 {
		fault, s.faults = s.faults[0], s.faults[1:]
	}
	s.mu.Unlock()
	switch fault {
	case SinkFault500:
		httpserver.WriteError(w, http.StatusInternalServerError, "internal", "sink failure injected", "")
		return
	case SinkFaultHang:
		select {
		case <-r.Context().Done():
		case <-s.ctx.Done():
		}
		return
	}
	key := r.Header.Get("Idempotency-Key")
	s.mu.Lock()
	s.accepted[key]++
	s.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

// SinkStats is what the sink has seen.
type SinkStats struct {
	Calls int `json:"calls"`
	// Accepted counts 2xx answers, Unique the distinct keys among them;
	// Duplicates is their difference: deliveries the receiver had to
	// deduplicate.
	Accepted   int            `json:"accepted"`
	Unique     int            `json:"unique"`
	Duplicates int            `json:"duplicates"`
	Faults     []string       `json:"faults"`
	ByKey      map[string]int `json:"by_key"`
}

// Stats returns a snapshot.
func (s *Sink) Stats() SinkStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := SinkStats{Calls: s.calls, Unique: len(s.accepted), Faults: slices.Clone(s.faults), ByKey: maps.Clone(s.accepted)}
	for _, n := range s.accepted {
		st.Accepted += n
	}
	st.Duplicates = st.Accepted - st.Unique
	if st.Faults == nil {
		st.Faults = []string{}
	}
	return st
}

func (s *Sink) show(w http.ResponseWriter, _ *http.Request) {
	httpserver.WriteJSON(w, http.StatusOK, s.Stats())
}

// control appends faults: POST /_sink/control?faults=500,hang.
func (s *Sink) control(w http.ResponseWriter, r *http.Request) {
	var faults []string
	for _, f := range strings.Split(r.URL.Query().Get("faults"), ",") {
		if f = strings.TrimSpace(f); f != "" {
			faults = append(faults, f)
		}
	}
	if err := checkFaults(faults); err != nil {
		httpserver.WriteError(w, http.StatusBadRequest, "invalid_argument", err.Error(), "faults")
		return
	}
	s.mu.Lock()
	s.faults = append(s.faults, faults...)
	s.mu.Unlock()
	httpserver.WriteJSON(w, http.StatusOK, s.Stats())
}

func (s *Sink) reset(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	s.faults, s.calls, s.accepted = nil, 0, map[string]int{}
	s.mu.Unlock()
	httpserver.WriteJSON(w, http.StatusOK, s.Stats())
}
