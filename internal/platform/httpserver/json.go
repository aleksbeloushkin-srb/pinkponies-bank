package httpserver

import (
	"encoding/json"
	"errors"
	"net/http"
)

// StatusClientClosedRequest is logged when the client disconnects before
// the answer (an nginx convention). It only reaches the access log and the
// metrics; nobody reads the response.
const StatusClientClosedRequest = 499

// ErrorBody is the JSON body of every error response.
type ErrorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Field   string `json:"field,omitempty"`
}

// WriteJSON writes v with the given status.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// WriteError writes an ErrorBody.
func WriteError(w http.ResponseWriter, status int, code, message, field string) {
	WriteJSON(w, status, ErrorBody{Code: code, Message: message, Field: field})
}

// BodyError means a request body is not the JSON object the handler
// expects. Services answer it with 400.
type BodyError struct {
	Reason string
}

func (e *BodyError) Error() string { return "invalid body: " + e.Reason }

// DecodeJSON reads a size-limited JSON body into dst and rejects unknown
// fields, so a misspelt field fails loudly instead of being ignored. It
// returns *BodyError for malformed input and *http.MaxBytesError for a body
// over maxBytes.
func DecodeJSON(w http.ResponseWriter, r *http.Request, maxBytes int64, dst any) error {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			return err
		}
		return &BodyError{Reason: "malformed JSON: " + err.Error()}
	}
	if dec.More() {
		return &BodyError{Reason: "must contain a single JSON object"}
	}
	return nil
}
