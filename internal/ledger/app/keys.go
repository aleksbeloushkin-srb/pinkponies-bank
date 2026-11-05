package app

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"github.com/google/uuid"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/ledger"
)

// MaxIdempotencyKeyLen bounds an idempotency key.
const MaxIdempotencyKeyLen = 255

func validateKey(key string) error {
	if key == "" || len(key) > MaxIdempotencyKeyLen {
		return &ledger.ValidationError{
			Field:  "idempotency_key",
			Reason: fmt.Sprintf("is required and must be at most %d characters", MaxIdempotencyKeyLen),
		}
	}
	for _, r := range key {
		if r > unicode.MaxASCII || !unicode.IsPrint(r) {
			return &ledger.ValidationError{Field: "idempotency_key", Reason: "must be printable ASCII"}
		}
	}
	return nil
}

// requestHash fingerprints the business content of a request. JSON encoding
// of a fixed struct is an unambiguous canonical form.
func requestHash(v any) string {
	canonical, err := json.Marshal(v)
	if err != nil {
		// Only fixed structs of strings and integers are hashed.
		panic("ledger/app: requestHash: " + err.Error())
	}
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:])
}

// Page sizes for the list use cases.
const (
	DefaultPageSize = 50
	MaxPageSize     = 500
)

func pageSize(n int) (int, error) {
	switch {
	case n == 0:
		return DefaultPageSize, nil
	case n < 0 || n > MaxPageSize:
		return 0, &ledger.ValidationError{Field: "page_size", Reason: fmt.Sprintf("must be between 0 and %d", MaxPageSize)}
	}
	return n, nil
}

// Page tokens are opaque to clients: base64 of a prefix and the last key
// returned, so a token from one list cannot be fed to another.
const (
	accountsTokenPrefix = "a:"
	entriesTokenPrefix  = "e:"
)

func encodeToken(prefix, last string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(prefix + last))
}

func decodeToken(prefix, token string) (string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || !strings.HasPrefix(string(raw), prefix) {
		return "", &ledger.ValidationError{Field: "page_token", Reason: "is not a token of this list"}
	}
	return strings.TrimPrefix(string(raw), prefix), nil
}

func accountsAfter(token string) (uuid.UUID, error) {
	if token == "" {
		return uuid.Nil, nil
	}
	last, err := decodeToken(accountsTokenPrefix, token)
	if err != nil {
		return uuid.Nil, err
	}
	id, err := uuid.Parse(last)
	if err != nil {
		return uuid.Nil, &ledger.ValidationError{Field: "page_token", Reason: "is malformed"}
	}
	return id, nil
}

func entriesAfter(token string) (int64, error) {
	if token == "" {
		return 0, nil
	}
	last, err := decodeToken(entriesTokenPrefix, token)
	if err != nil {
		return 0, err
	}
	id, err := strconv.ParseInt(last, 10, 64)
	if err != nil || id < 0 {
		return 0, &ledger.ValidationError{Field: "page_token", Reason: "is malformed"}
	}
	return id, nil
}
