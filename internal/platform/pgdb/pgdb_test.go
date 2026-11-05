package pgdb

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestIsTransient(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"serialization failure", &pgconn.PgError{Code: "40001"}, true},
		{"deadlock", &pgconn.PgError{Code: "40P01"}, true},
		{"lock timeout", &pgconn.PgError{Code: "55P03"}, true},
		{"statement timeout", &pgconn.PgError{Code: "57014"}, true},
		{"connection failure", &pgconn.PgError{Code: "08006"}, true},
		{"too many connections", &pgconn.PgError{Code: "53300"}, true},
		{"unique violation", &pgconn.PgError{Code: "23505"}, false},
		{"check violation", &pgconn.PgError{Code: "23514"}, false},
		{"wrapped deadline", fmt.Errorf("query: %w", context.DeadlineExceeded), true},
		{"plain error", errors.New("boom"), false},
	}
	for _, tt := range tests {
		if got := IsTransient(tt.err); got != tt.want {
			t.Errorf("%s: IsTransient = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestIsUniqueViolation(t *testing.T) {
	err := fmt.Errorf("insert: %w", &pgconn.PgError{Code: "23505", ConstraintName: "holds_idempotency_key_key"})
	if !IsUniqueViolation(err, "") || !IsUniqueViolation(err, "holds_idempotency_key_key") {
		t.Fatal("want a unique violation")
	}
	if IsUniqueViolation(err, "other") || IsUniqueViolation(errors.New("x"), "") {
		t.Fatal("wrong constraint or error must not match")
	}
}
