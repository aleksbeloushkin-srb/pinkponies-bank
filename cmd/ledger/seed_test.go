package main

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/ledger/app"
)

type recordingOpener struct{ got []app.OpenAccountRequest }

func (r *recordingOpener) OpenAccount(_ context.Context, req app.OpenAccountRequest) (app.OpenAccountResult, error) {
	r.got = append(r.got, req)
	return app.OpenAccountResult{}, nil
}

func TestSeedOpensEveryAccount(t *testing.T) {
	path := filepath.Join(t.TempDir(), "accounts.json")
	data := `{"accounts":[
		{"key":"demo:a","name":"A","currency":"EUR","initial_deposit_minor":100},
		{"key":"demo:b","name":"B","currency":"EUR","initial_deposit_minor":0}]}`
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
	var r recordingOpener
	if err := seed(context.Background(), path, &r, slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
		t.Fatal(err)
	}
	if len(r.got) != 2 || r.got[0].IdempotencyKey != "demo:a" || r.got[0].InitialDepositMinor != 100 {
		t.Fatalf("opened %+v", r.got)
	}
}

func TestSeedRejectsUnknownFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "accounts.json")
	if err := os.WriteFile(path, []byte(`{"accounts":[{"key":"k","nmae":"typo"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := seed(context.Background(), path, &recordingOpener{}, slog.New(slog.NewTextHandler(io.Discard, nil))); err == nil {
		t.Fatal("a misspelt field must stop the seed")
	}
}

// The seed file used by docker compose must parse.
func TestComposeSeedFileParses(t *testing.T) {
	var r recordingOpener
	if err := seed(context.Background(), "../../deploy/ledger/accounts.json", &r, slog.New(slog.NewTextHandler(io.Discard, nil))); err != nil {
		t.Fatal(err)
	}
	if len(r.got) < 3 {
		t.Fatalf("seed accounts = %d, want at least 3", len(r.got))
	}
}
