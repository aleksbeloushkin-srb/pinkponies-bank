package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/ledger/app"
)

// seedFile lists accounts to open at start. Each one is opened with its key,
// so starting twice opens nothing new; a key whose name, currency or
// deposit changed stops the start with an idempotency conflict.
type seedFile struct {
	Accounts []struct {
		Key                 string `json:"key"`
		Name                string `json:"name"`
		Currency            string `json:"currency"`
		InitialDepositMinor int64  `json:"initial_deposit_minor"`
	} `json:"accounts"`
}

type accountOpener interface {
	OpenAccount(ctx context.Context, req app.OpenAccountRequest) (app.OpenAccountResult, error)
}

func seed(ctx context.Context, path string, svc accountOpener, log *slog.Logger) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("seed: %w", err)
	}
	var f seedFile
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&f); err != nil {
		return fmt.Errorf("seed %s: %w", path, err)
	}
	for _, a := range f.Accounts {
		res, err := svc.OpenAccount(ctx, app.OpenAccountRequest{
			IdempotencyKey:      a.Key,
			Name:                a.Name,
			Currency:            a.Currency,
			InitialDepositMinor: a.InitialDepositMinor,
		})
		if err != nil {
			return fmt.Errorf("seed account %q: %w", a.Key, err)
		}
		log.InfoContext(ctx, "seed account", "account", res.Account, "replayed", res.Replayed)
	}
	return nil
}
