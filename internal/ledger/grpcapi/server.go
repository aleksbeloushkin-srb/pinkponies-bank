// Package grpcapi is the ledger's gRPC adapter: the LedgerService handlers,
// the single mapping from domain errors to status codes, and the server
// with its interceptors, health service and graceful stop.
package grpcapi

import (
	"context"
	"log/slog"

	"github.com/google/uuid"

	ledgerv1 "github.com/aleksbeloushkin-srb/pinkponies-bank/gen/ledger/v1"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/ledger"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/ledger/app"
)

// Service is what the handlers need from the use cases.
type Service interface {
	OpenAccount(ctx context.Context, req app.OpenAccountRequest) (app.OpenAccountResult, error)
	Account(ctx context.Context, id uuid.UUID) (ledger.Account, error)
	Accounts(ctx context.Context, size int, token string) (app.Page[ledger.Account], error)
	PlaceHold(ctx context.Context, req app.PlaceHoldRequest) (app.HoldResult, error)
	CaptureHold(ctx context.Context, req app.CaptureHoldRequest) (app.CaptureResult, error)
	ReleaseHold(ctx context.Context, req app.ReleaseHoldRequest) (app.HoldResult, error)
	Entries(ctx context.Context, accountID uuid.UUID, size int, token string) (app.Page[ledger.Entry], error)
}

// Handlers implements ledgerv1.LedgerServiceServer on top of Service.
type Handlers struct {
	ledgerv1.UnimplementedLedgerServiceServer
	svc Service
	log *slog.Logger
}

// NewHandlers returns the LedgerService handlers.
func NewHandlers(svc Service, log *slog.Logger) *Handlers {
	return &Handlers{svc: svc, log: log}
}

// OpenAccount implements ledgerv1.LedgerServiceServer.
func (h *Handlers) OpenAccount(ctx context.Context, req *ledgerv1.OpenAccountRequest) (*ledgerv1.OpenAccountResponse, error) {
	res, err := h.svc.OpenAccount(ctx, app.OpenAccountRequest{
		IdempotencyKey:      req.GetIdempotencyKey(),
		Name:                req.GetName(),
		Currency:            req.GetCurrency(),
		InitialDepositMinor: req.GetInitialDepositMinor(),
	})
	if err != nil {
		return nil, toStatus(ctx, h.log, err)
	}
	resp := &ledgerv1.OpenAccountResponse{Account: toAccount(res.Account)}
	if res.Replayed {
		return nil, replayed("account opened", resp)
	}
	return resp, nil
}

// GetAccount implements ledgerv1.LedgerServiceServer.
func (h *Handlers) GetAccount(ctx context.Context, req *ledgerv1.GetAccountRequest) (*ledgerv1.GetAccountResponse, error) {
	id, err := parseID("account_id", req.GetAccountId())
	if err != nil {
		return nil, toStatus(ctx, h.log, err)
	}
	a, err := h.svc.Account(ctx, id)
	if err != nil {
		return nil, toStatus(ctx, h.log, err)
	}
	return &ledgerv1.GetAccountResponse{Account: toAccount(a)}, nil
}

// ListAccounts implements ledgerv1.LedgerServiceServer.
func (h *Handlers) ListAccounts(ctx context.Context, req *ledgerv1.ListAccountsRequest) (*ledgerv1.ListAccountsResponse, error) {
	page, err := h.svc.Accounts(ctx, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, toStatus(ctx, h.log, err)
	}
	resp := &ledgerv1.ListAccountsResponse{NextPageToken: page.NextPageToken}
	for _, a := range page.Items {
		resp.Accounts = append(resp.Accounts, toAccount(a))
	}
	return resp, nil
}

// PlaceHold implements ledgerv1.LedgerServiceServer.
func (h *Handlers) PlaceHold(ctx context.Context, req *ledgerv1.PlaceHoldRequest) (*ledgerv1.PlaceHoldResponse, error) {
	src, err := parseID("account_id", req.GetAccountId())
	if err != nil {
		return nil, toStatus(ctx, h.log, err)
	}
	dst, err := parseID("destination_account_id", req.GetDestinationAccountId())
	if err != nil {
		return nil, toStatus(ctx, h.log, err)
	}
	ttl := req.GetTtl().AsDuration() // an unset TTL is zero: the default
	if req.GetTtl() != nil && (!req.GetTtl().IsValid() || ttl <= 0) {
		return nil, toStatus(ctx, h.log, &ledger.ValidationError{Field: "ttl", Reason: "must be positive"})
	}
	res, err := h.svc.PlaceHold(ctx, app.PlaceHoldRequest{
		IdempotencyKey: req.GetIdempotencyKey(),
		AccountID:      src,
		DestinationID:  dst,
		AmountMinor:    req.GetAmountMinor(),
		Currency:       req.GetCurrency(),
		TTL:            ttl,
	})
	if err != nil {
		return nil, toStatus(ctx, h.log, err)
	}
	resp := &ledgerv1.PlaceHoldResponse{Hold: toHold(res.Hold)}
	if res.Replayed {
		return nil, replayed("hold placed", resp)
	}
	return resp, nil
}

// CaptureHold implements ledgerv1.LedgerServiceServer.
func (h *Handlers) CaptureHold(ctx context.Context, req *ledgerv1.CaptureHoldRequest) (*ledgerv1.CaptureHoldResponse, error) {
	id, err := parseID("hold_id", req.GetHoldId())
	if err != nil {
		return nil, toStatus(ctx, h.log, err)
	}
	res, err := h.svc.CaptureHold(ctx, app.CaptureHoldRequest{IdempotencyKey: req.GetIdempotencyKey(), HoldID: id})
	if err != nil {
		return nil, toStatus(ctx, h.log, err)
	}
	resp := &ledgerv1.CaptureHoldResponse{Hold: toHold(res.Hold), Transaction: toTransaction(res.Transaction)}
	if res.Replayed {
		return nil, replayed("hold captured", resp)
	}
	return resp, nil
}

// ReleaseHold implements ledgerv1.LedgerServiceServer.
func (h *Handlers) ReleaseHold(ctx context.Context, req *ledgerv1.ReleaseHoldRequest) (*ledgerv1.ReleaseHoldResponse, error) {
	id, err := parseID("hold_id", req.GetHoldId())
	if err != nil {
		return nil, toStatus(ctx, h.log, err)
	}
	res, err := h.svc.ReleaseHold(ctx, app.ReleaseHoldRequest{
		IdempotencyKey: req.GetIdempotencyKey(), HoldID: id, Reason: req.GetReason(),
	})
	if err != nil {
		return nil, toStatus(ctx, h.log, err)
	}
	resp := &ledgerv1.ReleaseHoldResponse{Hold: toHold(res.Hold)}
	if res.Replayed {
		return nil, replayed("hold released", resp)
	}
	return resp, nil
}

// ListEntries implements ledgerv1.LedgerServiceServer.
func (h *Handlers) ListEntries(ctx context.Context, req *ledgerv1.ListEntriesRequest) (*ledgerv1.ListEntriesResponse, error) {
	id, err := parseID("account_id", req.GetAccountId())
	if err != nil {
		return nil, toStatus(ctx, h.log, err)
	}
	page, err := h.svc.Entries(ctx, id, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, toStatus(ctx, h.log, err)
	}
	resp := &ledgerv1.ListEntriesResponse{NextPageToken: page.NextPageToken}
	for _, e := range page.Items {
		resp.Entries = append(resp.Entries, toEntry(e))
	}
	return resp, nil
}
