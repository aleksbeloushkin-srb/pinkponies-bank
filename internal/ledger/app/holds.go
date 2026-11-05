package app

import (
	"context"
	"errors"
	"fmt"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/ledger"
)

// PlaceHoldRequest is the input of PlaceHold.
type PlaceHoldRequest struct {
	IdempotencyKey string
	AccountID      uuid.UUID
	DestinationID  uuid.UUID
	AmountMinor    int64
	Currency       string
	// TTL is how long the hold may stay active; zero means the default.
	TTL time.Duration
}

// HoldResult is a hold and whether the call was a replay of an earlier one.
type HoldResult struct {
	Hold     ledger.Hold
	Replayed bool
}

// PlaceHold reserves an amount on the source account for a transfer to the
// destination. It fails with ledger.ErrInsufficientFunds when the source's
// available balance is too low, so available never goes below zero.
//
// The source row is locked before the key is looked up: two concurrent
// calls with the same key on the same account are serialized, and the
// second one finds the first one's hold and returns it as a replay. The TTL
// is not part of the request hash, so a retry that asks for another TTL
// still replays and keeps the original expiry.
func (s *Service) PlaceHold(ctx context.Context, req PlaceHoldRequest) (HoldResult, error) {
	if err := validateKey(req.IdempotencyKey); err != nil {
		return HoldResult{}, err
	}
	currency, err := ledger.ParseCurrency(req.Currency)
	if err != nil {
		return HoldResult{}, err
	}
	ttl := req.TTL
	if ttl == 0 {
		ttl = s.cfg.DefaultHoldTTL
	}
	if ttl < 0 || ttl > s.cfg.MaxHoldTTL {
		return HoldResult{}, &ledger.ValidationError{Field: "ttl", Reason: fmt.Sprintf("must be between 0 and %s", s.cfg.MaxHoldTTL)}
	}
	id, err := s.newID()
	if err != nil {
		return HoldResult{}, fmt.Errorf("generate hold id: %w", err)
	}
	now := s.clock.Now()
	hash := requestHash(struct {
		Account     string `json:"account_id"`
		Destination string `json:"destination_account_id"`
		Amount      int64  `json:"amount_minor"`
		Currency    string `json:"currency"`
	}{req.AccountID.String(), req.DestinationID.String(), req.AmountMinor, string(currency)})
	hold, err := ledger.NewHold(id, req.AccountID, req.DestinationID, req.AmountMinor, currency,
		req.IdempotencyKey, hash, now, now.Add(ttl))
	if err != nil {
		return HoldResult{}, err
	}

	var res HoldResult
	err = s.tx.Run(ctx, func(ctx context.Context) error {
		locked, err := s.accounts.LockAccounts(ctx, req.AccountID)
		if err != nil {
			return fmt.Errorf("source account: %w", err)
		}
		if existing, ok, err := s.holds.HoldByKey(ctx, req.IdempotencyKey); err != nil {
			return fmt.Errorf("find hold: %w", err)
		} else if ok {
			return replayHold(existing, hash, &res)
		}
		src := locked[req.AccountID]
		dst, err := s.accounts.Account(ctx, req.DestinationID)
		if err != nil {
			return fmt.Errorf("destination account: %w", err)
		}
		if err := checkTransferAccounts(src, dst, currency); err != nil {
			return err
		}
		if err := src.Reserve(hold.AmountMinor, now); err != nil {
			return err
		}
		inserted, err := s.holds.InsertHold(ctx, hold)
		if err != nil {
			return fmt.Errorf("store hold: %w", err)
		}
		if !inserted {
			// The same key on another source account, committed by a
			// concurrent call. Our reservation is not saved.
			existing, _, err := s.holds.HoldByKey(ctx, req.IdempotencyKey)
			if err != nil {
				return fmt.Errorf("find hold: %w", err)
			}
			return replayHold(existing, hash, &res)
		}
		if err := s.accounts.SaveBalances(ctx, src); err != nil {
			return fmt.Errorf("save balances: %w", err)
		}
		res = HoldResult{Hold: hold}
		return nil
	})
	if err != nil {
		return HoldResult{}, err
	}
	if !res.Replayed {
		s.metrics.HoldPlaced()
		s.log.InfoContext(ctx, "hold placed", "hold", res.Hold)
	}
	return res, nil
}

func replayHold(existing ledger.Hold, hash string, res *HoldResult) error {
	if existing.RequestHash != hash {
		return ledger.ErrIdempotencyConflict
	}
	*res = HoldResult{Hold: existing, Replayed: true}
	return nil
}

// checkTransferAccounts checks that money may move from src to dst in
// currency.
func checkTransferAccounts(src, dst ledger.Account, currency ledger.Currency) error {
	if src.Kind != ledger.KindCustomer || dst.Kind != ledger.KindCustomer {
		return &ledger.ValidationError{Field: "account_id", Reason: "holds move money between customer accounts only"}
	}
	if src.Currency != currency || dst.Currency != currency {
		return fmt.Errorf("%w: source %s, destination %s, request %s", ledger.ErrCurrencyMismatch, src.Currency, dst.Currency, currency)
	}
	return nil
}

// CaptureHoldRequest is the input of CaptureHold.
type CaptureHoldRequest struct {
	IdempotencyKey string
	HoldID         uuid.UUID
}

// CaptureResult is the captured hold and the transaction that moved the
// money.
type CaptureResult struct {
	Hold        ledger.Hold
	Transaction ledger.Transaction
	Replayed    bool
}

// CaptureHold posts the held amount from the hold's source to its
// destination as one balanced transaction, and marks the hold captured.
//
// The hold row is locked first and the key looked up after: a concurrent
// replay waits for the first capture to commit and then finds its
// transaction, instead of seeing a captured hold and failing.
func (s *Service) CaptureHold(ctx context.Context, req CaptureHoldRequest) (CaptureResult, error) {
	if err := validateKey(req.IdempotencyKey); err != nil {
		return CaptureResult{}, err
	}
	txID, err := s.newID()
	if err != nil {
		return CaptureResult{}, fmt.Errorf("generate transaction id: %w", err)
	}
	now := s.clock.Now()

	var res CaptureResult
	err = s.tx.Run(ctx, func(ctx context.Context) error {
		hold, err := s.holds.LockHold(ctx, req.HoldID)
		if err != nil {
			return err
		}
		if txn, ok, err := s.journal.TransactionByKey(ctx, req.IdempotencyKey); err != nil {
			return fmt.Errorf("find transaction: %w", err)
		} else if ok {
			if txn.HoldID != hold.ID {
				return ledger.ErrIdempotencyConflict
			}
			res = CaptureResult{Hold: hold, Transaction: txn, Replayed: true}
			return nil
		}
		if err := hold.CheckCapturable(now); err != nil {
			return err
		}
		locked, err := s.accounts.LockAccounts(ctx, hold.AccountID, hold.DestinationID)
		if err != nil {
			return fmt.Errorf("lock accounts: %w", err)
		}
		src, dst := locked[hold.AccountID], locked[hold.DestinationID]
		if err := src.Unreserve(hold.AmountMinor, now); err != nil {
			return err
		}
		txn, err := ledger.Post(ledger.KindTransfer, txID, req.IdempotencyKey, hold.ID, hold.Currency, now,
			ledger.Leg{Account: &src, AmountMinor: -hold.AmountMinor},
			ledger.Leg{Account: &dst, AmountMinor: hold.AmountMinor})
		if err != nil {
			return err
		}
		hold.MarkCaptured(txn.ID, now)
		if txn, err = s.journal.InsertTransaction(ctx, txn); err != nil {
			return fmt.Errorf("store transaction: %w", err)
		}
		if err := s.holds.SaveHold(ctx, hold); err != nil {
			return fmt.Errorf("save hold: %w", err)
		}
		if err := s.accounts.SaveBalances(ctx, src, dst); err != nil {
			return fmt.Errorf("save balances: %w", err)
		}
		res = CaptureResult{Hold: hold, Transaction: txn}
		return nil
	})
	if err != nil {
		return CaptureResult{}, err
	}
	if !res.Replayed {
		s.metrics.HoldCaptured()
		s.log.InfoContext(ctx, "hold captured", "hold", res.Hold, "transaction_id", res.Transaction.ID)
	}
	return res, nil
}

// ReleaseHoldRequest is the input of ReleaseHold.
type ReleaseHoldRequest struct {
	IdempotencyKey string
	HoldID         uuid.UUID
	Reason         string
}

// MaxReasonLen bounds a release reason, in characters.
const MaxReasonLen = 200

// ReleaseHold gives a hold's amount back to the source account. Releasing a
// released hold, whoever released it (a client or the expiry sweep),
// returns it with Replayed set: compensation must be safe to repeat.
// Releasing a captured hold is ledger.ErrHoldCaptured.
func (s *Service) ReleaseHold(ctx context.Context, req ReleaseHoldRequest) (HoldResult, error) {
	if err := validateKey(req.IdempotencyKey); err != nil {
		return HoldResult{}, err
	}
	if n := utf8.RuneCountInString(req.Reason); n > MaxReasonLen || !printable(req.Reason) {
		return HoldResult{}, &ledger.ValidationError{Field: "reason", Reason: fmt.Sprintf("must be printable and at most %d characters", MaxReasonLen)}
	}
	reason := req.Reason
	if reason == "" {
		reason = "released on request"
	}
	res, err := s.release(ctx, req.HoldID, false, func(h *ledger.Hold, now time.Time) (bool, error) {
		return h.Release(reason, req.IdempotencyKey, now)
	})
	if err != nil {
		return HoldResult{}, err
	}
	if !res.Replayed {
		s.metrics.HoldReleased(CauseRequested)
		s.log.InfoContext(ctx, "hold released", "hold", res.Hold, "reason", reason)
	}
	return res, nil
}

func printable(s string) bool {
	for _, r := range s {
		if !unicode.IsPrint(r) {
			return false
		}
	}
	return true
}

// errHoldBusy means another transaction holds the row lock; the sweep
// skips such holds and comes back later.
var errHoldBusy = errors.New("hold is locked by another transaction")

// release runs one release in its own transaction. With skipLocked the
// hold's row is not waited for; a locked hold returns errHoldBusy.
func (s *Service) release(ctx context.Context, id uuid.UUID, skipLocked bool,
	change func(h *ledger.Hold, now time.Time) (bool, error)) (HoldResult, error) {
	var res HoldResult
	err := s.tx.Run(ctx, func(ctx context.Context) error {
		now := s.clock.Now()
		var (
			hold ledger.Hold
			err  error
		)
		if skipLocked {
			var ok bool
			if hold, ok, err = s.holds.TryLockHold(ctx, id); err == nil && !ok {
				return errHoldBusy
			}
		} else {
			hold, err = s.holds.LockHold(ctx, id)
		}
		if err != nil {
			return err
		}
		changed, err := change(&hold, now)
		if err != nil {
			return err
		}
		if !changed {
			res = HoldResult{Hold: hold, Replayed: true}
			return nil
		}
		locked, err := s.accounts.LockAccounts(ctx, hold.AccountID)
		if err != nil {
			return fmt.Errorf("lock account: %w", err)
		}
		src := locked[hold.AccountID]
		if err := src.Unreserve(hold.AmountMinor, now); err != nil {
			return err
		}
		if err := s.holds.SaveHold(ctx, hold); err != nil {
			return fmt.Errorf("save hold: %w", err)
		}
		if err := s.accounts.SaveBalances(ctx, src); err != nil {
			return fmt.Errorf("save balances: %w", err)
		}
		res = HoldResult{Hold: hold}
		return nil
	})
	return res, err
}
