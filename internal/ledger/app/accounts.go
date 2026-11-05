package app

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/ledger"
)

// OpenAccountRequest is the input of OpenAccount.
type OpenAccountRequest struct {
	IdempotencyKey string
	Name           string
	Currency       string
	// InitialDepositMinor, if positive, is moved from the treasury of the
	// currency to the new account in the same transaction.
	InitialDepositMinor int64
}

// OpenAccountResult is the stored account and whether it was there before.
type OpenAccountResult struct {
	Account  ledger.Account
	Replayed bool
}

// OpenAccount creates a customer account, funded from the treasury if an
// initial deposit is given. A replay with the same key and request returns
// the stored account; the same key with another request is
// ledger.ErrIdempotencyConflict.
//
// The deposit locks the treasury row of the currency: every funded open
// account in one currency is serialized on it. That is acceptable for
// account opening, and it would not be for a hot path.
func (s *Service) OpenAccount(ctx context.Context, req OpenAccountRequest) (OpenAccountResult, error) {
	if err := validateKey(req.IdempotencyKey); err != nil {
		return OpenAccountResult{}, err
	}
	currency, err := ledger.ParseCurrency(req.Currency)
	if err != nil {
		return OpenAccountResult{}, err
	}
	if req.InitialDepositMinor < 0 || req.InitialDepositMinor > ledger.MaxAmountMinor {
		return OpenAccountResult{}, &ledger.ValidationError{Field: "initial_deposit_minor", Reason: "must be between 0 and the maximum amount"}
	}
	id, err := s.newID()
	if err != nil {
		return OpenAccountResult{}, fmt.Errorf("generate account id: %w", err)
	}
	now := s.clock.Now()
	hash := requestHash(struct {
		Name     string `json:"name"`
		Currency string `json:"currency"`
		Deposit  int64  `json:"initial_deposit_minor"`
	}{req.Name, string(currency), req.InitialDepositMinor})
	acct, err := ledger.NewCustomerAccount(id, req.Name, currency, req.IdempotencyKey, hash, now)
	if err != nil {
		return OpenAccountResult{}, err
	}

	var res OpenAccountResult
	err = s.tx.Run(ctx, func(ctx context.Context) error {
		stored, inserted, err := s.accounts.InsertAccount(ctx, acct)
		if err != nil {
			return fmt.Errorf("store account: %w", err)
		}
		if !inserted {
			if stored.RequestHash != hash {
				return ledger.ErrIdempotencyConflict
			}
			res = OpenAccountResult{Account: stored, Replayed: true}
			return nil
		}
		if req.InitialDepositMinor > 0 {
			if err := s.deposit(ctx, &stored, req.InitialDepositMinor, now); err != nil {
				return err
			}
		}
		res = OpenAccountResult{Account: stored}
		return nil
	})
	if err != nil {
		return OpenAccountResult{}, err
	}
	if !res.Replayed {
		s.log.InfoContext(ctx, "account opened", "account", res.Account, "deposit_minor", req.InitialDepositMinor)
	}
	return res, nil
}

// deposit moves amount from the treasury to acct, a row inserted (and so
// locked) by the caller's transaction.
func (s *Service) deposit(ctx context.Context, acct *ledger.Account, amountMinor int64, now time.Time) error {
	treasuryID, err := s.newID()
	if err != nil {
		return fmt.Errorf("generate treasury id: %w", err)
	}
	treasury, err := s.accounts.LockTreasury(ctx, ledger.NewTreasury(treasuryID, acct.Currency, now))
	if err != nil {
		return fmt.Errorf("lock treasury: %w", err)
	}
	txID, err := s.newID()
	if err != nil {
		return fmt.Errorf("generate transaction id: %w", err)
	}
	txn, err := ledger.Post(ledger.KindDeposit, txID, "open:"+acct.IdempotencyKey, uuid.Nil, acct.Currency, now,
		ledger.Leg{Account: &treasury, AmountMinor: -amountMinor},
		ledger.Leg{Account: acct, AmountMinor: amountMinor})
	if err != nil {
		return err
	}
	if _, err := s.journal.InsertTransaction(ctx, txn); err != nil {
		return fmt.Errorf("store deposit: %w", err)
	}
	if err := s.accounts.SaveBalances(ctx, treasury, *acct); err != nil {
		return fmt.Errorf("save balances: %w", err)
	}
	return nil
}

// Account returns one account.
func (s *Service) Account(ctx context.Context, id uuid.UUID) (ledger.Account, error) {
	return s.accounts.Account(ctx, id)
}

// Page is one page of a list and the token of the next one, empty at the
// end.
type Page[T any] struct {
	Items         []T
	NextPageToken string
}

// Accounts lists accounts in id order, which for UUIDv7 ids is the order of
// creation.
func (s *Service) Accounts(ctx context.Context, size int, token string) (Page[ledger.Account], error) {
	limit, err := pageSize(size)
	if err != nil {
		return Page[ledger.Account]{}, err
	}
	after, err := accountsAfter(token)
	if err != nil {
		return Page[ledger.Account]{}, err
	}
	// One row more than asked tells whether another page exists.
	items, err := s.accounts.Accounts(ctx, after, limit+1)
	if err != nil {
		return Page[ledger.Account]{}, err
	}
	page := Page[ledger.Account]{Items: items}
	if len(items) > limit {
		page.Items = items[:limit]
		page.NextPageToken = encodeToken(accountsTokenPrefix, page.Items[limit-1].ID.String())
	}
	return page, nil
}

// Entries lists an account's journal entries, oldest first.
func (s *Service) Entries(ctx context.Context, accountID uuid.UUID, size int, token string) (Page[ledger.Entry], error) {
	limit, err := pageSize(size)
	if err != nil {
		return Page[ledger.Entry]{}, err
	}
	after, err := entriesAfter(token)
	if err != nil {
		return Page[ledger.Entry]{}, err
	}
	if _, err := s.accounts.Account(ctx, accountID); err != nil {
		return Page[ledger.Entry]{}, err
	}
	items, err := s.journal.Entries(ctx, accountID, after, limit+1)
	if err != nil {
		return Page[ledger.Entry]{}, err
	}
	page := Page[ledger.Entry]{Items: items}
	if len(items) > limit {
		page.Items = items[:limit]
		page.NextPageToken = encodeToken(entriesTokenPrefix, strconv.FormatInt(page.Items[limit-1].ID, 10))
	}
	return page, nil
}
