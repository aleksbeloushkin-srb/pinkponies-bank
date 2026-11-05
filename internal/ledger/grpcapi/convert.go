package grpcapi

import (
	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"

	ledgerv1 "github.com/aleksbeloushkin-srb/pinkponies-bank/gen/ledger/v1"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/ledger"
)

func parseID(field, s string) (uuid.UUID, error) {
	id, err := uuid.Parse(s)
	if err != nil {
		return uuid.Nil, &ledger.ValidationError{Field: field, Reason: "must be a UUID"}
	}
	return id, nil
}

var accountKinds = map[ledger.AccountKind]ledgerv1.AccountKind{
	ledger.KindCustomer: ledgerv1.AccountKind_ACCOUNT_KIND_CUSTOMER,
	ledger.KindSystem:   ledgerv1.AccountKind_ACCOUNT_KIND_SYSTEM,
}

var holdStatuses = map[ledger.HoldStatus]ledgerv1.HoldStatus{
	ledger.HoldActive:   ledgerv1.HoldStatus_HOLD_STATUS_ACTIVE,
	ledger.HoldCaptured: ledgerv1.HoldStatus_HOLD_STATUS_CAPTURED,
	ledger.HoldReleased: ledgerv1.HoldStatus_HOLD_STATUS_RELEASED,
}

var transactionKinds = map[ledger.TransactionKind]ledgerv1.TransactionKind{
	ledger.KindDeposit:  ledgerv1.TransactionKind_TRANSACTION_KIND_DEPOSIT,
	ledger.KindTransfer: ledgerv1.TransactionKind_TRANSACTION_KIND_TRANSFER,
}

func toAccount(a ledger.Account) *ledgerv1.Account {
	return &ledgerv1.Account{
		Id:             a.ID.String(),
		Name:           a.Name,
		Currency:       string(a.Currency),
		Kind:           accountKinds[a.Kind],
		PostedMinor:    a.PostedMinor,
		HeldMinor:      a.HeldMinor,
		AvailableMinor: a.AvailableMinor(),
		CreatedAt:      timestamppb.New(a.CreatedAt),
		UpdatedAt:      timestamppb.New(a.UpdatedAt),
	}
}

func toHold(h ledger.Hold) *ledgerv1.Hold {
	out := &ledgerv1.Hold{
		Id:                   h.ID.String(),
		AccountId:            h.AccountID.String(),
		DestinationAccountId: h.DestinationID.String(),
		AmountMinor:          h.AmountMinor,
		Currency:             string(h.Currency),
		Status:               holdStatuses[h.Status],
		ReleaseReason:        h.ReleaseReason,
		ExpiresAt:            timestamppb.New(h.ExpiresAt),
		CreatedAt:            timestamppb.New(h.CreatedAt),
		UpdatedAt:            timestamppb.New(h.UpdatedAt),
	}
	if h.TransactionID != uuid.Nil {
		out.TransactionId = h.TransactionID.String()
	}
	return out
}

func toEntry(e ledger.Entry) *ledgerv1.Entry {
	return &ledgerv1.Entry{
		Id:                e.ID,
		TransactionId:     e.TransactionID.String(),
		AccountId:         e.AccountID.String(),
		AmountMinor:       e.AmountMinor,
		BalanceAfterMinor: e.BalanceAfterMinor,
		CreatedAt:         timestamppb.New(e.CreatedAt),
	}
}

func toTransaction(t ledger.Transaction) *ledgerv1.Transaction {
	out := &ledgerv1.Transaction{
		Id:        t.ID.String(),
		Kind:      transactionKinds[t.Kind],
		Currency:  string(t.Currency),
		CreatedAt: timestamppb.New(t.CreatedAt),
	}
	if t.HoldID != uuid.Nil {
		out.HoldId = t.HoldID.String()
	}
	for _, e := range t.Entries {
		out.Entries = append(out.Entries, toEntry(e))
	}
	return out
}
