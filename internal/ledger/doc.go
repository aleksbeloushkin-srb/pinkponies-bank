// Package ledger is the domain model of the ledger service: accounts, holds
// and the double-entry journal of Pinkponies Bank.
//
// It is the innermost layer of the service. It imports only the standard
// library and google/uuid, and it is the one place that decides how money
// moves:
//
//   - every posting is a Transaction whose entries sum to zero, so money is
//     never created or destroyed, only moved between accounts;
//   - a customer's available balance is the posted balance minus active
//     holds, and it never goes below zero;
//   - a hold is active, captured or released, and only an active hold that
//     has not expired can be captured.
//
// The database enforces the same rules a second time (check constraints and
// a deferred trigger), so a bug in this package fails a transaction instead
// of corrupting balances.
package ledger
