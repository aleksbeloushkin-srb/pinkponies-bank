package ledger

import "regexp"

// Currency is an ISO 4217 alphabetic code, for example "EUR". Amounts are
// always integers in minor units (cents for EUR, yen for JPY), never floats.
type Currency string

// exponents lists the supported currencies with their number of decimal
// places.
var exponents = map[Currency]int{
	"EUR": 2,
	"USD": 2,
	"GBP": 2,
	"CHF": 2,
	"JPY": 0,
}

var currencyPattern = regexp.MustCompile(`^[A-Z]{3}$`)

// MaxAmountMinor caps a single amount: 10 million in major units of a
// two-decimal currency. Balances are sums of such amounts, so arithmetic
// stays far away from int64 overflow.
const MaxAmountMinor int64 = 1_000_000_000

// ParseCurrency validates a currency code.
func ParseCurrency(s string) (Currency, error) {
	if !currencyPattern.MatchString(s) {
		return "", &ValidationError{Field: "currency", Reason: "must be a 3-letter ISO 4217 code"}
	}
	c := Currency(s)
	if _, ok := exponents[c]; !ok {
		return "", &ValidationError{Field: "currency", Reason: "is not supported"}
	}
	return c, nil
}

// validateAmount checks an amount of a hold or a transfer.
func validateAmount(field string, amountMinor int64) error {
	switch {
	case amountMinor <= 0:
		return &ValidationError{Field: field, Reason: "must be positive"}
	case amountMinor > MaxAmountMinor:
		return &ValidationError{Field: field, Reason: "exceeds the maximum"}
	}
	return nil
}
