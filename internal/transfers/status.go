package transfers

// Status is the lifecycle state of a transfer.
//
//	pending ──► held ──► completed
//	   │          ├────► rejected   (screening declined; hold released)
//	   │          └────► canceled   (screening unavailable, review timed
//	   │                             out, or the hold expired; released)
//	   └──────────────► rejected    (the hold was refused: insufficient
//	                                 funds, unknown account, currency)
//
// completed, rejected and canceled are final.
type Status string

// Transfer statuses.
const (
	StatusPending   Status = "pending"
	StatusHeld      Status = "held"
	StatusCompleted Status = "completed"
	StatusRejected  Status = "rejected"
	StatusCanceled  Status = "canceled"
)

var transitions = map[Status][]Status{
	StatusPending: {StatusHeld, StatusRejected},
	StatusHeld:    {StatusCompleted, StatusRejected, StatusCanceled},
}

// Valid reports whether s is a known status.
func (s Status) Valid() bool {
	switch s {
	case StatusPending, StatusHeld, StatusCompleted, StatusRejected, StatusCanceled:
		return true
	}
	return false
}

// Final reports whether s is a terminal status.
func (s Status) Final() bool {
	return s == StatusCompleted || s == StatusRejected || s == StatusCanceled
}

// CanTransitionTo reports whether the state machine allows s -> next.
func (s Status) CanTransitionTo(next Status) bool {
	for _, n := range transitions[s] {
		if n == next {
			return true
		}
	}
	return false
}

// Reason says why a transfer was rejected or canceled. It is part of the
// API and of the events, so the values are stable.
type Reason string

// Reasons.
const (
	ReasonInsufficientFunds    Reason = "insufficient_funds"
	ReasonAccountNotFound      Reason = "account_not_found"
	ReasonCurrencyMismatch     Reason = "currency_mismatch"
	ReasonScreeningDeclined    Reason = "screening_declined"
	ReasonScreeningUnavailable Reason = "screening_unavailable"
	ReasonScreeningTimeout     Reason = "screening_review_timeout"
	ReasonHoldExpired          Reason = "hold_expired"
)
