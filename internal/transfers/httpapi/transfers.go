package httpapi

import (
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/platform/httpserver"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers/app"
)

const maxCreateBody = 16 << 10

type createTransferJSON struct {
	SourceAccountID      uuid.UUID `json:"source_account_id"`
	DestinationAccountID uuid.UUID `json:"destination_account_id"`
	AmountMinor          int64     `json:"amount_minor"`
	Currency             string    `json:"currency"`
}

type stepJSON struct {
	Step      string    `json:"step"`
	State     string    `json:"state"`
	Outcome   string    `json:"outcome,omitempty"`
	Attempts  int       `json:"attempts"`
	StartedAt time.Time `json:"started_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type transferJSON struct {
	ID                   uuid.UUID  `json:"id"`
	SourceAccountID      uuid.UUID  `json:"source_account_id"`
	DestinationAccountID uuid.UUID  `json:"destination_account_id"`
	AmountMinor          int64      `json:"amount_minor"`
	Currency             string     `json:"currency"`
	Status               string     `json:"status"`
	Reason               string     `json:"reason,omitempty"`
	Version              int64      `json:"version"`
	HoldID               *uuid.UUID `json:"hold_id,omitempty"`
	CreatedAt            time.Time  `json:"created_at"`
	UpdatedAt            time.Time  `json:"updated_at"`
	Steps                []stepJSON `json:"steps"`
}

// waitJSON is the long-poll answer: the transfer as of the answer, and
// whether its version moved past the client's marker.
type waitJSON struct {
	transferJSON
	Modified bool `json:"modified"`
}

func toJSON(t transfers.Transfer) transferJSON {
	out := transferJSON{
		ID:                   t.ID,
		SourceAccountID:      t.SourceAccountID,
		DestinationAccountID: t.DestinationAccountID,
		AmountMinor:          t.AmountMinor,
		Currency:             t.Currency,
		Status:               string(t.Status),
		Reason:               string(t.Reason),
		Version:              t.Version,
		CreatedAt:            t.CreatedAt.UTC(),
		UpdatedAt:            t.UpdatedAt.UTC(),
		Steps:                []stepJSON{},
	}
	if t.HoldID != uuid.Nil {
		hold := t.HoldID
		out.HoldID = &hold
	}
	for _, s := range t.Steps {
		out.Steps = append(out.Steps, stepJSON{
			Step: string(s.Step), State: string(s.State), Outcome: s.Outcome, Attempts: s.Attempts,
			StartedAt: s.StartedAt.UTC(), UpdatedAt: s.UpdatedAt.UTC(),
		})
	}
	return out
}

// createTransfer handles POST /v1/transfers: 201 for a new transfer, 200
// for a replay with the same Idempotency-Key and body, 422 for the same key
// with another body. The saga runs in the background; the answer is the
// pending transfer.
func (h *handler) createTransfer(w http.ResponseWriter, r *http.Request) {
	var in createTransferJSON
	if err := httpserver.DecodeJSON(w, r, maxCreateBody, &in); err != nil {
		writeError(w, r, h.log, err)
		return
	}
	res, err := h.creator.CreateTransfer(r.Context(), app.CreateTransferRequest{
		IdempotencyKey:       r.Header.Get("Idempotency-Key"),
		SourceAccountID:      in.SourceAccountID,
		DestinationAccountID: in.DestinationAccountID,
		AmountMinor:          in.AmountMinor,
		Currency:             in.Currency,
	})
	if err != nil {
		writeError(w, r, h.log, err)
		return
	}
	status := http.StatusOK
	if res.Created {
		status = http.StatusCreated
		w.Header().Set("Location", "/v1/transfers/"+res.Transfer.ID.String())
	}
	httpserver.WriteJSON(w, status, toJSON(res.Transfer))
}

// transfer handles GET /v1/transfers/{id}.
func (h *handler) transfer(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		writeError(w, r, h.log, err)
		return
	}
	t, err := h.reader.Transfer(r.Context(), id)
	if err != nil {
		writeError(w, r, h.log, err)
		return
	}
	httpserver.WriteJSON(w, http.StatusOK, toJSON(t))
}

// waitTransfer handles GET /v1/transfers/{id}/wait?version=N, the long
// poll. It answers when the version exceeds N or after the soft timeout,
// with "modified" telling the two apart. The client sends the version it
// last saw and repeats.
func (h *handler) waitTransfer(w http.ResponseWriter, r *http.Request) {
	id, err := pathID(r)
	if err != nil {
		writeError(w, r, h.log, err)
		return
	}
	version, err := strconv.ParseInt(r.URL.Query().Get("version"), 10, 64)
	if err != nil {
		writeError(w, r, h.log, &transfers.ValidationError{Field: "version", Reason: "query parameter must be an integer"})
		return
	}
	res, err := h.reader.WaitTransfer(r.Context(), id, version)
	if err != nil {
		writeError(w, r, h.log, err)
		return
	}
	httpserver.WriteJSON(w, http.StatusOK, waitJSON{transferJSON: toJSON(res.Transfer), Modified: res.Modified})
}

type accountJSON struct {
	ID             uuid.UUID `json:"id"`
	Name           string    `json:"name"`
	Currency       string    `json:"currency"`
	PostedMinor    int64     `json:"posted_minor"`
	HeldMinor      int64     `json:"held_minor"`
	AvailableMinor int64     `json:"available_minor"`
}

// listAccounts handles GET /v1/accounts, read from the ledger over gRPC.
func (h *handler) listAccounts(w http.ResponseWriter, r *http.Request) {
	accounts, err := h.accounts.Accounts(r.Context())
	if err != nil {
		writeError(w, r, h.log, err)
		return
	}
	out := struct {
		Accounts []accountJSON `json:"accounts"`
	}{Accounts: []accountJSON{}}
	for _, a := range accounts {
		out.Accounts = append(out.Accounts, accountJSON(a))
	}
	httpserver.WriteJSON(w, http.StatusOK, out)
}

// uiConfig handles GET /config.json: what the browser needs to know and the
// server knows, such as where the notifications API is.
func (h *handler) uiConfig(w http.ResponseWriter, _ *http.Request) {
	httpserver.WriteJSON(w, http.StatusOK, map[string]string{"notifications_url": h.notificationsURL})
}

func pathID(r *http.Request) (uuid.UUID, error) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		return uuid.Nil, &transfers.ValidationError{Field: "id", Reason: "must be a UUID"}
	}
	return id, nil
}
