// Package channels holds the fake delivery channels of the notifications
// service: a log line, and a webhook POST to a configurable URL. Both
// implement app.Channel. A real bank would add push, e-mail or SMS
// providers here (see docs/EXTENDING.md).
package channels

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"time"

	"github.com/google/uuid"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/notifications"
)

// Log writes each notification as a log line. It cannot fail, which makes
// it the channel to compare the webhook against.
type Log struct {
	log *slog.Logger
}

// NewLog returns the log channel.
func NewLog(log *slog.Logger) *Log { return &Log{log: log} }

// Name implements app.Channel.
func (*Log) Name() string { return "log" }

// Send implements app.Channel.
func (l *Log) Send(ctx context.Context, n notifications.Notification) error {
	l.log.InfoContext(ctx, "notification", "notification_id", n.ID, "account_id", n.AccountID,
		"transfer_id", n.TransferID, "kind", n.Kind, "message", n.Message)
	return nil
}

// Webhook POSTs each notification as JSON to a URL, with the notification
// id as Idempotency-Key: a retry after a lost answer sends the same key,
// and the receiver deduplicates.
type Webhook struct {
	url     string
	client  *http.Client
	timeout time.Duration
}

// NewWebhook returns the webhook channel to target, each call bounded by
// timeout.
func NewWebhook(target string, client *http.Client, timeout time.Duration) (*Webhook, error) {
	u, err := url.Parse(target)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("channels: invalid webhook URL %q", target)
	}
	return &Webhook{url: target, client: client, timeout: timeout}, nil
}

// Name implements app.Channel.
func (*Webhook) Name() string { return "webhook" }

type webhookBody struct {
	ID         uuid.UUID `json:"id"`
	AccountID  uuid.UUID `json:"account_id"`
	TransferID uuid.UUID `json:"transfer_id"`
	Kind       string    `json:"kind"`
	Message    string    `json:"message"`
	CreatedAt  time.Time `json:"created_at"`
}

// Send implements app.Channel. Any answer other than 2xx, and any
// transport error, is a failure to retry.
func (w *Webhook) Send(ctx context.Context, n notifications.Notification) error {
	ctx, cancel := context.WithTimeout(ctx, w.timeout)
	defer cancel()
	body, err := json.Marshal(webhookBody{ID: n.ID, AccountID: n.AccountID, TransferID: n.TransferID,
		Kind: string(n.Kind), Message: n.Message, CreatedAt: n.CreatedAt.UTC()})
	if err != nil {
		return fmt.Errorf("webhook: encode: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("webhook: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", n.ID.String())
	resp, err := w.client.Do(req)
	if err != nil {
		return fmt.Errorf("webhook: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("webhook: status %d", resp.StatusCode)
	}
	return nil
}
