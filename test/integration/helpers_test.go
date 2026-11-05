//go:build integration

package integration

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/platform/httpserver"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers/app"
	"github.com/aleksbeloushkin-srb/pinkponies-bank/internal/transfers/longpoll"
)

func jsonDecode(resp *http.Response, v any) error { return json.NewDecoder(resp.Body).Decode(v) }

func errorsIs(err, target error) bool { return errors.Is(err, target) }

func newBatcher(e *sagaEnv) *longpoll.Batcher {
	return longpoll.NewBatcher(e.waiters, e.store, longpoll.NewMetrics(prometheus.NewRegistry()),
		longpoll.BatcherConfig{Interval: 20 * time.Millisecond, QueryTimeout: time.Second}, discard())
}

func appCreate(e *sagaEnv, key string, amount int64) app.CreateTransferRequest {
	return app.CreateTransferRequest{IdempotencyKey: key, SourceAccountID: e.src, DestinationAccountID: e.dst, AmountMinor: amount, Currency: "EUR"}
}

func httpserverMetrics() *httpserver.Metrics { return httpserver.NewMetrics(prometheus.NewRegistry()) }
