// Package integration runs the services' adapters and use cases against a
// real PostgreSQL (one database per service, as in production) and a real
// Redpanda, both started with testcontainers. The tests carry the
// integration build tag, so a plain go test ./... needs no Docker:
//
//	go test -race -tags integration ./test/integration/...
package integration
