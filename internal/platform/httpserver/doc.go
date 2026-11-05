// Package httpserver holds the HTTP plumbing shared by the services' APIs:
// the middleware chain (request id, access log, metrics, panic recovery),
// JSON helpers, the server constructor with mandatory timeouts, and the
// admin handler that serves metrics and probes on a separate listener.
//
// Mapping domain errors to status codes stays in each service's own
// httpapi package: only the service knows its errors.
package httpserver
