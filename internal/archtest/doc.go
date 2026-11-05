// Package archtest holds no code, only a test that enforces the import
// rules of the repository: services talk to each other through their
// contracts (gen/, api/) and never import each other's internals; domains
// depend on nothing; use cases import no adapter libraries; the shared
// platform imports no service.
package archtest
