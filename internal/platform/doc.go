// Package platform holds process-level plumbing shared by every binary in
// this repository: strict configuration from the environment, the
// structured logger, a readiness flag, a clock, an owner for background
// goroutines and helpers to run and stop HTTP servers.
//
// It contains no business logic and imports no service package.
package platform
