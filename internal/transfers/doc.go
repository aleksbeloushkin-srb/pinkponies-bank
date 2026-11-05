// Package transfers is the domain model of the transfers service: a
// transfer of money between two customer accounts, the state machine it
// moves through, and the log of the saga steps that move it.
//
// It imports only the standard library and google/uuid. The rules live
// here: which status may follow which, which outcome a step may record, and
// when a change bumps the version that long polls and event consumers
// compare.
package transfers
