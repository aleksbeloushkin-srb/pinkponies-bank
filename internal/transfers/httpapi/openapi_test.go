package httpapi

import (
	"os"
	"strings"
	"testing"
)

// Every route the handler serves is documented in the OpenAPI contract,
// with its method. A cheap check that catches a route added without its
// contract; the schemas themselves are reviewed by hand.
func TestRoutesAreInTheContract(t *testing.T) {
	spec, err := os.ReadFile("../../../api/openapi/transfers.yaml")
	if err != nil {
		t.Fatal(err)
	}
	doc := string(spec)
	for _, pattern := range []string{
		"POST /v1/transfers", "GET /v1/transfers/{id}", WaitPattern, "GET /v1/accounts", "GET /config.json",
	} {
		method, path, _ := strings.Cut(pattern, " ")
		i := strings.Index(doc, "\n  "+path+":\n")
		if i < 0 {
			t.Errorf("path %s is not in the contract", path)
			continue
		}
		section := doc[i+1:]
		if next := strings.Index(section[1:], "\n  /"); next >= 0 {
			section = section[:next+1]
		}
		if !strings.Contains(section, "\n    "+strings.ToLower(method)+":\n") {
			t.Errorf("%s %s is not in the contract", method, path)
		}
	}
	for _, code := range []string{"invalid_argument", "too_large", "not_found", "idempotency_conflict", "too_many_waiters", "unavailable", "upstream_unavailable", "internal"} {
		if !strings.Contains(doc, code) {
			t.Errorf("error code %s is not in the contract", code)
		}
	}
}
