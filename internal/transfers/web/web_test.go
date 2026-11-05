package web

import (
	"io/fs"
	"strings"
	"testing"
)

// Every asset the page references is embedded, and the page keeps the
// accessibility basics: a language, a skip link, labelled live regions.
func TestPageAndAssets(t *testing.T) {
	page, err := fs.ReadFile(FS, "index.html")
	if err != nil {
		t.Fatal(err)
	}
	html := string(page)
	for _, asset := range []string{"/style.css", "/app.js", "/logo.svg"} {
		if !strings.Contains(html, asset) {
			t.Errorf("index.html does not reference %s", asset)
		}
		if _, err := fs.Stat(FS, strings.TrimPrefix(asset, "/")); err != nil {
			t.Errorf("%s is not embedded: %v", asset, err)
		}
	}
	for _, want := range []string{`<html lang="en">`, `class="skip"`, `aria-live="polite"`, `role="alert"`, "Pinkponies Bank"} {
		if !strings.Contains(html, want) {
			t.Errorf("index.html lacks %s", want)
		}
	}
	js, err := fs.ReadFile(FS, "app.js")
	if err != nil {
		t.Fatal(err)
	}
	// The key that makes a retried submit safe must come from the intent.
	if !strings.Contains(string(js), "Idempotency-Key") || !strings.Contains(string(js), "function keyFor") {
		t.Error("app.js must send an Idempotency-Key derived from the form intent")
	}
}
