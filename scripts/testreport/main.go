// Command testreport reads `go test -json` from stdin, prints the usual test
// output, and turns every failed test into a GitHub Actions error annotation,
// so a failure is visible on the run page without opening the job log. It
// exits 1 if any test or package failed.
//
// Since Go 1.24 compiler errors arrive as build-output events of their own;
// they are printed too, and a package that did not build carries its
// compiler messages in its annotation.
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"
)

type event struct {
	Action     string
	Package    string
	Test       string
	Output     string
	ImportPath string // build events
	// FailedBuild names the build (an ImportPath) that made a package fail.
	FailedBuild string
}

func main() {
	failed, err := report(os.Stdin, os.Stdout)
	if err != nil {
		fmt.Fprintln(os.Stderr, "testreport:", err)
		os.Exit(1)
	}
	if failed {
		os.Exit(1)
	}
}

// report copies the test output from r to w with an annotation per failure.
// It reports whether anything failed.
func report(r io.Reader, w io.Writer) (failed bool, err error) {
	output := map[string][]string{} // package/test -> output lines
	build := map[string][]string{}  // import path -> compiler output
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 1024*1024), 16*1024*1024)
	for sc.Scan() {
		var e event
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			fmt.Fprintln(w, sc.Text()) // not JSON: errors of the go command itself
			continue
		}
		key := e.Package + "/" + e.Test
		switch e.Action {
		case "build-output":
			fmt.Fprint(w, e.Output)
			build[e.ImportPath] = append(build[e.ImportPath], e.Output)
		case "build-fail":
			failed = true
			fmt.Fprintf(w, "::error title=build of %s failed::%s\n", e.ImportPath, annotation(build[e.ImportPath]))
		case "output":
			fmt.Fprint(w, e.Output)
			output[key] = append(output[key], e.Output)
		case "fail":
			failed = true
			name := e.Package
			if e.Test != "" {
				name += "." + e.Test
			}
			lines := output[key]
			if e.FailedBuild != "" {
				lines = slices.Concat(build[e.FailedBuild], lines)
			}
			fmt.Fprintf(w, "::error title=%s failed::%s\n", name, annotation(lines))
		}
	}
	return failed, sc.Err()
}

// annotation keeps the last lines of a test's output, the part that usually
// holds the assertion, in the single-line form annotations require.
func annotation(lines []string) string {
	const keep = 12
	if len(lines) > keep {
		lines = lines[len(lines)-keep:]
	}
	s := strings.TrimSpace(strings.Join(lines, ""))
	s = strings.ReplaceAll(s, "%", "%25")
	s = strings.ReplaceAll(s, "\r", "%0D")
	return strings.ReplaceAll(s, "\n", "%0A")
}
