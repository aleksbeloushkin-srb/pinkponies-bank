package main

import (
	"strings"
	"testing"
)

// The events of a test file that does not compile, as go test -json
// reports them since Go 1.24 (times trimmed).
const brokenBuild = `{"ImportPath":"example.com/m/p [example.com/m/p.test]","Action":"build-output","Output":"# example.com/m/p [example.com/m/p.test]\n"}
{"ImportPath":"example.com/m/p [example.com/m/p.test]","Action":"build-output","Output":"p/p_test.go:5:40: cannot use \"not an int\" (untyped string constant) as int value in variable declaration\n"}
{"ImportPath":"example.com/m/p [example.com/m/p.test]","Action":"build-fail"}
{"Action":"start","Package":"example.com/m/p"}
{"Action":"output","Package":"example.com/m/p","Output":"FAIL\texample.com/m/p [build failed]\n"}
{"Action":"fail","Package":"example.com/m/p","Elapsed":0,"FailedBuild":"example.com/m/p [example.com/m/p.test]"}
`

// A compile error must reach both the log and the annotation; before, the
// annotation said only "[build failed]".
func TestCompilerErrorsAreKept(t *testing.T) {
	var out strings.Builder
	failed, err := report(strings.NewReader(brokenBuild), &out)
	if err != nil || !failed {
		t.Fatalf("report = %v, %v; want failed", failed, err)
	}
	const compiler = `p/p_test.go:5:40: cannot use "not an int"`
	var annotations []string
	for _, line := range strings.Split(out.String(), "\n") {
		if strings.HasPrefix(line, "::error ") {
			annotations = append(annotations, line)
		}
	}
	if !strings.Contains(out.String(), compiler+" (untyped") {
		t.Fatalf("the log lost the compiler message:\n%s", out.String())
	}
	if len(annotations) != 2 {
		t.Fatalf("annotations = %q, want the build and the package", annotations)
	}
	for _, a := range annotations {
		if !strings.Contains(a, compiler) {
			t.Errorf("annotation without the compiler message: %s", a)
		}
	}
}

func TestPassingRunIsQuiet(t *testing.T) {
	in := `{"Action":"output","Package":"p","Test":"TestA","Output":"--- PASS: TestA\n"}
{"Action":"pass","Package":"p","Test":"TestA"}
{"Action":"pass","Package":"p"}
`
	var out strings.Builder
	failed, err := report(strings.NewReader(in), &out)
	if err != nil || failed || strings.Contains(out.String(), "::error") {
		t.Fatalf("report = %v, %v, output %q", failed, err, out.String())
	}
}

func TestFailedTestIsAnnotated(t *testing.T) {
	in := `{"Action":"output","Package":"p","Test":"TestA","Output":"    a_test.go:9: got 1, want 2\n"}
{"Action":"fail","Package":"p","Test":"TestA"}
`
	var out strings.Builder
	if failed, _ := report(strings.NewReader(in), &out); !failed {
		t.Fatal("not failed")
	}
	if !strings.Contains(out.String(), "::error title=p.TestA failed::a_test.go:9: got 1, want 2") {
		t.Fatalf("output %q", out.String())
	}
}
