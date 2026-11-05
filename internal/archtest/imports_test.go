package archtest

import (
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

const module = "github.com/aleksbeloushkin-srb/pinkponies-bank"

// pkgImports maps a package path to its imports, without and with its test
// files.
type pkgImports struct {
	prod, test map[string]bool
}

func loadImports(t *testing.T) map[string]*pkgImports {
	t.Helper()
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]*pkgImports{}
	fset := token.NewFileSet()
	err = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && (d.Name() == ".git" || d.Name() == "bin" || d.Name() == "node_modules") {
			return filepath.SkipDir
		}
		if d.IsDir() || !strings.HasSuffix(path, ".go") {
			return nil
		}
		f, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, filepath.Dir(path))
		if err != nil {
			return err
		}
		pkg := module + "/" + filepath.ToSlash(rel)
		if rel == "." {
			pkg = module
		}
		p := out[pkg]
		if p == nil {
			p = &pkgImports{prod: map[string]bool{}, test: map[string]bool{}}
			out[pkg] = p
		}
		for _, imp := range f.Imports {
			path, _ := strconv.Unquote(imp.Path.Value)
			p.test[path] = true
			if !strings.HasSuffix(f.Name.Name, "_test") && !strings.HasSuffix(fset.File(f.Pos()).Name(), "_test.go") {
				p.prod[path] = true
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) < 20 {
		t.Fatalf("found only %d packages; is the repository root right?", len(out))
	}
	return out
}

func under(pkg, prefix string) bool {
	p := module + "/" + prefix
	return pkg == p || strings.HasPrefix(pkg, p+"/")
}

var services = []string{"internal/ledger", "internal/transfers", "internal/notifications"}

// No service imports another service's packages. They meet only in the
// contracts: gen/ledger/v1 (gRPC) and api/events/v1 (Kafka).
func TestServicesDoNotImportEachOther(t *testing.T) {
	for pkg, imps := range loadImports(t) {
		for _, own := range services {
			if !under(pkg, own) {
				continue
			}
			for imp := range imps.prod {
				for _, other := range append(slices.DeleteFunc(slices.Clone(services), func(s string) bool { return s == own }), "internal/fakerisk") {
					if under(imp, other) {
						t.Errorf("%s imports %s", pkg, imp)
					}
				}
			}
			// Tests may drive the fake screening provider, never another
			// service.
			for imp := range imps.test {
				for _, other := range services {
					if other != own && under(imp, other) {
						t.Errorf("%s (tests) imports %s", pkg, imp)
					}
				}
			}
		}
	}
}

// Each binary wires its own service and nothing of the others.
func TestCommandsWireOnlyTheirService(t *testing.T) {
	own := map[string]string{
		"cmd/ledger": "internal/ledger", "cmd/transfers": "internal/transfers",
		"cmd/notifications": "internal/notifications", "cmd/fakerisk": "internal/fakerisk",
	}
	all := append(slices.Clone(services), "internal/fakerisk")
	imports := loadImports(t)
	for cmd, svc := range own {
		imps, ok := imports[module+"/"+cmd]
		if !ok {
			t.Fatalf("no package %s", cmd)
		}
		for imp := range imps.test {
			for _, other := range all {
				if other != svc && under(imp, other) {
					t.Errorf("%s imports %s", cmd, imp)
				}
			}
		}
	}
}

// The domain packages import the standard library, uuid and, for
// notifications, the event contract: nothing about SQL, gRPC, Kafka or HTTP.
func TestDomainsAreLeaves(t *testing.T) {
	allowed := map[string][]string{
		"internal/ledger":        {"github.com/google/uuid"},
		"internal/transfers":     {"github.com/google/uuid"},
		"internal/notifications": {"github.com/google/uuid", module + "/api/events/v1"},
	}
	imports := loadImports(t)
	for dir, ok := range allowed {
		for imp := range imports[module+"/"+dir].prod {
			if strings.Contains(strings.SplitN(imp, "/", 2)[0], ".") && !slices.Contains(ok, imp) {
				t.Errorf("domain %s imports %s", dir, imp)
			}
		}
	}
}

// Use cases depend on ports they declare, not on adapter libraries.
func TestUseCasesImportNoAdapters(t *testing.T) {
	forbidden := []string{
		"github.com/jackc/pgx", "google.golang.org/grpc", "github.com/twmb/franz-go",
		"github.com/prometheus", "net/http", module + "/gen/", module + "/pkg/txctx",
	}
	for pkg, imps := range loadImports(t) {
		if !strings.HasSuffix(pkg, "/app") || !strings.HasPrefix(pkg, module+"/internal/") {
			continue
		}
		for imp := range imps.prod {
			for _, f := range forbidden {
				if strings.HasPrefix(imp, f) {
					t.Errorf("%s imports %s", pkg, imp)
				}
			}
		}
	}
}

// The shared platform knows no service; contracts and pkg know nothing
// internal at all.
func TestSharedCodeKnowsNoService(t *testing.T) {
	for pkg, imps := range loadImports(t) {
		shared := under(pkg, "internal/platform") || under(pkg, "pkg") || under(pkg, "api") || under(pkg, "gen")
		if !shared {
			continue
		}
		for imp := range imps.test {
			for _, svc := range append(slices.Clone(services), "internal/fakerisk") {
				if under(imp, svc) {
					t.Errorf("%s imports %s", pkg, imp)
				}
			}
			if (under(pkg, "pkg") || under(pkg, "api") || under(pkg, "gen")) && under(imp, "internal") {
				t.Errorf("%s imports %s", pkg, imp)
			}
		}
	}
}
