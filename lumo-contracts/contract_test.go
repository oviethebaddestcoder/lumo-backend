package contracts_test

// Contract compatibility: services deploy independently and Kafka keeps old
// events for days, so an event's JSON shape may only grow. This test reads
// every struct in events/ and realtime/, extracts its JSON field names and
// Go types, and compares them with testdata/schema.golden:
//
//   - removing or renaming a JSON field, or changing its type: FAIL
//   - removing an event type: FAIL
//   - adding fields or types: fine (refresh the golden with -update)
//
//	go test . -run Contract -update   # after an additive change
//
// For a real breaking change, publish a new event type (e.g. "...v2") and
// keep the old one until every consumer has moved.

import (
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

var update = flag.Bool("update", false, "rewrite testdata/schema.golden")

// schema: "pkg.Type" -> json field -> Go type
type schema map[string]map[string]string

func current(t *testing.T) schema {
	out := schema{}
	for _, dir := range []string{"events", "realtime"} {
		files, _ := filepath.Glob(filepath.Join(dir, "*.go"))
		for _, f := range files {
			if strings.HasSuffix(f, "_test.go") {
				continue
			}
			file, err := parser.ParseFile(token.NewFileSet(), f, nil, 0)
			if err != nil {
				t.Fatal(err)
			}
			for _, d := range file.Decls {
				g, ok := d.(*ast.GenDecl)
				if !ok || g.Tok != token.TYPE {
					continue
				}
				for _, sp := range g.Specs {
					ts := sp.(*ast.TypeSpec)
					st, ok := ts.Type.(*ast.StructType)
					if !ok || !ts.Name.IsExported() {
						continue
					}
					fields := map[string]string{}
					for _, fl := range st.Fields.List {
						typ := exprString(fl.Type)
						name := ""
						if fl.Tag != nil {
							tag := reflect.StructTag(strings.Trim(fl.Tag.Value, "`")).Get("json")
							name = strings.Split(tag, ",")[0]
						}
						if name == "-" {
							continue
						}
						if len(fl.Names) == 0 { // embedded
							fields["(embedded) "+typ] = typ
							continue
						}
						for _, n := range fl.Names {
							if !n.IsExported() {
								continue
							}
							k := name
							if k == "" {
								k = n.Name
							}
							fields[k] = typ
						}
					}
					out[dir+"."+ts.Name.Name] = fields
				}
			}
		}
	}
	return out
}

func exprString(e ast.Expr) string {
	switch x := e.(type) {
	case *ast.Ident:
		return x.Name
	case *ast.SelectorExpr:
		return exprString(x.X) + "." + x.Sel.Name
	case *ast.StarExpr:
		return "*" + exprString(x.X)
	case *ast.ArrayType:
		return "[]" + exprString(x.Elt)
	case *ast.MapType:
		return "map[" + exprString(x.Key) + "]" + exprString(x.Value)
	case *ast.InterfaceType:
		return "any"
	}
	return fmt.Sprintf("%T", e)
}

func (s schema) String() string {
	var b strings.Builder
	types := make([]string, 0, len(s))
	for k := range s {
		types = append(types, k)
	}
	sort.Strings(types)
	for _, ty := range types {
		fmt.Fprintf(&b, "%s\n", ty)
		fields := make([]string, 0, len(s[ty]))
		for f := range s[ty] {
			fields = append(fields, f)
		}
		sort.Strings(fields)
		for _, f := range fields {
			fmt.Fprintf(&b, "\t%s %s\n", f, s[ty][f])
		}
	}
	return b.String()
}

func parse(text string) schema {
	out := schema{}
	var cur string
	for _, line := range strings.Split(text, "\n") {
		switch {
		case line == "":
		case !strings.HasPrefix(line, "\t"):
			cur = line
			out[cur] = map[string]string{}
		default:
			f := strings.TrimPrefix(line, "\t")
			i := strings.LastIndex(f, " ")
			out[cur][f[:i]] = f[i+1:]
		}
	}
	return out
}

func TestContractCompatibility(t *testing.T) {
	got := current(t)
	golden := filepath.Join("testdata", "schema.golden")
	if *update {
		os.MkdirAll("testdata", 0o755)
		if err := os.WriteFile(golden, []byte(got.String()), 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	b, err := os.ReadFile(golden)
	if err != nil {
		t.Fatalf("%v (run: go test . -run Contract -update)", err)
	}
	want := parse(string(b))
	var broken []string
	for ty, fields := range want {
		have, ok := got[ty]
		if !ok {
			broken = append(broken, fmt.Sprintf("%s was removed", ty))
			continue
		}
		for f, typ := range fields {
			switch h, ok := have[f]; {
			case !ok:
				broken = append(broken, fmt.Sprintf("%s.%s was removed or renamed", ty, f))
			case h != typ:
				broken = append(broken, fmt.Sprintf("%s.%s changed type %s -> %s", ty, f, typ, h))
			}
		}
	}
	sort.Strings(broken)
	for _, b := range broken {
		t.Errorf("breaking contract change: %s", b)
	}
	if len(broken) == 0 && got.String() != string(b) {
		t.Errorf("contracts gained fields or types (fine): refresh the golden with `go test . -run Contract -update` and commit it")
	}
}
