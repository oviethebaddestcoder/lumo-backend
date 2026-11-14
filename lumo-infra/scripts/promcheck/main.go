// Command promcheck parses every PromQL expression in the alert rules and
// the generated dashboard, and checks each alert has a severity and a
// runbook anchor. CI runs it; run locally with:
//
//	cd lumo-infra/scripts/promcheck && go run . ../../k8s/alerts.yaml ../../k8s/dashboards.yaml
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/prometheus/prometheus/promql/parser"
	"gopkg.in/yaml.v3"
)

func main() {
	bad := 0
	report := func(format string, a ...any) { bad++; fmt.Printf(format+"\n", a...) }
	p := parser.NewParser(parser.Options{})
	runbook, _ := os.ReadFile("../../docs/runbook.md")
	for _, path := range os.Args[1:] {
		b, err := os.ReadFile(path)
		if err != nil {
			report("%s: %v", path, err)
			continue
		}
		var doc struct {
			Kind string
			Data map[string]string
			Spec struct {
				Groups []struct {
					Rules []struct {
						Alert, Expr string
						Labels      map[string]string
						Annotations map[string]string
					}
				}
			}
		}
		if err := yaml.Unmarshal(b, &doc); err != nil {
			report("%s: %v", path, err)
			continue
		}
		n := 0
		for _, g := range doc.Spec.Groups {
			for _, r := range g.Rules {
				n++
				if _, err := p.ParseExpr(r.Expr); err != nil {
					report("%s: %v", r.Alert, err)
				}
				anchor := strings.ToLower(r.Alert)
				if r.Labels["severity"] != "page" && r.Labels["severity"] != "ticket" {
					report("%s: severity must be page or ticket", r.Alert)
				}
				if !strings.HasSuffix(r.Annotations["runbook"], "#"+anchor) || !strings.Contains(string(runbook), "### "+r.Alert+"\n") {
					report("%s: needs a runbook section '### %s'", r.Alert, r.Alert)
				}
			}
		}
		for name, js := range doc.Data {
			var d struct {
				Panels []struct {
					Title   string
					Targets []struct{ Expr string }
				}
			}
			if err := json.Unmarshal([]byte(js), &d); err != nil {
				report("%s: %v", name, err)
				continue
			}
			for _, pn := range d.Panels {
				for _, t := range pn.Targets {
					n++
					if _, err := p.ParseExpr(t.Expr); err != nil {
						report("%s / %s: %v", name, pn.Title, err)
					}
				}
			}
		}
		fmt.Printf("%s: %d expressions\n", path, n)
	}
	if bad > 0 {
		fmt.Printf("%d problems\n", bad)
		os.Exit(1)
	}
}
