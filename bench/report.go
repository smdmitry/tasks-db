package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// report — сводная markdown-таблица по results/*.json (+ ресурсы контейнеров из *.docker.json от run.sh).
func report(args []string) {
	dir := "/results"
	if len(args) > 0 {
		dir = args[0]
	}
	files, _ := filepath.Glob(filepath.Join(dir, "*.json"))
	var all []Summary
	for _, f := range files {
		if filepath.Ext(f) != ".json" || len(f) > 12 && f[len(f)-12:] == ".docker.json" {
			continue
		}
		data, err := os.ReadFile(f)
		if err != nil {
			continue
		}
		var s Summary
		if json.Unmarshal(data, &s) == nil && s.Backend != "" {
			if s.Config.Role == "producer" {
				s.Variant = fmt.Sprintf("MAXWRITE writers=%d", s.Config.Writers)
				if s.Config.PayloadRandom {
					s.Variant += " payload=random"
				}
			}
			all = append(all, s)
		}
	}
	sort.Slice(all, func(i, j int) bool { return all[i].RunID < all[j].RunID })

	fmt.Println("| run | backend | variant | prefill batch rows/s (err) | write/s | write p99 ms | read avg/s | read peak/s | read q/s | read p99 ms | max lag (virt s) | errs w/r | DB CPU avg/max % | DB mem max MB | DB disk max MB | storage (bench) |")
	fmt.Println("|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|")
	for _, s := range all {
		res := map[string]float64{}
		if data, err := os.ReadFile(filepath.Join(dir, s.RunID+".docker.json")); err == nil {
			_ = json.Unmarshal(data, &res)
		}
		fmt.Printf("| %s | %s | %s | %.0f (%d) | %.0f | %.1f | %.0f | %.0f | %.0f | %.1f | %.0f | %d/%d | %.0f/%.0f | %.0f | %.0f | %s|\n",
			s.RunID, s.Backend, s.Variant, s.PrefillRPS, s.PrefillErrs, s.WriteRPS, s.WriteLat.P99, s.ReadRPS, s.PeakReadRPS, s.ReadQPS, s.ReadLat.P99,
			s.MaxLag, s.WriteErrs, s.ReadErrs, res["cpu_avg"], res["cpu_max"], res["mem_max_mb"], res["disk_max_mb"], fmtSize(s.MaxSize))
	}
}
