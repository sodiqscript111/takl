package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"text/tabwriter"

	"takl/internal/version"
)

var (
	addr	= flag.String("addr", "http://127.0.0.1:8090", "takld query api address")
	limit	= flag.Int("limit", 100, "max events to fetch")
	jsonOut	= flag.Bool("json", false, "output raw json")
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "version" {
		fmt.Println(version.Info())
		return
	}
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		usage()
		os.Exit(2)
	}
	cmd, rest := args[0], args[1:]
	var path string
	switch cmd {
	case "runners":
		path = "api/v1/runners"
	case "runner":
		if len(rest) != 1 {
			usage()
			os.Exit(2)
		}
		path = "api/v1/runners/" + rest[0]
	case "builds":
		path = "api/v1/builds"
	case "queues":
		path = "api/v1/queues"
	case "containers":
		path = "api/v1/containers"
	case "mounts":
		path = "api/v1/mounts"
	case "events":
		path = fmt.Sprintf("api/v1/events?limit=%d", *limit)
	case "summary":
		path = "api/v1/summary"
	case "cluster":
		path = "api/v1/cluster"
	default:
		usage()
		os.Exit(2)
	}

	body, err := get(*addr, path)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if *jsonOut {
		fmt.Println(string(body))
		return
	}

	printTable(cmd, body)
}

func get(addr, path string) ([]byte, error) {
	resp, err := http.Get(addr + "/" + path)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("%s: %s", resp.Status, string(body))
	}
	return body, nil
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: taklctl [-addr URL] [-json] <command> [args]")
	fmt.Fprintln(os.Stderr, "commands: runners, runner <id>, builds, queues, containers, mounts, events, cluster")
}

func printTable(cmd string, body []byte) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	defer w.Flush()

	switch cmd {
	case "runners":
		var items []map[string]any
		_ = json.Unmarshal(body, &items)
		fmt.Fprintln(w, "ID\tHOSTNAME\tSTATUS\tCAPACITY\tACTIVE\tCPU%\tMEM%")
		for _, r := range items {
			fmt.Fprintf(w, "%v\t%v\t%v\t%v\t%v\t%.2f\t%.2f\n", r["runner_id"], r["hostname"], r["status"], r["worker_capacity"], r["active_builds"], r["cpu_util"], r["mem_util"])
		}
	case "builds":
		var items []map[string]any
		_ = json.Unmarshal(body, &items)
		fmt.Fprintln(w, "ID\tRUNNER\tPROJECT\tSTATUS")
		for _, b := range items {
			fmt.Fprintf(w, "%v\t%v\t%v\t%v\n", b["build_id"], b["runner_id"], b["project_id"], b["status"])
		}
	case "events":
		var items []map[string]any
		_ = json.Unmarshal(body, &items)
		fmt.Fprintln(w, "RUNNER\tSEQ\tTYPE\tHLC")
		for _, e := range items {
			hlc, _ := e["hlc"].(map[string]any)
			ts := int64(hlc["ts"].(float64))
			fmt.Fprintf(w, "%v\t%v\t%v\t%d\n", e["runner_id"], e["seq"], e["type"], ts)
		}
	case "cluster":
		var c map[string]any
		_ = json.Unmarshal(body, &c)
		fmt.Fprintln(w, "ACTIVE RUNNERS\tWORKER CAPACITY\tAVAILABLE\tACTIVE BUILDS\tAVG CPU%\tAVG MEM%")

		cpu, _ := c["avg_cpu_util"].(float64)
		mem, _ := c["avg_mem_util"].(float64)
		fmt.Fprintf(w, "%v\t%v\t%v\t%v\t%.2f\t%.2f\n", c["active_runners"], c["worker_capacity"], c["available_workers"], c["active_builds"], cpu, mem)
	default:
		fmt.Println(string(body))
	}
}
