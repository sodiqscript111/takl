package main

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"

	"takl/internal/version"
)

var (
	addr    = flag.String("addr", "http://127.0.0.1:8090", "takld query api address")
	limit   = flag.Int("limit", 100, "max events to fetch")
	jsonOut = flag.Bool("json", false, "output raw json")
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "version" {
		fmt.Println(version.Info())
		return
	}
	if len(os.Args) > 2 && os.Args[1] == "keygen" {
		if err := runKeygen(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) > 2 && os.Args[1] == "ca" {
		if err := runCA(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
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

func runKeygen(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: taklctl keygen gossip")
	}
	switch args[0] {
	case "gossip":
		b := make([]byte, 32)
		if _, err := rand.Read(b); err != nil {
			return err
		}
		fmt.Println(base64.StdEncoding.EncodeToString(b))
		return nil
	default:
		return fmt.Errorf("unknown keygen command: %s", args[0])
	}
}

func runCA(args []string) error {
	if len(args) < 1 {
		return fmt.Errorf("usage: taklctl ca <init|issue>")
	}
	switch args[0] {
	case "init":
		fs := flag.NewFlagSet("ca init", flag.ContinueOnError)
		fs.SetOutput(io.Discard)
		outDir := fs.String("out-dir", ".", "")
		cn := fs.String("cn", "takl-ca", "")
		days := fs.Int("days", 3650, "")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		return caInit(*outDir, *cn, *days)
	case "issue":
		fs := flag.NewFlagSet("ca issue", flag.ContinueOnError)
		fs.SetOutput(io.Discard)
		caCertPath := fs.String("ca-cert", "", "")
		caKeyPath := fs.String("ca-key", "", "")
		outDir := fs.String("out-dir", ".", "")
		node := fs.String("node", "", "")
		ipsRaw := fs.String("ips", "", "")
		dnsRaw := fs.String("dns", "", "")
		days := fs.Int("days", 365, "")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}
		if strings.TrimSpace(*caCertPath) == "" || strings.TrimSpace(*caKeyPath) == "" || strings.TrimSpace(*node) == "" {
			return fmt.Errorf("usage: taklctl ca issue --ca-cert <file> --ca-key <file> --node <id> [--ips a,b] [--dns a,b] [--out-dir dir]")
		}
		return caIssue(*caCertPath, *caKeyPath, *outDir, *node, splitCSV(*ipsRaw), splitCSV(*dnsRaw), *days)
	default:
		return fmt.Errorf("unknown ca command: %s", args[0])
	}
}

func caInit(outDir, cn string, days int) error {
	if days <= 0 {
		return fmt.Errorf("days must be > 0")
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	serial, err := serialNumber()
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	tpl := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: cn, Organization: []string{"takl"}},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(time.Duration(days) * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
		MaxPathLenZero:        false,
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		return err
	}
	certOut := filepath.Join(outDir, "ca.crt")
	keyOut := filepath.Join(outDir, "ca.key")
	if err := writePEM(certOut, "CERTIFICATE", der, 0o644); err != nil {
		return err
	}
	b, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return err
	}
	if err := writePEM(keyOut, "EC PRIVATE KEY", b, 0o600); err != nil {
		return err
	}
	fmt.Println(certOut)
	fmt.Println(keyOut)
	return nil
}

func caIssue(caCertPath, caKeyPath, outDir, node string, ips, dns []string, days int) error {
	if days <= 0 {
		return fmt.Errorf("days must be > 0")
	}
	if err := os.MkdirAll(outDir, 0o755); err != nil {
		return err
	}
	caCert, caKey, err := loadCA(caCertPath, caKeyPath)
	if err != nil {
		return err
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	serial, err := serialNumber()
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	tpl := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName:   node,
			Organization: []string{"takl"},
		},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(time.Duration(days) * 24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}
	for _, v := range ips {
		ip := net.ParseIP(v)
		if ip == nil {
			return fmt.Errorf("invalid ip: %s", v)
		}
		tpl.IPAddresses = append(tpl.IPAddresses, ip)
	}
	tpl.DNSNames = append(tpl.DNSNames, dns...)
	der, err := x509.CreateCertificate(rand.Reader, tpl, caCert, &key.PublicKey, caKey)
	if err != nil {
		return err
	}
	certOut := filepath.Join(outDir, node+".crt")
	keyOut := filepath.Join(outDir, node+".key")
	if err := writePEM(certOut, "CERTIFICATE", der, 0o644); err != nil {
		return err
	}
	b, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return err
	}
	if err := writePEM(keyOut, "EC PRIVATE KEY", b, 0o600); err != nil {
		return err
	}
	fmt.Println(certOut)
	fmt.Println(keyOut)
	return nil
}

func loadCA(caCertPath, caKeyPath string) (*x509.Certificate, *ecdsa.PrivateKey, error) {
	certPEM, err := os.ReadFile(caCertPath)
	if err != nil {
		return nil, nil, err
	}
	certBlock, _ := pem.Decode(certPEM)
	if certBlock == nil || certBlock.Type != "CERTIFICATE" {
		return nil, nil, errors.New("invalid ca cert pem")
	}
	cert, err := x509.ParseCertificate(certBlock.Bytes)
	if err != nil {
		return nil, nil, err
	}
	keyPEM, err := os.ReadFile(caKeyPath)
	if err != nil {
		return nil, nil, err
	}
	keyBlock, _ := pem.Decode(keyPEM)
	if keyBlock == nil {
		return nil, nil, errors.New("invalid ca key pem")
	}
	key, err := x509.ParseECPrivateKey(keyBlock.Bytes)
	if err != nil {
		return nil, nil, err
	}
	return cert, key, nil
}

func serialNumber() (*big.Int, error) {
	limit := new(big.Int).Lsh(big.NewInt(1), 128)
	return rand.Int(rand.Reader, limit)
}

func writePEM(path, pemType string, der []byte, perm os.FileMode) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	defer f.Close()
	return pem.Encode(f, &pem.Block{Type: pemType, Bytes: der})
}

func splitCSV(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
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
	fmt.Fprintln(os.Stderr, "security commands: keygen gossip | ca init | ca issue")
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
