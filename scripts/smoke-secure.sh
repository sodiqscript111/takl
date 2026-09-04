#!/usr/bin/env sh
set -eu

TMP_DIR=".tmp-smoke-secure"
rm -rf "$TMP_DIR"
mkdir -p "$TMP_DIR/bin" "$TMP_DIR/pki" "$TMP_DIR/pki/node-a" "$TMP_DIR/pki/node-b"

go build -o "$TMP_DIR/bin/takld" ./cmd/takld
go build -o "$TMP_DIR/bin/taklctl" ./cmd/taklctl

GOSSIP_KEY=$($TMP_DIR/bin/taklctl keygen gossip)
$TMP_DIR/bin/taklctl ca init --out-dir "$TMP_DIR/pki" --cn takl-smoke-ca --days 30 > "$TMP_DIR/pki/ca-files.txt"
$TMP_DIR/bin/taklctl ca issue --ca-cert "$TMP_DIR/pki/ca.crt" --ca-key "$TMP_DIR/pki/ca.key" --out-dir "$TMP_DIR/pki/node-a" --node node-a --ips 127.0.0.1 --dns localhost --days 30
$TMP_DIR/bin/taklctl ca issue --ca-cert "$TMP_DIR/pki/ca.crt" --ca-key "$TMP_DIR/pki/ca.key" --out-dir "$TMP_DIR/pki/node-b" --node node-b --ips 127.0.0.1 --dns localhost --days 30

"$TMP_DIR/bin/takld" --node-id node-a --backend stub --http-addr 127.0.0.1:18091 --sync-addr 127.0.0.1:18101 --bind 17941 --cluster-profile wan --gossip-key "$GOSSIP_KEY" --sync-tls-ca-file "$TMP_DIR/pki/ca.crt" --sync-tls-cert-file "$TMP_DIR/pki/node-a/node-a.crt" --sync-tls-key-file "$TMP_DIR/pki/node-a/node-a.key" --db "$TMP_DIR/node-a.db" > "$TMP_DIR/node-a.log" 2>&1 &
PID_A=$!
"$TMP_DIR/bin/takld" --node-id node-b --backend stub --http-addr 127.0.0.1:18092 --sync-addr 127.0.0.1:18102 --bind 17942 --join 127.0.0.1:17941 --cluster-profile wan --gossip-key "$GOSSIP_KEY" --sync-tls-ca-file "$TMP_DIR/pki/ca.crt" --sync-tls-cert-file "$TMP_DIR/pki/node-b/node-b.crt" --sync-tls-key-file "$TMP_DIR/pki/node-b/node-b.key" --db "$TMP_DIR/node-b.db" > "$TMP_DIR/node-b.log" 2>&1 &
PID_B=$!

cleanup() {
  kill "$PID_A" "$PID_B" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

sleep 6

$TMP_DIR/bin/taklctl -addr http://127.0.0.1:18091 cluster
$TMP_DIR/bin/taklctl -addr http://127.0.0.1:18092 cluster

echo "smoke-secure ok"
