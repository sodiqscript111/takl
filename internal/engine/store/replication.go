package store

import (
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"

	"takl/internal/model"
)

// Replication bookkeeping.
//
// Every change to this store gets the next local version (nextVersion). A peer
// that has pulled up to version w is missing exactly the rows and events with
// version > w, even ones that reached this node late through a relay.
//
// Each store has a random store id. A peer's watermark is only meaningful for
// the store id it was taken against: a wiped or recreated database gets a new
// id, and peers start over from 0.
//
// The peers table holds, per peer store: how far we have pulled it
// (watermark), how far it has pulled us (ack, reported in its requests), when
// we last exchanged with it in either direction, and whether we have forgotten
// it. Tombstones are only dropped once every peer we have not forgotten has
// acked past them. A peer unreachable for too long is forgotten; until it
// wipes, we refuse to sync with it in either direction.

func (s *Store) init() error {
	for _, stmt := range []string{
		`PRAGMA journal_mode=WAL`,
		`PRAGMA busy_timeout=5000`,
	} {
		if _, err := s.db.Exec(stmt); err != nil {
			return fmt.Errorf("schema: %w", err)
		}
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	for _, stmt := range []string{
		`CREATE TABLE IF NOT EXISTS rows (
			kind TEXT NOT NULL,
			key TEXT NOT NULL,
			owner TEXT NOT NULL,
			hlc_ts INTEGER NOT NULL,
			hlc_seq INTEGER NOT NULL,
			tombstone INTEGER NOT NULL DEFAULT 0,
			payload BLOB NOT NULL,
			ver INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (kind, key)
		)`,
		`CREATE INDEX IF NOT EXISTS rows_hlc ON rows (kind, hlc_ts, hlc_seq)`,
		`CREATE INDEX IF NOT EXISTS idx_rows_owner ON rows(owner)`,
		`CREATE TABLE IF NOT EXISTS events_v3 (
			origin TEXT NOT NULL,
			runner_id TEXT NOT NULL,
			seq INTEGER NOT NULL,
			type TEXT NOT NULL,
			payload TEXT NOT NULL,
			hlc_ts INTEGER NOT NULL,
			hlc_seq INTEGER NOT NULL,
			ver INTEGER NOT NULL,
			PRIMARY KEY (origin, runner_id, seq)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_events3_ts ON events_v3(hlc_ts, hlc_seq)`,
		`CREATE INDEX IF NOT EXISTS idx_events3_ver ON events_v3(ver)`,
		`CREATE TABLE IF NOT EXISTS store_meta (
			id INTEGER PRIMARY KEY CHECK (id = 1),
			store_id TEXT NOT NULL,
			version INTEGER NOT NULL,
			served INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS peers (
			store_id TEXT PRIMARY KEY,
			node_id TEXT NOT NULL DEFAULT '',
			watermark INTEGER NOT NULL DEFAULT 0,
			ack INTEGER NOT NULL DEFAULT 0,
			last_seen INTEGER NOT NULL DEFAULT 0,
			forgotten INTEGER NOT NULL DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS peer_addrs (
			addr TEXT PRIMARY KEY,
			store_id TEXT NOT NULL
		)`,
	} {
		if _, err := tx.Exec(stmt); err != nil {
			return fmt.Errorf("schema: %w", err)
		}
	}
	if err := migrate(tx); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}
	if _, err := tx.Exec(`CREATE INDEX IF NOT EXISTS rows_ver ON rows (ver)`); err != nil {
		return fmt.Errorf("schema: %w", err)
	}
	var n int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM store_meta`).Scan(&n); err != nil {
		return err
	}
	if n == 0 {
		var maxVer uint64
		if err := tx.QueryRow(`SELECT MAX(
			(SELECT COALESCE(MAX(ver), 0) FROM rows),
			(SELECT COALESCE(MAX(ver), 0) FROM events_v3))`).Scan(&maxVer); err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO store_meta (id, store_id, version, served) VALUES (1, ?, ?, 0)`, newStoreID(), maxVer); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// migrate upgrades databases written by the HLC-watermark protocol: rows get a
// version column, events move to events_v3 (keyed by origin), and the old
// per-peer HLC watermarks are dropped (peers resync once from version 0).
func migrate(tx *sql.Tx) error {
	var hasVer int
	if err := tx.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('rows') WHERE name = 'ver'`).Scan(&hasVer); err != nil {
		return err
	}
	if hasVer == 0 {
		if _, err := tx.Exec(`ALTER TABLE rows ADD COLUMN ver INTEGER NOT NULL DEFAULT 0`); err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE rows SET ver = 1`); err != nil {
			return err
		}
	}
	if tableExists(tx, "events_v2") {
		if _, err := tx.Exec(`INSERT OR IGNORE INTO events_v3 (origin, runner_id, seq, type, payload, hlc_ts, hlc_seq, ver)
			SELECT '', runner_id, seq, type, payload, hlc_ts, hlc_seq, 1 FROM events_v2`); err != nil {
			return err
		}
		if _, err := tx.Exec(`DROP TABLE events_v2`); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(`DROP TABLE IF EXISTS watermarks`); err != nil {
		return err
	}
	return nil
}

func tableExists(tx *sql.Tx, name string) bool {
	var n int
	_ = tx.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?`, name).Scan(&n)
	return n > 0
}

func newStoreID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

type queryRower interface {
	QueryRow(query string, args ...any) *sql.Row
}

func storeIDTx(q queryRower) (string, error) {
	var id string
	err := q.QueryRow(`SELECT store_id FROM store_meta WHERE id = 1`).Scan(&id)
	return id, err
}

// StoreID returns this store's current id. It changes when the store wipes.
func (s *Store) StoreID() (string, error) {
	return storeIDTx(s.db)
}

// Version returns the last local version handed out.
func (s *Store) Version() (uint64, error) {
	var v uint64
	err := s.db.QueryRow(`SELECT version FROM store_meta WHERE id = 1`).Scan(&v)
	return v, err
}

// Peer is what this store knows about one peer store.
type Peer struct {
	StoreID   string
	NodeID    string
	Watermark uint64 // how far we have pulled its store
	Ack       uint64 // how far it has pulled ours
	LastSeen  int64  // unix ms of the last exchange in either direction
	Forgotten bool
}

func (s *Store) Peer(storeID string) (Peer, bool, error) {
	p := Peer{StoreID: storeID}
	var forgotten int
	err := s.db.QueryRow(`SELECT node_id, watermark, ack, last_seen, forgotten FROM peers WHERE store_id = ?`, storeID).
		Scan(&p.NodeID, &p.Watermark, &p.Ack, &p.LastSeen, &forgotten)
	if errors.Is(err, sql.ErrNoRows) {
		return p, false, nil
	}
	p.Forgotten = forgotten != 0
	return p, err == nil, err
}

// PeerByAddr returns what we know about the store last seen at addr. An unknown
// address gives a zero Peer (empty store id, watermark 0).
func (s *Store) PeerByAddr(addr string) (Peer, error) {
	var id string
	err := s.db.QueryRow(`SELECT store_id FROM peer_addrs WHERE addr = ?`, addr).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return Peer{}, nil
	}
	if err != nil {
		return Peer{}, err
	}
	p, _, err := s.Peer(id)
	return p, err
}

// ServeRequest is a peer's pull, as seen by the serving store.
type ServeRequest struct {
	StoreID     string // requester's store id
	NodeID      string // requester's node id
	PeerStoreID string // our store id as the requester knows it
	Since       uint64 // requester's watermark for PeerStoreID
	NowMs       int64
}

type ServeResult struct {
	StoreID   string
	Version   uint64
	Rows      []Row
	Events    []model.Event
	Forgotten bool // we have forgotten the requester: it must wipe
}

// Serve answers a pull. In one transaction it records the requester's ack,
// reads every row and event changed after the requester's watermark, and
// reports the current version, which becomes the requester's new watermark.
func (s *Store) Serve(req ServeRequest) (ServeResult, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return ServeResult{}, err
	}
	defer func() { _ = tx.Rollback() }()

	var res ServeResult
	var served uint64
	if err := tx.QueryRow(`SELECT store_id, version, served FROM store_meta WHERE id = 1`).Scan(&res.StoreID, &res.Version, &served); err != nil {
		return ServeResult{}, err
	}
	since := req.Since
	if req.PeerStoreID != res.StoreID {
		since = 0
	}
	if req.StoreID != "" && req.StoreID != res.StoreID {
		var forgotten int
		err := tx.QueryRow(`SELECT forgotten FROM peers WHERE store_id = ?`, req.StoreID).Scan(&forgotten)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return ServeResult{}, err
		}
		if forgotten != 0 {
			res.Forgotten = true
			return res, tx.Commit()
		}
		if _, err := tx.Exec(`INSERT INTO peers (store_id, node_id, ack, last_seen) VALUES (?, ?, ?, ?)
			ON CONFLICT(store_id) DO UPDATE SET
				node_id = excluded.node_id,
				ack = MAX(ack, excluded.ack),
				last_seen = MAX(last_seen, excluded.last_seen)`, req.StoreID, req.NodeID, since, req.NowMs); err != nil {
			return ServeResult{}, err
		}
	}
	if res.Rows, err = queryRows(tx, `SELECT `+rowCols+` FROM rows WHERE ver > ? AND kind IN (?, ?) ORDER BY ver`, since, KindRunner, KindMeta); err != nil {
		return ServeResult{}, err
	}
	if res.Events, err = queryEvents(tx, `SELECT `+eventCols+` FROM events_v3 WHERE ver > ? ORDER BY ver`, since); err != nil {
		return ServeResult{}, err
	}
	if res.Version > served {
		if _, err := tx.Exec(`UPDATE store_meta SET served = ? WHERE id = 1`, res.Version); err != nil {
			return ServeResult{}, err
		}
	}
	return res, tx.Commit()
}

// Remote is one pull response to apply.
type Remote struct {
	// RequestedAs is our own store id when the request was sent. If the store
	// has wiped since, the response is discarded.
	RequestedAs string
	Addr        string
	StoreID     string
	NodeID      string
	Version     uint64
	Rows        []Row
	Events      []model.Event
	NowMs       int64
}

// ErrPeerForgotten is returned when a peer we have forgotten answers with its
// old store id instead of wiping.
var ErrPeerForgotten = errors.New("peer store was forgotten and has not wiped")

// ApplyRemote applies one pull response in a single transaction: rows by
// last-write-wins, new events, and the watermark for the peer store. Every
// row or event that actually changes this store gets one new local version, so
// it is passed on to our own peers even if its HLC is old.
func (s *Store) ApplyRemote(r Remote) (int, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	if r.RequestedAs != "" {
		self, err := storeIDTx(tx)
		if err != nil {
			return 0, err
		}
		if self != r.RequestedAs {
			// We wiped while this pull was in flight. The response was computed
			// for the old store's watermark; recording it now would skip rows
			// the wipe just dropped. The next round pulls from scratch.
			return 0, nil
		}
	}
	if r.StoreID != "" {
		var forgotten int
		err := tx.QueryRow(`SELECT forgotten FROM peers WHERE store_id = ?`, r.StoreID).Scan(&forgotten)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return 0, err
		}
		if forgotten != 0 {
			return 0, ErrPeerForgotten
		}
	}

	vr := &versioner{tx: tx}
	applied := 0
	for _, row := range r.Rows {
		var cur Row
		err := tx.QueryRow(`SELECT owner, hlc_ts, hlc_seq FROM rows WHERE kind = ? AND key = ?`, row.Kind, row.Key).
			Scan(&cur.Owner, &cur.HLC.TS, &cur.HLC.Seq)
		if err == nil {
			if !newer(row, cur) {
				continue
			}
		} else if !errors.Is(err, sql.ErrNoRows) {
			return 0, err
		}
		v, err := vr.get()
		if err != nil {
			return 0, err
		}
		if err := upsertTx(tx, row, v); err != nil {
			return 0, err
		}
		applied++
	}
	for _, e := range r.Events {
		var exists int
		err := tx.QueryRow(`SELECT 1 FROM events_v3 WHERE origin = ? AND runner_id = ? AND seq = ?`, e.Origin, e.RunnerID, e.Seq).Scan(&exists)
		if err == nil {
			continue
		}
		if !errors.Is(err, sql.ErrNoRows) {
			return 0, err
		}
		v, err := vr.get()
		if err != nil {
			return 0, err
		}
		pl, err := marshalPayload(e.Payload)
		if err != nil {
			return 0, err
		}
		if _, err := tx.Exec(`INSERT INTO events_v3 (origin, runner_id, seq, type, payload, hlc_ts, hlc_seq, ver)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, e.Origin, e.RunnerID, e.Seq, e.Type, pl, e.HLC.TS, e.HLC.Seq, v); err != nil {
			return 0, err
		}
		applied++
	}
	if r.StoreID != "" {
		if _, err := tx.Exec(`INSERT INTO peers (store_id, node_id, watermark, last_seen) VALUES (?, ?, ?, ?)
			ON CONFLICT(store_id) DO UPDATE SET
				node_id = excluded.node_id,
				watermark = MAX(watermark, excluded.watermark),
				last_seen = MAX(last_seen, excluded.last_seen)`, r.StoreID, r.NodeID, r.Version, r.NowMs); err != nil {
			return 0, err
		}
		if r.Addr != "" {
			if _, err := tx.Exec(`INSERT INTO peer_addrs (addr, store_id) VALUES (?, ?)
				ON CONFLICT(addr) DO UPDATE SET store_id = excluded.store_id`, r.Addr, r.StoreID); err != nil {
				return 0, err
			}
		}
	}
	return applied, tx.Commit()
}

// ForgetPeers forgets every peer store we have not exchanged with since
// cutoffMs. A forgotten peer no longer blocks tombstone GC, and we refuse to
// sync with it until it wipes. The caller must only do this after it has
// itself been up and syncing for the whole forget window, so that its own
// downtime never makes it forget healthy peers.
func (s *Store) ForgetPeers(cutoffMs int64) (int64, error) {
	res, err := s.db.Exec(`UPDATE peers SET forgotten = 1, ack = 0 WHERE forgotten = 0 AND last_seen < ?`, cutoffMs)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// Wipe is what a store does when a peer tells it it has been forgotten: peers
// may have dropped tombstones it never saw, so any row it holds could be one
// they deleted. It drops every row except its own writes that it has never
// sent to anyone (nobody can have superseded those), forgets everything about
// its peers, and takes a new store id so every peer pulls it from scratch.
// Events are kept: they are never deleted, so they cannot be resurrected.
func (s *Store) Wipe() (newID string, kept int64, err error) {
	tx, err := s.db.Begin()
	if err != nil {
		return "", 0, err
	}
	defer func() { _ = tx.Rollback() }()

	var served uint64
	if err := tx.QueryRow(`SELECT served FROM store_meta WHERE id = 1`).Scan(&served); err != nil {
		return "", 0, err
	}
	lo, hi := ownerRange(s.nodeID)
	own := `(owner = ? OR (owner >= ? AND owner < ?))`
	if _, err := tx.Exec(`DELETE FROM rows WHERE NOT (`+own+` AND ver > ?)`, "api/"+s.nodeID, lo, hi, served); err != nil {
		return "", 0, err
	}
	if err := tx.QueryRow(`SELECT COUNT(*) FROM rows`).Scan(&kept); err != nil {
		return "", 0, err
	}
	for _, stmt := range []string{`DELETE FROM peers`, `DELETE FROM peer_addrs`} {
		if _, err := tx.Exec(stmt); err != nil {
			return "", 0, err
		}
	}
	newID = newStoreID()
	if _, err := tx.Exec(`UPDATE store_meta SET store_id = ? WHERE id = 1`, newID); err != nil {
		return "", 0, err
	}
	return newID, kept, tx.Commit()
}

func marshalPayload(p map[string]any) (string, error) {
	b, err := json.Marshal(p)
	return string(b), err
}
