package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"

	_ "modernc.org/sqlite"

	"takl/internal/model"
)

type Kind string

const (
	KindRunner Kind = "runner"
	KindMeta   Kind = "meta"
	KindEvent  Kind = "event"
)

var Kinds = []Kind{KindRunner, KindMeta}

type Row struct {
	Kind      Kind
	Key       string
	Owner     string
	HLC       model.HLC
	Tombstone bool
	Payload   []byte
	// Version is this store's local version for the row's last change. It is
	// local bookkeeping (replication watermarks, GC acks), never replicated,
	// and differs between nodes.
	Version uint64
}

type Store struct {
	db     *sql.DB
	nodeID string
}

func Open(path, nodeID string) (*Store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	s := &Store{db: db, nodeID: nodeID}
	if err := s.init(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) NodeID() string {
	return s.nodeID
}

func (s *Store) Close() error {
	return s.db.Close()
}

const rowCols = `kind, key, owner, hlc_ts, hlc_seq, tombstone, payload, ver`

const upsertRowSQL = `INSERT INTO rows (kind, key, owner, hlc_ts, hlc_seq, tombstone, payload, ver)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(kind, key) DO UPDATE SET
		owner = excluded.owner,
		hlc_ts = excluded.hlc_ts,
		hlc_seq = excluded.hlc_seq,
		tombstone = excluded.tombstone,
		payload = excluded.payload,
		ver = excluded.ver`

func upsertTx(tx *sql.Tx, r Row, ver uint64) error {
	payload := r.Payload
	if payload == nil {
		payload = []byte{}
	}
	_, err := tx.Exec(upsertRowSQL, r.Kind, r.Key, r.Owner, r.HLC.TS, r.HLC.Seq, b2i(r.Tombstone), payload, ver)
	return err
}

// nextVersion hands out this store's next local version. Every change to the
// rows or events tables gets one, so "everything with version > w" is exactly
// what a peer that has pulled up to w is missing, whatever order rows arrived in.
func nextVersion(tx *sql.Tx) (uint64, error) {
	var v uint64
	err := tx.QueryRow(`UPDATE store_meta SET version = version + 1 WHERE id = 1 RETURNING version`).Scan(&v)
	return v, err
}

// versioner hands out at most one version per transaction, and only if the
// transaction actually changes something.
type versioner struct {
	tx *sql.Tx
	v  uint64
}

func (vr *versioner) get() (uint64, error) {
	if vr.v == 0 {
		v, err := nextVersion(vr.tx)
		if err != nil {
			return 0, err
		}
		vr.v = v
	}
	return vr.v, nil
}

// Put is a raw upsert with no conflict check. Production writers use PutLocal
// (local changes) or ApplyRemote (replicated changes); Put is for seeding.
func (s *Store) Put(r Row) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	v, err := nextVersion(tx)
	if err != nil {
		return err
	}
	if err := upsertTx(tx, r, v); err != nil {
		return err
	}
	return tx.Commit()
}

// PutLocal writes a row that originates on this node (agent ticks, label API).
//
// build gets the current row (ok is false if there is none) and returns the
// new payload and tombstone flag. The HLC is taken inside the same transaction
// that reads the current row: the row's HLC is folded into clock first, then
// clock ticks. The store has a single connection, so no ApplyRemote can commit
// between the read and the write, and the new row always supersedes the one it
// replaces, even when the sync engine has applied a remote row but not yet
// called clock.Update for it.
func (s *Store) PutLocal(kind Kind, key, owner string, clock *model.Clock, build func(cur Row, ok bool) (payload []byte, tombstone bool, err error)) (Row, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return Row{}, err
	}
	defer func() { _ = tx.Rollback() }()

	cur, err := scanRow(tx.QueryRow(`SELECT `+rowCols+` FROM rows WHERE kind = ? AND key = ?`, kind, key))
	ok := err == nil
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return Row{}, err
	}
	payload, tomb, err := build(cur, ok)
	if err != nil {
		return Row{}, err
	}
	if payload == nil {
		payload = []byte{}
	}
	if ok {
		clock.Update(cur.HLC)
	}
	v, err := nextVersion(tx)
	if err != nil {
		return Row{}, err
	}
	r := Row{Kind: kind, Key: key, Owner: owner, HLC: clock.Tick(), Tombstone: tomb, Payload: payload, Version: v}
	if err := upsertTx(tx, r, v); err != nil {
		return Row{}, err
	}
	return r, tx.Commit()
}

// Apply writes r if it wins last-write-wins against the current row.
func (s *Store) Apply(r Row) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	cur, err := scanRow(tx.QueryRow(`SELECT `+rowCols+` FROM rows WHERE kind = ? AND key = ?`, r.Kind, r.Key))
	if err == nil {
		if !newer(r, cur) {
			return nil
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	v, err := nextVersion(tx)
	if err != nil {
		return err
	}
	if err := upsertTx(tx, r, v); err != nil {
		return err
	}
	return tx.Commit()
}

func newer(a, b Row) bool {
	switch a.HLC.Compare(b.HLC) {
	case 1:
		return true
	case -1:
		return false
	}
	return a.Owner > b.Owner
}

func (s *Store) Get(kind Kind, key string) (Row, bool, error) {
	r, err := scanRow(s.db.QueryRow(`SELECT `+rowCols+` FROM rows WHERE kind = ? AND key = ?`, kind, key))
	if errors.Is(err, sql.ErrNoRows) {
		return Row{}, false, nil
	}
	if err != nil {
		return Row{}, false, err
	}
	return r, true, nil
}

var safeKey = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

func (s *Store) List(kind Kind, limit, offset int, filters map[string]any, fn func(Row) bool) error {
	query := `SELECT ` + rowCols + ` FROM rows WHERE kind = ? AND tombstone = 0`
	args := []any{kind}

	for k, v := range filters {
		if !safeKey.MatchString(k) {
			return fmt.Errorf("invalid filter key: %q", k)
		}
		query += fmt.Sprintf(" AND json_extract(payload, '$.%s') = ?", k)
		args = append(args, v)
	}
	query += " ORDER BY key"

	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
		if offset > 0 {
			query += " OFFSET ?"
			args = append(args, offset)
		}
	} else if limit == -1 && offset > 0 {
		query += " LIMIT -1 OFFSET ?"
		args = append(args, offset)
	}

	rows, err := s.db.Query(query, args...)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		r, err := scanRow(rows)
		if err != nil {
			return err
		}
		if !fn(r) {
			break
		}
	}
	return rows.Err()
}

// ChangesSince returns the rows of kind (tombstones included) whose local
// version is greater than since, oldest change first.
func (s *Store) ChangesSince(kind Kind, since uint64) ([]Row, error) {
	return queryRows(s.db, `SELECT `+rowCols+` FROM rows WHERE kind = ? AND ver > ? ORDER BY ver`, kind, since)
}

type querier interface {
	Query(query string, args ...any) (*sql.Rows, error)
}

func queryRows(q querier, query string, args ...any) ([]Row, error) {
	rows, err := q.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := []Row{}
	for rows.Next() {
		r, err := scanRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// Delete blindly writes a tombstone for (kind, key), keeping the old payload.
// Production deletes go through PutLocal; Delete is for seeding.
func (s *Store) Delete(kind Kind, key, owner string, hlc model.HLC) error {
	payload := []byte{}
	if cur, ok, err := s.Get(kind, key); err != nil {
		return err
	} else if ok {
		payload = cur.Payload
	}
	return s.Put(Row{Kind: kind, Key: key, Owner: owner, HLC: hlc, Tombstone: true, Payload: payload})
}

// ownerRange returns [lo, hi) such that lo <= owner < hi exactly when owner
// starts with nodeID + "/". Unlike LIKE, it treats '_' and '%' in node ids
// literally.
func ownerRange(nodeID string) (lo, hi string) {
	return nodeID + "/", nodeID + "0" // '0' is the byte after '/'
}

// DeleteByNode tombstones every live row owned by nodeID. Like PutLocal, it
// folds the newest of those rows' HLCs into clock before ticking, inside one
// transaction, so each tombstone supersedes the row it replaces. It returns the
// HLC it used.
func (s *Store) DeleteByNode(nodeID string, clock *model.Clock) (model.HLC, error) {
	lo, hi := ownerRange(nodeID)
	tx, err := s.db.Begin()
	if err != nil {
		return model.HLC{}, err
	}
	defer func() { _ = tx.Rollback() }()

	var newest model.HLC
	err = tx.QueryRow(`SELECT hlc_ts, hlc_seq FROM rows
		WHERE owner >= ? AND owner < ? AND tombstone = 0
		ORDER BY hlc_ts DESC, hlc_seq DESC LIMIT 1`, lo, hi).Scan(&newest.TS, &newest.Seq)
	if errors.Is(err, sql.ErrNoRows) {
		return clock.Tick(), nil
	}
	if err != nil {
		return model.HLC{}, err
	}
	clock.Update(newest)
	hlc := clock.Tick()
	v, err := nextVersion(tx)
	if err != nil {
		return model.HLC{}, err
	}
	if _, err := tx.Exec(`UPDATE rows SET
		tombstone = 1,
		hlc_ts = ?,
		hlc_seq = ?,
		ver = ?
		WHERE owner >= ? AND owner < ? AND tombstone = 0`, hlc.TS, hlc.Seq, v, lo, hi); err != nil {
		return model.HLC{}, err
	}
	return hlc, tx.Commit()
}

// GCTombstones drops tombstones that are older than minTS (wall-clock ms) AND
// that every known peer has already pulled. A tombstone a peer has not seen
// yet is kept, however old: dropping it would let that peer's older live row
// come back. Peers that have been unreachable for too long are forgotten
// (ForgetPeers) and stop blocking GC.
func (s *Store) GCTombstones(minTS int64) (int64, error) {
	res, err := s.db.Exec(`DELETE FROM rows
		WHERE tombstone = 1 AND hlc_ts < ?
		AND ver <= (SELECT COALESCE(MIN(ack), 9223372036854775807) FROM peers WHERE forgotten = 0)`, minTS)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// AppendEvent records an event emitted by this node. Events are identified by
// (origin, runner_id, seq), where origin is this store's id.
func (s *Store) AppendEvent(e model.Event) error {
	payload, err := json.Marshal(e.Payload)
	if err != nil {
		return err
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	origin, err := storeIDTx(tx)
	if err != nil {
		return err
	}
	var seq int64
	if err := tx.QueryRow(`SELECT COALESCE(MAX(seq), 0) FROM events_v3 WHERE origin = ? AND runner_id = ?`, origin, e.RunnerID).Scan(&seq); err != nil {
		return err
	}
	v, err := nextVersion(tx)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(`INSERT INTO events_v3 (origin, runner_id, seq, type, payload, hlc_ts, hlc_seq, ver)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		origin, e.RunnerID, seq+1, e.Type, string(payload), e.HLC.TS, e.HLC.Seq, v); err != nil {
		return err
	}
	return tx.Commit()
}

// Events returns the newest limit events, oldest first.
func (s *Store) Events(limit int) ([]model.Event, error) {
	if limit <= 0 {
		limit = 100
	}
	out, err := queryEvents(s.db, `SELECT `+eventCols+` FROM events_v3
		ORDER BY hlc_ts DESC, hlc_seq DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out, nil
}

const eventCols = `origin, runner_id, seq, type, payload, hlc_ts, hlc_seq`

func queryEvents(q querier, query string, args ...any) ([]model.Event, error) {
	rows, err := q.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := []model.Event{}
	for rows.Next() {
		var (
			e      model.Event
			ts, sq int64
			pl     string
		)
		if err := rows.Scan(&e.Origin, &e.RunnerID, &e.Seq, &e.Type, &pl, &ts, &sq); err != nil {
			return nil, err
		}
		e.HLC = model.HLC{TS: ts, Seq: sq}
		if pl != "" {
			if err := json.Unmarshal([]byte(pl), &e.Payload); err != nil {
				return nil, err
			}
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

func scanRow(sc interface{ Scan(...any) error }) (Row, error) {
	var (
		r      Row
		ts, sq int64
		tomb   int
	)
	if err := sc.Scan(&r.Kind, &r.Key, &r.Owner, &ts, &sq, &tomb, &r.Payload, &r.Version); err != nil {
		return Row{}, err
	}
	r.HLC = model.HLC{TS: ts, Seq: sq}
	r.Tombstone = tomb != 0
	return r, nil
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}
