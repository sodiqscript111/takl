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
	KindRunner    Kind = "runner"
	KindBuild     Kind = "build"
	KindQueue     Kind = "queue"
	KindContainer Kind = "container"
	KindMount     Kind = "mount"
	KindEvent     Kind = "event"
	KindImage     Kind = "image"
)

var Kinds = []Kind{KindRunner, KindBuild, KindQueue, KindContainer, KindMount, KindImage}

type Row struct {
	Kind      Kind
	Key       string
	Owner     string
	HLC       model.HLC
	Tombstone bool
	Payload   []byte
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

func (s *Store) init() error {
	for _, stmt := range []string{
		`PRAGMA journal_mode=WAL`,
		`PRAGMA busy_timeout=5000`,
		`CREATE TABLE IF NOT EXISTS rows (
			kind TEXT NOT NULL,
			key TEXT NOT NULL,
			owner TEXT NOT NULL,
			hlc_ts INTEGER NOT NULL,
			hlc_seq INTEGER NOT NULL,
			tombstone INTEGER NOT NULL DEFAULT 0,
			payload BLOB NOT NULL,
			PRIMARY KEY (kind, key)
		)`,
		`CREATE INDEX IF NOT EXISTS rows_hlc ON rows (kind, hlc_ts, hlc_seq)`,
		`CREATE TABLE IF NOT EXISTS events_v2 (
			runner_id TEXT NOT NULL,
			seq INTEGER NOT NULL,
			type TEXT NOT NULL,
			payload TEXT NOT NULL,
			hlc_ts INTEGER NOT NULL,
			hlc_seq INTEGER NOT NULL,
			PRIMARY KEY (runner_id, seq)
		)`,
		`CREATE TABLE IF NOT EXISTS watermarks (
			peer TEXT NOT NULL,
			kind TEXT NOT NULL,
			hlc_ts INTEGER NOT NULL,
			hlc_seq INTEGER NOT NULL,
			PRIMARY KEY (peer, kind)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_events_ts ON events_v2(hlc_ts, hlc_seq)`,
		`CREATE INDEX IF NOT EXISTS idx_rows_owner ON rows(owner)`,
	} {
		if _, err := s.db.Exec(stmt); err != nil {
			return fmt.Errorf("schema: %w", err)
		}
	}
	return nil
}

func (s *Store) NodeID() string {
	return s.nodeID
}

func (s *Store) ApplyRemote(peer string, rows []Row) error {
	if len(rows) == 0 {
		return nil
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	maxByKind := map[Kind]model.HLC{}
	for _, r := range rows {
		var curOwner string
		var cts, cseq int64
		err := tx.QueryRow(`SELECT owner, hlc_ts, hlc_seq FROM rows WHERE kind = ? AND key = ?`, r.Kind, r.Key).Scan(&curOwner, &cts, &cseq)
		if err == nil {
			cur := Row{Kind: r.Kind, Key: r.Key, Owner: curOwner, HLC: model.HLC{TS: cts, Seq: cseq}}
			if !newer(r, cur) {
				maxByKind[r.Kind] = maxHLC(maxByKind[r.Kind], r.HLC)
				continue
			}
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if _, err := tx.Exec(upsertRowSQL, r.Kind, r.Key, r.Owner, r.HLC.TS, r.HLC.Seq, b2i(r.Tombstone), r.Payload); err != nil {
			return err
		}
		maxByKind[r.Kind] = maxHLC(maxByKind[r.Kind], r.HLC)
	}
	for kind, hlc := range maxByKind {
		var cts, cseq int64
		err := tx.QueryRow(`SELECT hlc_ts, hlc_seq FROM watermarks WHERE peer = ? AND kind = ?`, peer, kind).Scan(&cts, &cseq)
		if err == nil {
			if !hlc.After(model.HLC{TS: cts, Seq: cseq}) {
				continue
			}
		} else if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO watermarks (peer, kind, hlc_ts, hlc_seq)
			VALUES (?, ?, ?, ?)
			ON CONFLICT(peer, kind) DO UPDATE SET
				hlc_ts = excluded.hlc_ts,
				hlc_seq = excluded.hlc_seq`, peer, kind, hlc.TS, hlc.Seq); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func maxHLC(a, b model.HLC) model.HLC {
	if b.After(a) {
		return b
	}
	return a
}

const upsertRowSQL = `INSERT INTO rows (kind, key, owner, hlc_ts, hlc_seq, tombstone, payload)
	VALUES (?, ?, ?, ?, ?, ?, ?)
	ON CONFLICT(kind, key) DO UPDATE SET
		owner = excluded.owner,
		hlc_ts = excluded.hlc_ts,
		hlc_seq = excluded.hlc_seq,
		tombstone = excluded.tombstone,
		payload = excluded.payload`

func (s *Store) Put(r Row) error {
	return s.upsert(r)
}

func (s *Store) Apply(r Row) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	row := tx.QueryRow(`SELECT kind, key, owner, hlc_ts, hlc_seq, tombstone, payload FROM rows WHERE kind = ? AND key = ?`, r.Kind, r.Key)
	cur, err := scanRow(row)
	if err == nil {
		if !newer(r, cur) {
			return nil
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}

	_, err = tx.Exec(upsertRowSQL, r.Kind, r.Key, r.Owner, r.HLC.TS, r.HLC.Seq, b2i(r.Tombstone), r.Payload)
	if err != nil {
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

func (s *Store) upsert(r Row) error {
	_, err := s.db.Exec(upsertRowSQL, r.Kind, r.Key, r.Owner, r.HLC.TS, r.HLC.Seq, b2i(r.Tombstone), r.Payload)
	return err
}

func (s *Store) Get(kind Kind, key string) (Row, bool, error) {
	return s.get(kind, key)
}

func (s *Store) get(kind Kind, key string) (Row, bool, error) {
	row := s.db.QueryRow(`SELECT kind, key, owner, hlc_ts, hlc_seq, tombstone, payload
		FROM rows WHERE kind = ? AND key = ?`, kind, key)
	r, err := scanRow(row)
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
	query := `SELECT kind, key, owner, hlc_ts, hlc_seq, tombstone, payload FROM rows WHERE kind = ? AND tombstone = 0`
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

func (s *Store) ChangesSince(kind Kind, wm model.HLC) ([]Row, error) {
	rows, err := s.db.Query(`SELECT kind, key, owner, hlc_ts, hlc_seq, tombstone, payload
		FROM rows WHERE kind = ? AND (hlc_ts > ? OR (hlc_ts = ? AND hlc_seq > ?))
		ORDER BY hlc_ts, hlc_seq`, kind, wm.TS, wm.TS, wm.Seq)
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

func (s *Store) Delete(kind Kind, key, owner string, hlc model.HLC) error {
	payload := []byte{}
	if cur, ok, err := s.get(kind, key); err != nil {
		return err
	} else if ok {
		payload = cur.Payload
	}
	return s.upsert(Row{Kind: kind, Key: key, Owner: owner, HLC: hlc, Tombstone: true, Payload: payload})
}

func (s *Store) DeleteByNode(nodeID string, hlc model.HLC) error {
	prefix := nodeID + "/%"
	_, err := s.db.Exec(`UPDATE rows SET 
		tombstone = 1, 
		hlc_ts = ?, 
		hlc_seq = ? 
		WHERE owner LIKE ? AND tombstone = 0`, hlc.TS, hlc.Seq, prefix)
	return err
}

func (s *Store) GCTombstones(minHLC model.HLC) (int64, error) {
	res, err := s.db.Exec(`DELETE FROM rows WHERE tombstone = 1 AND (hlc_ts < ? OR (hlc_ts = ? AND hlc_seq < ?))`, minHLC.TS, minHLC.TS, minHLC.Seq)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

func (s *Store) Checksum(kind Kind) (uint64, error) {
	rows, err := s.db.Query(`SELECT key, hlc_ts, hlc_seq FROM rows WHERE kind = ? AND tombstone = 0`, kind)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var hash uint64
	for rows.Next() {
		var key string
		var ts, seq int64
		if err := rows.Scan(&key, &ts, &seq); err != nil {
			return 0, err
		}
		var kHash uint64 = 14695981039346656037
		for i := 0; i < len(key); i++ {
			kHash ^= uint64(key[i])
			kHash *= 1099511628211
		}
		hash ^= kHash ^ uint64(ts) ^ uint64(seq)
	}
	return hash, rows.Err()
}

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

	var seq int64
	err = tx.QueryRow(`SELECT COALESCE(MAX(seq), 0) FROM events_v2 WHERE runner_id = ?`, e.RunnerID).Scan(&seq)
	if err != nil {
		return err
	}
	seq++

	_, err = tx.Exec(`INSERT INTO events_v2 (runner_id, seq, type, payload, hlc_ts, hlc_seq)
		VALUES (?, ?, ?, ?, ?, ?)`,
		e.RunnerID, seq, e.Type, string(payload), e.HLC.TS, e.HLC.Seq)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) Events(limit int) ([]model.Event, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.Query(`SELECT seq, type, runner_id, payload, hlc_ts, hlc_seq
		FROM events_v2 ORDER BY hlc_ts DESC, hlc_seq DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := []model.Event{}
	for rows.Next() {
		var (
			e      model.Event
			seq    int64
			ts, sq int64
			pl     string
		)
		if err := rows.Scan(&seq, &e.Type, &e.RunnerID, &pl, &ts, &sq); err != nil {
			return nil, err
		}
		e.Seq = seq
		e.HLC = model.HLC{TS: ts, Seq: sq}
		if pl != "" {
			if err := json.Unmarshal([]byte(pl), &e.Payload); err != nil {
				return nil, err
			}
		}
		out = append(out, e)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out, nil
}

func (s *Store) EventsSince(wm model.HLC) ([]model.Event, error) {
	rows, err := s.db.Query(`SELECT runner_id, seq, type, payload, hlc_ts, hlc_seq
		FROM events_v2 WHERE hlc_ts > ? OR (hlc_ts = ? AND hlc_seq > ?)
		ORDER BY hlc_ts, hlc_seq`, wm.TS, wm.TS, wm.Seq)
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
		if err := rows.Scan(&e.RunnerID, &e.Seq, &e.Type, &pl, &ts, &sq); err != nil {
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

func (s *Store) ChecksumEvents() (uint64, error) {
	rows, err := s.db.Query(`SELECT runner_id, seq, hlc_ts, hlc_seq FROM events_v2`)
	if err != nil {
		return 0, err
	}
	defer func() { _ = rows.Close() }()
	var hash uint64
	for rows.Next() {
		var runnerID string
		var seq, ts, sq int64
		if err := rows.Scan(&runnerID, &seq, &ts, &sq); err != nil {
			return 0, err
		}
		var kHash uint64 = 14695981039346656037
		key := fmt.Sprintf("%s/%d", runnerID, seq)
		for i := 0; i < len(key); i++ {
			kHash ^= uint64(key[i])
			kHash *= 1099511628211
		}
		hash ^= kHash ^ uint64(ts) ^ uint64(sq)
	}
	return hash, rows.Err()
}

func (s *Store) ApplyEvents(events []model.Event) error {
	if len(events) == 0 {
		return nil
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.Prepare(`INSERT INTO events_v2 (runner_id, seq, type, payload, hlc_ts, hlc_seq)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(runner_id, seq) DO NOTHING`)
	if err != nil {
		return err
	}
	defer func() { _ = stmt.Close() }()

	for _, e := range events {
		pl, err := json.Marshal(e.Payload)
		if err != nil {
			return err
		}
		if _, err := stmt.Exec(e.RunnerID, e.Seq, e.Type, string(pl), e.HLC.TS, e.HLC.Seq); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) SetWatermark(peer string, kind Kind, hlc model.HLC) error {
	cur, err := s.Watermark(peer, kind)
	if err != nil {
		return err
	}
	if !hlc.After(cur) {
		return nil
	}
	_, err = s.db.Exec(`INSERT INTO watermarks (peer, kind, hlc_ts, hlc_seq)
		VALUES (?, ?, ?, ?)
		ON CONFLICT(peer, kind) DO UPDATE SET
			hlc_ts = excluded.hlc_ts,
			hlc_seq = excluded.hlc_seq`, peer, kind, hlc.TS, hlc.Seq)
	return err
}

func (s *Store) Watermark(peer string, kind Kind) (model.HLC, error) {
	var ts, seq int64
	err := s.db.QueryRow(`SELECT hlc_ts, hlc_seq FROM watermarks WHERE peer = ? AND kind = ?`, peer, kind).Scan(&ts, &seq)
	if errors.Is(err, sql.ErrNoRows) {
		return model.HLC{}, nil
	}
	if err != nil {
		return model.HLC{}, err
	}
	return model.HLC{TS: ts, Seq: seq}, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func scanRow(sc interface{ Scan(...any) error }) (Row, error) {
	var (
		r      Row
		ts, sq int64
		tomb   int
	)
	if err := sc.Scan(&r.Kind, &r.Key, &r.Owner, &ts, &sq, &tomb, &r.Payload); err != nil {
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
