// Package store is the in-memory state layer for the scaleset orchestrator.
//
// State lives in a hashicorp/go-memdb instance. There is no persistent
// backing — on startup the orchestrator's pool manager reconciles its
// empty view against Proxmox via Provisioner.ListOwnedVMs to rebuild
// reality. See the project memory note for why a persistent DB was ripped
// out in favor of this.
//
// Concurrency: go-memdb serialises write transactions internally (single
// writer at a time, snapshot reads). Every helper here that mutates state
// opens a write transaction, so the orchestrator gets atomic compare-and-set
// without an additional mutex.
package store

import (
	"errors"
	"fmt"
	"time"

	"github.com/hashicorp/go-memdb"
)

// ErrNotFound is returned by single-row lookups when the VMID is unknown.
var ErrNotFound = errors.New("store: vm not found")

// ErrAtCapacity is returned by AcquireHot when the busy-count cap would
// be exceeded by claiming another VM. Distinct from "no Hot VM
// available" — at-capacity means the pool is intentionally rejecting,
// not that it's exhausted.
var ErrAtCapacity = errors.New("store: at capacity")

// ErrNoneAvailable is returned by AcquireHot when no rows are currently
// in the Hot state. The caller should kick a refill and back off.
var ErrNoneAvailable = errors.New("store: no hot vm available")

// Store wraps a memdb.MemDB with typed helpers for the orchestrator's
// VM table.
type Store struct {
	db *memdb.MemDB
}

// New returns an empty Store ready to use.
func New() (*Store, error) {
	db, err := memdb.NewMemDB(schema())
	if err != nil {
		return nil, fmt.Errorf("store: build schema: %w", err)
	}
	return &Store{db: db}, nil
}

// ---------------------------------------------------------------------------
// Reads
// ---------------------------------------------------------------------------

// Get returns the row with the given VMID, or ErrNotFound.
func (s *Store) Get(vmid int) (*VM, error) {
	txn := s.db.Txn(false)
	defer txn.Abort()
	raw, err := txn.First(tableVM, "id", vmid)
	if err != nil {
		return nil, fmt.Errorf("store: get %d: %w", vmid, err)
	}
	if raw == nil {
		return nil, ErrNotFound
	}
	return raw.(*VM).Clone(), nil
}

// List returns every row in the store. The slice is a copy; callers may
// mutate it freely.
func (s *Store) List() ([]*VM, error) {
	txn := s.db.Txn(false)
	defer txn.Abort()
	it, err := txn.Get(tableVM, "id")
	if err != nil {
		return nil, fmt.Errorf("store: list: %w", err)
	}
	return collect(it), nil
}

// ListByState returns every row currently in the given state.
func (s *Store) ListByState(states ...State) ([]*VM, error) {
	if len(states) == 0 {
		return nil, nil
	}
	txn := s.db.Txn(false)
	defer txn.Abort()
	var out []*VM
	for _, st := range states {
		it, err := txn.Get(tableVM, "state", string(st))
		if err != nil {
			return nil, fmt.Errorf("store: list state %s: %w", st, err)
		}
		out = append(out, collect(it)...)
	}
	return out, nil
}

// ListExcludingStates returns every row whose state is NOT in the given
// set. Used by the GitHub reconciler to skip rows already on their way out.
func (s *Store) ListExcludingStates(excluded ...State) ([]*VM, error) {
	skip := make(map[State]struct{}, len(excluded))
	for _, st := range excluded {
		skip[st] = struct{}{}
	}
	all, err := s.List()
	if err != nil {
		return nil, err
	}
	out := make([]*VM, 0, len(all))
	for _, v := range all {
		if _, drop := skip[v.State]; drop {
			continue
		}
		out = append(out, v)
	}
	return out, nil
}

// Count returns the number of rows currently in the given state.
func (s *Store) Count(state State) (int, error) {
	txn := s.db.Txn(false)
	defer txn.Abort()
	it, err := txn.Get(tableVM, "state", string(state))
	if err != nil {
		return 0, fmt.Errorf("store: count %s: %w", state, err)
	}
	n := 0
	for raw := it.Next(); raw != nil; raw = it.Next() {
		n++
	}
	return n, nil
}

// CountByPoolKindState returns the number of rows in the given (pool_kind,
// state) tuple. Backed by the compound index so it's cheap.
func (s *Store) CountByPoolKindState(kind PoolKind, state State) (int, error) {
	txn := s.db.Txn(false)
	defer txn.Abort()
	it, err := txn.Get(tableVM, "pool_kind_state", string(kind), string(state))
	if err != nil {
		return 0, fmt.Errorf("store: count %s/%s: %w", kind, state, err)
	}
	n := 0
	for raw := it.Next(); raw != nil; raw = it.Next() {
		n++
	}
	return n, nil
}

// Stats returns one count per State in a single read transaction so all
// nine values share a consistent snapshot.
func (s *Store) Stats() (map[State]int, error) {
	txn := s.db.Txn(false)
	defer txn.Abort()
	out := make(map[State]int, len(AllStates))
	for _, st := range AllStates {
		it, err := txn.Get(tableVM, "state", string(st))
		if err != nil {
			return nil, fmt.Errorf("store: stats %s: %w", st, err)
		}
		n := 0
		for raw := it.Next(); raw != nil; raw = it.Next() {
			n++
		}
		out[st] = n
	}
	return out, nil
}

// UsedVMIDs returns the set of VMIDs currently in use within [minID, maxID].
// Used by the VMID allocator to pick the lowest free id.
func (s *Store) UsedVMIDs(minID, maxID int) (map[int]struct{}, error) {
	all, err := s.List()
	if err != nil {
		return nil, err
	}
	used := make(map[int]struct{}, len(all))
	for _, v := range all {
		if v.VMID >= minID && v.VMID <= maxID {
			used[v.VMID] = struct{}{}
		}
	}
	return used, nil
}

// ---------------------------------------------------------------------------
// Writes
// ---------------------------------------------------------------------------

// Insert creates a new row. Fails if the VMID is already present. Stamps
// CreatedAt / UpdatedAt / StateSince to now() if unset.
func (s *Store) Insert(v *VM) error {
	if v.VMID <= 0 {
		return fmt.Errorf("store: insert: vmid must be positive (got %d)", v.VMID)
	}
	txn := s.db.Txn(true)
	defer txn.Abort()
	existing, err := txn.First(tableVM, "id", v.VMID)
	if err != nil {
		return fmt.Errorf("store: insert: lookup %d: %w", v.VMID, err)
	}
	if existing != nil {
		return fmt.Errorf("store: insert: vmid %d already exists", v.VMID)
	}
	now := time.Now()
	row := v.Clone()
	if row.CreatedAt.IsZero() {
		row.CreatedAt = now
	}
	if row.UpdatedAt.IsZero() {
		row.UpdatedAt = now
	}
	if row.StateSince.IsZero() {
		row.StateSince = now
	}
	if err := txn.Insert(tableVM, row); err != nil {
		return fmt.Errorf("store: insert %d: %w", v.VMID, err)
	}
	txn.Commit()
	return nil
}

// Delete removes the row with the given VMID. A missing row is not an
// error — callers (e.g. the destroy path) treat double-delete as a no-op.
func (s *Store) Delete(vmid int) error {
	txn := s.db.Txn(true)
	defer txn.Abort()
	existing, err := txn.First(tableVM, "id", vmid)
	if err != nil {
		return fmt.Errorf("store: delete: lookup %d: %w", vmid, err)
	}
	if existing == nil {
		return nil
	}
	if err := txn.Delete(tableVM, existing); err != nil {
		return fmt.Errorf("store: delete %d: %w", vmid, err)
	}
	txn.Commit()
	return nil
}

// Update applies mutate to a copy of the row and persists it. UpdatedAt is
// stamped automatically. Returns ErrNotFound if vmid is unknown. The
// caller's mutate function must NOT change VMID — that field is the
// primary key.
func (s *Store) Update(vmid int, mutate func(*VM)) (*VM, error) {
	txn := s.db.Txn(true)
	defer txn.Abort()
	row, err := updateInTxn(txn, vmid, mutate)
	if err != nil {
		return nil, err
	}
	txn.Commit()
	return row.Clone(), nil
}

// UpdateState atomically transitions a row from `from` to `to`. Returns
// (true, nil) if the transition was applied, (false, nil) if the current
// state doesn't match `from` (the typical CAS-lost outcome), or
// (false, err) on lookup/index failure. mutate may be nil; when non-nil
// it runs after the state is set but before commit, so callers can stamp
// related fields (JobID, RunnerID, ...) in the same atomic write.
//
// StateSince is stamped automatically on every successful transition.
func (s *Store) UpdateState(vmid int, from, to State, mutate func(*VM)) (bool, error) {
	txn := s.db.Txn(true)
	defer txn.Abort()
	raw, err := txn.First(tableVM, "id", vmid)
	if err != nil {
		return false, fmt.Errorf("store: cas: lookup %d: %w", vmid, err)
	}
	if raw == nil {
		return false, nil
	}
	cur := raw.(*VM)
	if cur.State != from {
		return false, nil
	}
	now := time.Now()
	cp := cur.Clone()
	cp.State = to
	cp.StateSince = now
	cp.UpdatedAt = now
	if mutate != nil {
		mutate(cp)
	}
	if err := txn.Insert(tableVM, cp); err != nil {
		return false, fmt.Errorf("store: cas: write %d: %w", vmid, err)
	}
	txn.Commit()
	return true, nil
}

// AcquireHot atomically claims the oldest Hot VM by transitioning it to
// Assigned, but only if total busy (Assigned + Running) is strictly less
// than maxConcurrent. Returns ErrAtCapacity if the cap would be exceeded
// or ErrNoneAvailable if no Hot rows exist.
//
// The cap check and the CAS happen inside the same write transaction so
// concurrent Acquire callers cannot all see "busy < cap" against the
// same snapshot and then each claim a different VM — the canonical
// over-provisioning bug fixed by this design.
//
// Oldest-Hot-first selection is preserved (closest to vm_max_age recycle
// goes first).
func (s *Store) AcquireHot(jobID int64, maxConcurrent int) (*VM, error) {
	txn := s.db.Txn(true)
	defer txn.Abort()

	// Count busy inside the txn for snapshot consistency. We can't use
	// the read-only Stats helper here — it opens its own transaction.
	busy := 0
	for _, st := range []State{StateAssigned, StateRunning} {
		it, err := txn.Get(tableVM, "state", string(st))
		if err != nil {
			return nil, fmt.Errorf("store: acquire: count %s: %w", st, err)
		}
		for raw := it.Next(); raw != nil; raw = it.Next() {
			busy++
		}
	}
	if busy >= maxConcurrent {
		return nil, ErrAtCapacity
	}

	// Pick the oldest Hot row by CreatedAt — same policy the manager
	// applied previously, just inside the same txn now.
	it, err := txn.Get(tableVM, "state", string(StateHot))
	if err != nil {
		return nil, fmt.Errorf("store: acquire: list hot: %w", err)
	}
	var oldest *VM
	for raw := it.Next(); raw != nil; raw = it.Next() {
		cand := raw.(*VM)
		if oldest == nil || cand.CreatedAt.Before(oldest.CreatedAt) {
			oldest = cand
		}
	}
	if oldest == nil {
		return nil, ErrNoneAvailable
	}

	now := time.Now()
	cp := oldest.Clone()
	cp.State = StateAssigned
	cp.JobID = jobID
	cp.StateSince = now
	cp.UpdatedAt = now
	if err := txn.Insert(tableVM, cp); err != nil {
		return nil, fmt.Errorf("store: acquire: write %d: %w", cp.VMID, err)
	}
	txn.Commit()
	return cp.Clone(), nil
}

// UpdateStateIn is the multi-state variant of UpdateState: the transition
// is applied when the current state is one of `from`. Used by
// PromoteToRunning which accepts both Assigned→Running and Hot→Running.
func (s *Store) UpdateStateIn(vmid int, from []State, to State, mutate func(*VM)) (bool, error) {
	allowed := make(map[State]struct{}, len(from))
	for _, st := range from {
		allowed[st] = struct{}{}
	}
	txn := s.db.Txn(true)
	defer txn.Abort()
	raw, err := txn.First(tableVM, "id", vmid)
	if err != nil {
		return false, fmt.Errorf("store: cas: lookup %d: %w", vmid, err)
	}
	if raw == nil {
		return false, nil
	}
	cur := raw.(*VM)
	if _, ok := allowed[cur.State]; !ok {
		return false, nil
	}
	now := time.Now()
	cp := cur.Clone()
	cp.State = to
	cp.StateSince = now
	cp.UpdatedAt = now
	if mutate != nil {
		mutate(cp)
	}
	if err := txn.Insert(tableVM, cp); err != nil {
		return false, fmt.Errorf("store: cas: write %d: %w", vmid, err)
	}
	txn.Commit()
	return true, nil
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// updateInTxn does the lookup-mutate-write for Update, sharing logic with
// any future helper that needs an unconditional update inside an existing
// transaction.
func updateInTxn(txn *memdb.Txn, vmid int, mutate func(*VM)) (*VM, error) {
	raw, err := txn.First(tableVM, "id", vmid)
	if err != nil {
		return nil, fmt.Errorf("store: update: lookup %d: %w", vmid, err)
	}
	if raw == nil {
		return nil, ErrNotFound
	}
	cur := raw.(*VM).Clone()
	if mutate != nil {
		mutate(cur)
	}
	cur.UpdatedAt = time.Now()
	if err := txn.Insert(tableVM, cur); err != nil {
		return nil, fmt.Errorf("store: update %d: %w", vmid, err)
	}
	return cur, nil
}

// collect drains an iterator into a slice of cloned VMs. Cloning insulates
// callers from accidentally mutating the indexed copy.
func collect(it memdb.ResultIterator) []*VM {
	var out []*VM
	for raw := it.Next(); raw != nil; raw = it.Next() {
		out = append(out, raw.(*VM).Clone())
	}
	return out
}
