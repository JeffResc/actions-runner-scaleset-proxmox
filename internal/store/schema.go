package store

import (
	"time"

	"github.com/hashicorp/go-memdb"
)

// State is the lifecycle state of a managed VM. The orchestrator is the
// sole writer; Proxmox tags are not mirrored.
type State string

const (
	StateProvisioning State = "provisioning"
	StateWarm         State = "warm"
	StateBooting      State = "booting"
	StateHot          State = "hot"
	StateAssigned     State = "assigned"
	StateRunning      State = "running"
	StateDraining     State = "draining"
	StateDestroying   State = "destroying"
	StatePoison       State = "poison"
)

// PoolKind is the pool budget a VM counts toward.
type PoolKind string

const (
	PoolKindHot  PoolKind = "hot"
	PoolKindWarm PoolKind = "warm"
)

// AllStates is the fixed enumeration of lifecycle states, used by Stats
// and the metrics labels.
var AllStates = []State{
	StateProvisioning,
	StateWarm,
	StateBooting,
	StateHot,
	StateAssigned,
	StateRunning,
	StateDraining,
	StateDestroying,
	StatePoison,
}

// VM is a single Proxmox virtual machine the scaleset is managing.
//
// JobID and RunnerID are int64 with 0 meaning "unset" rather than *int64.
// VMIDs and GitHub IDs are positive integers, so the sentinel is unambiguous
// and the indexer doesn't have to deal with nilable pointers.
type VM struct {
	VMID         int
	Node         string
	Name         string
	PoolKind     PoolKind
	State        State
	JobID        int64
	RunnerID     int64
	BootAttempts int
	CreatedAt    time.Time
	UpdatedAt    time.Time
	StateSince   time.Time
}

// Clone returns a deep copy. memdb stores by pointer; mutating a row read
// from a read txn would corrupt the index, so writers always work on a
// Clone and Insert it back.
func (v *VM) Clone() *VM {
	cp := *v
	return &cp
}

// tableVM is the memdb table name.
const tableVM = "vm"

// schema defines the memdb tables and indexes.
//
// Indexes:
//   - "id" (unique, primary) on VMID — every lookup-by-id and the unique
//     constraint that prevents two clones from racing on the same VMID.
//   - "state" on State — drives ListByState, Count, Stats, and the
//     stuck-state / max-age sweeps.
//   - "pool_kind_state" compound on (PoolKind, State) — drives the
//     countByPoolKind helper the reconciler uses to compute hot/warm
//     provisioning headroom.
//
// Other fields (JobID, RunnerID, timestamps) aren't indexed — the table
// is bounded by max_concurrent_runners (tens to low hundreds of rows) so
// filtering with a scan is cheaper than maintaining extra indexes.
func schema() *memdb.DBSchema {
	return &memdb.DBSchema{
		Tables: map[string]*memdb.TableSchema{
			tableVM: {
				Name: tableVM,
				Indexes: map[string]*memdb.IndexSchema{
					"id": {
						Name:    "id",
						Unique:  true,
						Indexer: &memdb.IntFieldIndex{Field: "VMID"},
					},
					"state": {
						Name:    "state",
						Indexer: &memdb.StringFieldIndex{Field: "State"},
					},
					"pool_kind_state": {
						Name: "pool_kind_state",
						Indexer: &memdb.CompoundIndex{
							Indexes: []memdb.Indexer{
								&memdb.StringFieldIndex{Field: "PoolKind"},
								&memdb.StringFieldIndex{Field: "State"},
							},
						},
					},
				},
			},
		},
	}
}
