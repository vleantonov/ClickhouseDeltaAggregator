package keeper

import (
	offsetmanager "delta_aggregator/internal/offset_manager"
	"encoding/binary"
	"errors"
	"fmt"
	"path"
	"strconv"

	"github.com/samuel/go-zookeeper/zk"
)

// znode payload layout (current, 25 bytes):
//
//	[0:8]   CompletedUpTo   uint64 BE — exclusive watermark; offsets [0,CompletedUpTo) durable
//	[8:16]  InProgress.Min  uint64 BE
//	[16:24] InProgress.Max  uint64 BE
//	[24]    flags           bit0 = HasInProgress
//
// The legacy layout (17 bytes) is still decoded for backward compatibility:
//
//	[0:8]  MinOffset uint64 BE
//	[8:16] MaxOffset uint64 BE
//	[16]   State     (UNKNOWN|IN_PROGRESS|COMPLETED)
const (
	recordSize       = 25
	legacyRecordSize = 17

	flagHasInProgress = 0x01

	// maxCASRetries bounds the read-modify-write loop in StoreRangeOffsetState; a
	// znode is only contended by the (single) lock holder plus stragglers, so a
	// handful of retries is plenty.
	maxCASRetries = 8
)

type KeeperOffsetManager struct {
	conn *zk.Conn
	root string
}

func NewKeeperOffsetManager(conn *zk.Conn, rootPath string) (*KeeperOffsetManager, error) {
	manager := &KeeperOffsetManager{conn: conn, root: rootPath}

	exists, _, err := conn.Exists(rootPath)
	if err != nil {
		return nil, err
	}
	if !exists {
		_, err := conn.Create(rootPath, []byte{}, 0, zk.WorldACL(zk.PermAll))
		if err != nil && err != zk.ErrNodeExists {
			return nil, err
		}
	}
	return manager, nil
}

func (m *KeeperOffsetManager) getPartitionPath(partition int64) string {
	return path.Join(m.root, strconv.Itoa(int(partition)))
}

// record is the durable per-partition state. It separates the contiguous COMPLETED
// prefix (CompletedUpTo) from the single range currently being attempted (InProgress).
type record struct {
	CompletedUpTo uint64
	InProgress    offsetmanager.RangeOffset
	HasInProgress bool
}

func encodeRecord(r record) []byte {
	data := make([]byte, recordSize)
	binary.BigEndian.PutUint64(data[0:8], r.CompletedUpTo)
	binary.BigEndian.PutUint64(data[8:16], r.InProgress.MinOffset)
	binary.BigEndian.PutUint64(data[16:24], r.InProgress.MaxOffset)
	if r.HasInProgress {
		data[24] = flagHasInProgress
	}
	return data
}

func decodeRecord(data []byte) (record, error) {
	switch len(data) {
	case recordSize:
		return record{
			CompletedUpTo: binary.BigEndian.Uint64(data[0:8]),
			InProgress: offsetmanager.RangeOffset{
				MinOffset: binary.BigEndian.Uint64(data[8:16]),
				MaxOffset: binary.BigEndian.Uint64(data[16:24]),
			},
			HasInProgress: data[24]&flagHasInProgress != 0,
		}, nil
	case legacyRecordSize:
		// Migrate the old single-triple layout into the watermark model.
		min := binary.BigEndian.Uint64(data[0:8])
		max := binary.BigEndian.Uint64(data[8:16])
		switch offsetmanager.OffsetState(data[16]) {
		case offsetmanager.COMPLETED:
			return record{CompletedUpTo: max + 1}, nil
		case offsetmanager.IN_PROGRESS:
			return record{
				CompletedUpTo: min,
				InProgress:    offsetmanager.RangeOffset{MinOffset: min, MaxOffset: max},
				HasInProgress: true,
			}, nil
		default:
			return record{}, nil
		}
	default:
		return record{}, fmt.Errorf("invalid offset data length: %d", len(data))
	}
}

// toState projects the durable record into the RangeOffsetState the reader consumes.
// The reader routes messages by {MinOffset,MaxOffset,State}, so we surface the active
// in-progress range when one exists, otherwise the completed prefix as a COMPLETED
// range. CompletedUpTo is always carried through for the start-offset decision.
func (r record) toState() offsetmanager.RangeOffsetState {
	if r.HasInProgress {
		return offsetmanager.RangeOffsetState{
			RangeOffset:   r.InProgress,
			State:         offsetmanager.IN_PROGRESS,
			CompletedUpTo: r.CompletedUpTo,
		}
	}
	if r.CompletedUpTo > 0 {
		return offsetmanager.RangeOffsetState{
			RangeOffset:   offsetmanager.RangeOffset{MinOffset: 0, MaxOffset: r.CompletedUpTo - 1},
			State:         offsetmanager.COMPLETED,
			CompletedUpTo: r.CompletedUpTo,
		}
	}
	return offsetmanager.RangeOffsetState{State: offsetmanager.UNKNOWN}
}

// errGapBelowWatermark is returned when a COMPLETED range starts above the durable
// watermark, i.e. a gap of un-written offsets sits below it. Advancing the watermark
// would skip that gap, so we refuse the write and let the caller stall/retry — the
// range is never YDB-committed, so recovery re-reads from the watermark and fills the
// gap. This should not occur in normal flow (the reader always resumes from the
// watermark); it is a guard against a corrupted/raced state.
var errGapBelowWatermark = errors.New("completed range starts above watermark: gap below would be skipped")

// StoreRangeOffsetState applies an incoming range transition under a compare-and-set
// loop so a stale writer (e.g. an aggregator that paused mid-handoff) can never clobber
// a newer record. The watermark CompletedUpTo only ever advances, and only by contiguous
// extension, so a lost or out-of-order write degrades to a stall — never silent loss.
//
// Transitions (Min is always compared against the durable watermark CompletedUpTo):
//   - IN_PROGRESS, Min == watermark → record the attempt extending the prefix.
//   - IN_PROGRESS, Min <  watermark → stale (a racing/restarted writer re-reading an
//     already-durable range); idempotent no-op. Recording it would make GetRangeOffsetState
//     report a resume point BELOW the watermark, which YDB rejects (read_offset < committed).
//   - IN_PROGRESS, Min >  watermark → refuse (errGapBelowWatermark): a gap sits below it.
//   - COMPLETED, Min == watermark  → advance watermark to Max+1, clear in-progress (happy path).
//   - COMPLETED, Min <  watermark  → stale/duplicate; idempotent no-op (never regress).
//   - COMPLETED, Min >  watermark  → refuse (errGapBelowWatermark): committing would skip
//     the gap below. The caller must not advance, so recovery re-fills from the watermark.
func (m KeeperOffsetManager) StoreRangeOffsetState(partition int64, incoming offsetmanager.RangeOffsetState) error {
	partPath := m.getPartitionPath(partition)

	for range maxCASRetries {
		cur, stat, exists, err := m.load(partPath)
		if err != nil {
			return err
		}

		next, write, err := applyTransition(cur, incoming)
		if err != nil {
			return err
		}
		if !write {
			return nil // idempotent no-op
		}
		data := encodeRecord(next)

		if !exists {
			_, err = m.conn.Create(partPath, data, 0, zk.WorldACL(zk.PermAll))
			if errors.Is(err, zk.ErrNodeExists) {
				continue // someone created it first; re-read and merge
			}
			return err
		}

		_, err = m.conn.Set(partPath, data, stat.Version)
		if errors.Is(err, zk.ErrBadVersion) {
			continue // lost the race; re-read and merge
		}
		return err
	}
	return fmt.Errorf("store range state for partition %d: exhausted %d CAS attempts", partition, maxCASRetries)
}

// applyTransition computes the next durable record from the current one and an incoming
// transition. write=false signals a stale idempotent no-op; a non-nil error signals an
// unsafe transition the caller must refuse.
func applyTransition(cur record, incoming offsetmanager.RangeOffsetState) (next record, write bool, err error) {
	next = cur
	switch incoming.State {
	case offsetmanager.IN_PROGRESS:
		switch {
		case incoming.MinOffset == cur.CompletedUpTo:
			// Extends the durable prefix: the only in-progress range worth recording.
			next.InProgress = incoming.RangeOffset
			next.HasInProgress = true
			return next, true, nil
		case incoming.MinOffset < cur.CompletedUpTo:
			// Below the watermark: a stale re-read by a racing/restarted writer. Ignoring
			// it keeps the reported resume point at the watermark (never below committed).
			return cur, false, nil
		default:
			// A gap sits below this range; advancing to it would skip un-written offsets.
			return cur, false, fmt.Errorf("%w: in-progress range [%d,%d] watermark %d",
				errGapBelowWatermark, incoming.MinOffset, incoming.MaxOffset, cur.CompletedUpTo)
		}

	case offsetmanager.COMPLETED:
		switch {
		case incoming.MinOffset == cur.CompletedUpTo:
			// Contiguous extension of the durable prefix.
			next.CompletedUpTo = incoming.MaxOffset + 1
			next.HasInProgress = false
			next.InProgress = offsetmanager.RangeOffset{}
			return next, true, nil
		case incoming.MinOffset < cur.CompletedUpTo:
			// Already covered by the watermark: stale/duplicate completion. Never regress.
			return cur, false, nil
		default:
			// A gap sits between the watermark and this range; advancing would skip it.
			return cur, false, fmt.Errorf("%w: range [%d,%d] watermark %d",
				errGapBelowWatermark, incoming.MinOffset, incoming.MaxOffset, cur.CompletedUpTo)
		}

	default:
		// UNKNOWN incoming carries no transition.
		return cur, false, nil
	}
}

func (m KeeperOffsetManager) load(partPath string) (record, *zk.Stat, bool, error) {
	data, stat, err := m.conn.Get(partPath)
	if err != nil {
		if errors.Is(err, zk.ErrNoNode) {
			return record{}, nil, false, nil
		}
		return record{}, nil, false, err
	}
	rec, err := decodeRecord(data)
	if err != nil {
		return record{}, nil, false, err
	}
	return rec, stat, true, nil
}

func (m *KeeperOffsetManager) GetRangeOffsetState(partition int64) (
	offsetmanager.RangeOffsetState,
	error,
) {
	rec, _, exists, err := m.load(m.getPartitionPath(partition))
	if err != nil {
		return offsetmanager.RangeOffsetState{}, err
	}
	if !exists {
		return offsetmanager.RangeOffsetState{}, nil // no offset committed yet
	}
	return rec.toState(), nil
}
