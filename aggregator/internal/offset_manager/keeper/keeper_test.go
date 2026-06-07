package keeper

import (
	offsetmanager "delta_aggregator/internal/offset_manager"
	"encoding/binary"
	"errors"
	"math/rand"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/samuel/go-zookeeper/zk"
	"github.com/stretchr/testify/require"
)

// ---- pure logic: encode/decode round-trip + legacy migration --------------

func TestEncodeDecodeRoundTrip(t *testing.T) {
	in := record{
		CompletedUpTo: 12000,
		InProgress:    offsetmanager.RangeOffset{MinOffset: 12000, MaxOffset: 12999},
		HasInProgress: true,
	}
	out, err := decodeRecord(encodeRecord(in))
	require.NoError(t, err)
	require.Equal(t, in, out)

	// No-in-progress record.
	in2 := record{CompletedUpTo: 5000}
	out2, err := decodeRecord(encodeRecord(in2))
	require.NoError(t, err)
	require.Equal(t, in2, out2)
}

func TestDecodeLegacy17Byte(t *testing.T) {
	legacy := func(min, max uint64, state offsetmanager.OffsetState) []byte {
		b := make([]byte, legacyRecordSize)
		binary.BigEndian.PutUint64(b[0:8], min)
		binary.BigEndian.PutUint64(b[8:16], max)
		b[16] = byte(state)
		return b
	}

	// COMPLETED legacy → watermark at Max+1, no in-progress.
	rec, err := decodeRecord(legacy(0, 9999, offsetmanager.COMPLETED))
	require.NoError(t, err)
	require.Equal(t, record{CompletedUpTo: 10000}, rec)

	// IN_PROGRESS legacy → in-progress range, watermark at Min.
	rec, err = decodeRecord(legacy(10000, 10999, offsetmanager.IN_PROGRESS))
	require.NoError(t, err)
	require.Equal(t, record{
		CompletedUpTo: 10000,
		InProgress:    offsetmanager.RangeOffset{MinOffset: 10000, MaxOffset: 10999},
		HasInProgress: true,
	}, rec)

	// UNKNOWN legacy → empty.
	rec, err = decodeRecord(legacy(0, 0, offsetmanager.UNKNOWN))
	require.NoError(t, err)
	require.Equal(t, record{}, rec)

	_, err = decodeRecord(make([]byte, 9))
	require.Error(t, err)
}

func TestToState(t *testing.T) {
	// In-progress wins and carries the watermark.
	s := record{
		CompletedUpTo: 10000,
		InProgress:    offsetmanager.RangeOffset{MinOffset: 10000, MaxOffset: 10999},
		HasInProgress: true,
	}.toState()
	require.Equal(t, offsetmanager.IN_PROGRESS, s.State)
	require.Equal(t, uint64(10000), s.MinOffset)
	require.Equal(t, uint64(10999), s.MaxOffset)
	require.Equal(t, uint64(10000), s.CompletedUpTo)

	// Completed prefix surfaces as a COMPLETED [0, watermark-1] range.
	s = record{CompletedUpTo: 10000}.toState()
	require.Equal(t, offsetmanager.COMPLETED, s.State)
	require.Equal(t, uint64(0), s.MinOffset)
	require.Equal(t, uint64(9999), s.MaxOffset)
	require.Equal(t, uint64(10000), s.CompletedUpTo)

	// Empty → UNKNOWN.
	s = record{}.toState()
	require.Equal(t, offsetmanager.UNKNOWN, s.State)
}

// ---- pure logic: the crash-safety state machine ---------------------------

func inProgress(min, max uint64) offsetmanager.RangeOffsetState {
	return offsetmanager.RangeOffsetState{
		RangeOffset: offsetmanager.RangeOffset{MinOffset: min, MaxOffset: max},
		State:       offsetmanager.IN_PROGRESS,
	}
}

func completed(min, max uint64) offsetmanager.RangeOffsetState {
	return offsetmanager.RangeOffsetState{
		RangeOffset: offsetmanager.RangeOffset{MinOffset: min, MaxOffset: max},
		State:       offsetmanager.COMPLETED,
	}
}

func TestApplyTransition_HappyPath(t *testing.T) {
	cur := record{} // fresh partition

	// Mark first range in progress: watermark untouched.
	next, write, err := applyTransition(cur, inProgress(0, 999))
	require.NoError(t, err)
	require.True(t, write)
	require.Equal(t, uint64(0), next.CompletedUpTo)
	require.True(t, next.HasInProgress)

	// Complete it contiguously (Min == watermark 0): watermark advances, in-progress cleared.
	next, write, err = applyTransition(next, completed(0, 999))
	require.NoError(t, err)
	require.True(t, write)
	require.Equal(t, uint64(1000), next.CompletedUpTo)
	require.False(t, next.HasInProgress)

	// Next contiguous range.
	next, _, _ = applyTransition(next, inProgress(1000, 1999))
	next, write, err = applyTransition(next, completed(1000, 1999))
	require.NoError(t, err)
	require.True(t, write)
	require.Equal(t, uint64(2000), next.CompletedUpTo)
}

func TestApplyTransition_StaleCompletedIsNoOp(t *testing.T) {
	cur := record{CompletedUpTo: 5000}
	// A duplicate/stale COMPLETED entirely below the watermark must never regress it.
	next, write, err := applyTransition(cur, completed(2000, 2999))
	require.NoError(t, err)
	require.False(t, write)
	require.Equal(t, cur, next)
}

func TestApplyTransition_GapBelowIsRefused(t *testing.T) {
	cur := record{CompletedUpTo: 5000}
	// A COMPLETED that starts above the watermark would skip [5000, range.Min); refuse it.
	_, write, err := applyTransition(cur, completed(8000, 8999))
	require.False(t, write)
	require.ErrorIs(t, err, errGapBelowWatermark)
}

func TestApplyTransition_InProgressAtWatermarkRecorded(t *testing.T) {
	cur := record{CompletedUpTo: 5000}
	next, write, err := applyTransition(cur, inProgress(5000, 5999))
	require.NoError(t, err)
	require.True(t, write)
	require.Equal(t, uint64(5000), next.CompletedUpTo) // watermark unchanged
	require.True(t, next.HasInProgress)
}

func TestApplyTransition_StaleInProgressIsNoOp(t *testing.T) {
	// A racing/restarted writer re-reads a range already below the watermark. Recording
	// it would make the reported resume point drop below the watermark (and below YDB's
	// committed offset, which YDB rejects). It must be ignored.
	cur := record{CompletedUpTo: 5000}
	next, write, err := applyTransition(cur, inProgress(2000, 2999))
	require.NoError(t, err)
	require.False(t, write)
	require.Equal(t, cur, next)

	// A genuine in-progress recorded earlier must not be reported below the watermark.
	require.Equal(t, uint64(5000), cur.toState().CompletedUpTo)
}

func TestApplyTransition_GapAboveInProgressRefused(t *testing.T) {
	cur := record{CompletedUpTo: 5000}
	_, write, err := applyTransition(cur, inProgress(8000, 8999))
	require.False(t, write)
	require.ErrorIs(t, err, errGapBelowWatermark)
}

// ---- integration: real ZooKeeper round-trip + persisted layout ------------

func TestStoreGetRangeOffsetState(t *testing.T) {
	conn, _, err := zk.Connect([]string{"127.0.0.1:9182", "127.0.0.1:9181", "127.0.0.1:9183"}, time.Second*10)
	require.NoError(t, err)
	defer conn.Close()

	manager, err := NewKeeperOffsetManager(conn, "/offsets")
	require.NoError(t, err)

	// Use a partition far from the live ones (0..2) and start from a clean node so the
	// CAS read-modify-write begins from a known-empty watermark.
	const part = int64(987654)
	partPath := path.Join("/offsets", strconv.Itoa(int(part)))
	_ = conn.Delete(partPath, -1)
	defer func() { _ = conn.Delete(partPath, -1) }()

	// Mark a range in progress at the start of the partition.
	require.NoError(t, manager.StoreRangeOffsetState(part, inProgress(0, 9)))

	// Persisted payload is the new 25-byte layout: watermark 0, in-progress [0,9], flag set.
	expected := make([]byte, recordSize)
	binary.BigEndian.PutUint64(expected[8:16], 0)
	binary.BigEndian.PutUint64(expected[16:24], 9)
	expected[24] = flagHasInProgress
	raw, _, err := conn.Get(partPath)
	require.NoError(t, err)
	require.Equal(t, expected, raw)

	// Reader sees it as IN_PROGRESS [0,9].
	state, err := manager.GetRangeOffsetState(part)
	require.NoError(t, err)
	require.Equal(t, offsetmanager.IN_PROGRESS, state.State)
	require.Equal(t, uint64(0), state.MinOffset)
	require.Equal(t, uint64(9), state.MaxOffset)

	// Complete it contiguously: watermark advances to 10.
	require.NoError(t, manager.StoreRangeOffsetState(part, completed(0, 9)))
	state, err = manager.GetRangeOffsetState(part)
	require.NoError(t, err)
	require.Equal(t, offsetmanager.COMPLETED, state.State)
	require.Equal(t, uint64(10), state.CompletedUpTo)
	require.Equal(t, uint64(9), state.MaxOffset)

	// A gap-below completion is refused and the watermark does not move.
	err = manager.StoreRangeOffsetState(part, completed(20, 29))
	require.ErrorIs(t, err, errGapBelowWatermark)
	state, err = manager.GetRangeOffsetState(part)
	require.NoError(t, err)
	require.Equal(t, uint64(10), state.CompletedUpTo)
}

func TestGetRangeOffsetState_MissingNode(t *testing.T) {
	conn, _, err := zk.Connect([]string{"127.0.0.1:9182", "127.0.0.1:9181", "127.0.0.1:9183"}, time.Second*10)
	require.NoError(t, err)
	defer conn.Close()

	manager, err := NewKeeperOffsetManager(conn, "/offsets")
	require.NoError(t, err)

	state, err := manager.GetRangeOffsetState(int64(rand.Intn(1_000_000) + 1000))
	require.NoError(t, err)
	require.Equal(t, offsetmanager.UNKNOWN, state.State)
}

func deleteRecursive(conn *zk.Conn, zNodePath string) error {
	children, _, err := conn.Children(zNodePath)
	if err != nil {
		if errors.Is(err, zk.ErrNoNode) {
			return nil
		}
		return err
	}
	for _, child := range children {
		childPath := path.Join(zNodePath, child)
		if err := deleteRecursive(conn, childPath); err != nil {
			return err
		}
	}
	return conn.Delete(zNodePath, -1)
}
