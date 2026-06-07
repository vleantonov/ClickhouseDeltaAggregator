package offsetmanager

import (
	"errors"
	"fmt"
)

var ErrNoStoredOffset = errors.New("no stored offset")

type OffsetState int8

var (
	UNKNOWN     OffsetState = 0
	IN_PROGRESS OffsetState = 1
	COMPLETED   OffsetState = 2
)

func (o OffsetState) String() string {
	switch o {
	case IN_PROGRESS:
		return "IN_PROGRESS"
	case COMPLETED:
		return "COMPLETED"
	case UNKNOWN:
		return "UNKNOWN"
	}

	panic(fmt.Sprintf("unknown offset state: %d", o))
}

type RangeOffset struct {
	MinOffset uint64
	MaxOffset uint64
}

// RangeOffsetState carries two distinct notions that must not be conflated:
//
//   - RangeOffset+State describe a SINGLE range and its lifecycle. The reader builds
//     these per batch from the incoming message offsets, and the keeper reports the
//     currently-active range here (the IN_PROGRESS attempt, or the completed prefix as
//     a COMPLETED range) so the existing old-vs-new-insert routing keeps working.
//   - CompletedUpTo is the durable, forward-only watermark maintained by the keeper:
//     EVERY offset strictly below CompletedUpTo is durably in ClickHouse. It is the
//     contiguous prefix and is the only safe place to resume reading from after a
//     crash/rebalance.
//
// Separating the two is what makes offset advance crash-safe: a lost or clobbered
// IN_PROGRESS marker can never push CompletedUpTo past an un-written range, because the
// watermark only ever advances by contiguous extension (InProgress.Min == CompletedUpTo).
// The reader leaves CompletedUpTo zero on the batch-local values it constructs; only the
// keeper populates and interprets it.
type RangeOffsetState struct {
	RangeOffset
	State OffsetState

	// CompletedUpTo is exclusive: offsets [0, CompletedUpTo) are durably persisted.
	CompletedUpTo uint64
}

type OffsetManager interface {
	StoreRangeOffsetState(partition int64, r RangeOffsetState) error
	GetRangeOffsetState(partition int64) (RangeOffsetState, error)
}
