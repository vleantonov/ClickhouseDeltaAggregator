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

type RangeOffsetState struct {
	RangeOffset
	State OffsetState
}

type OffsetManager interface {
	StoreRangeOffsetState(partition int64, r RangeOffsetState) error
	GetRangeOffsetState(partition int64) (RangeOffsetState, error)
}
