package reader

import (
	"delta_aggregator/internal/domain"
	offsetmanager "delta_aggregator/internal/offset_manager"
	"testing"
)

// recs builds a record slice carrying just the given offsets — the only field the
// reconciliation logic looks at.
func recs(offsets ...uint64) []domain.Transaction {
	out := make([]domain.Transaction, len(offsets))
	for i, o := range offsets {
		out[i] = domain.Transaction{Offset: o}
	}
	return out
}

func rng(min, max uint64) offsetmanager.RangeOffset {
	return offsetmanager.RangeOffset{MinOffset: min, MaxOffset: max}
}

func rangeState(min, max uint64, state offsetmanager.OffsetState) offsetmanager.RangeOffsetState {
	return offsetmanager.RangeOffsetState{RangeOffset: rng(min, max), State: state}
}

func offsets(records []domain.Transaction) []uint64 {
	out := make([]uint64, len(records))
	for i, r := range records {
		out[i] = r.Offset
	}
	return out
}

func equalOffsets(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestCoversFullRange(t *testing.T) {
	tests := []struct {
		name    string
		records []domain.Transaction
		r       offsetmanager.RangeOffset
		want    bool
	}{
		{"full contiguous range", recs(100, 101, 102, 103), rng(100, 103), true},
		{"single-offset range", recs(100), rng(100, 100), true},
		{"partial prefix", recs(100, 101), rng(100, 103), false},
		{"missing low end", recs(101, 102, 103), rng(100, 103), false},
		{"missing high end", recs(100, 101, 102), rng(100, 103), false},
		{"internal gap (count short)", recs(100, 101, 103), rng(100, 103), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := coversFullRange(tt.records, tt.r); got != tt.want {
				t.Fatalf("coversFullRange(%v, %v) = %v, want %v", offsets(tt.records), tt.r, got, tt.want)
			}
		})
	}
}

func TestMergePendingOld(t *testing.T) {
	tests := []struct {
		name     string
		pending  []domain.Transaction
		incoming []domain.Transaction
		want     []uint64
	}{
		{"empty pending", nil, recs(100, 101), []uint64{100, 101}},
		{"disjoint append", recs(100, 101), recs(102, 103), []uint64{100, 101, 102, 103}},
		{"overlap deduplicated", recs(100, 101, 102), recs(101, 102, 103), []uint64{100, 101, 102, 103}},
		{"out-of-order incoming is sorted", recs(102), recs(100, 101), []uint64{100, 101, 102}},
		{"fully redundant redelivery", recs(100, 101), recs(100, 101), []uint64{100, 101}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := offsets(mergePendingOld(tt.pending, tt.incoming))
			if !equalOffsets(got, tt.want) {
				t.Fatalf("mergePendingOld offsets = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestMergePendingOldAccumulatesToFullRange mirrors the recovery loop: a stored
// IN_PROGRESS range redelivered across several reads must eventually be recognised
// as complete.
func TestMergePendingOldAccumulatesToFullRange(t *testing.T) {
	r := rng(100, 104)
	var pending []domain.Transaction

	pending = mergePendingOld(pending, recs(100, 101))
	if coversFullRange(pending, r) {
		t.Fatalf("range reported complete after first partial read: %v", offsets(pending))
	}

	// A redelivery that overlaps the part we already have.
	pending = mergePendingOld(pending, recs(101, 102, 103))
	if coversFullRange(pending, r) {
		t.Fatalf("range reported complete before the last offset arrived: %v", offsets(pending))
	}

	pending = mergePendingOld(pending, recs(104))
	if !coversFullRange(pending, r) {
		t.Fatalf("range not complete after final read: %v", offsets(pending))
	}
}

func TestValidateOldInsertRecords(t *testing.T) {
	r := &Reader{}
	stored := rangeState(100, 109, offsetmanager.IN_PROGRESS)

	tests := []struct {
		name    string
		records []domain.Transaction
		state   offsetmanager.RangeOffsetState
		wantErr bool
	}{
		{"exact full range", recs(100, 101, 102, 103, 104, 105, 106, 107, 108, 109), rangeState(100, 109, offsetmanager.IN_PROGRESS), false},
		{"contiguous prefix subset", recs(100, 101, 102), rangeState(100, 102, offsetmanager.IN_PROGRESS), false},
		{"contiguous suffix subset", recs(105, 106, 107, 108, 109), rangeState(105, 109, offsetmanager.IN_PROGRESS), false},
		{"below stored min", recs(98, 99, 100), rangeState(98, 100, offsetmanager.IN_PROGRESS), true},
		{"above stored max", recs(108, 109, 110), rangeState(108, 110, offsetmanager.IN_PROGRESS), true},
		{"internal gap", recs(100, 102), rangeState(100, 102, offsetmanager.IN_PROGRESS), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := r.validateOldInsertRecords(tt.records, tt.state, stored)
			if (err != nil) != tt.wantErr {
				t.Fatalf("validateOldInsertRecords err = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateNewInsertRecords(t *testing.T) {
	r := &Reader{}

	tests := []struct {
		name     string
		records  []domain.Transaction
		state    offsetmanager.RangeOffsetState
		previous offsetmanager.RangeOffsetState
		wantErr  bool
	}{
		{
			name:     "first ever insert (no previous range)",
			records:  recs(0, 1, 2),
			state:    rangeState(0, 2, offsetmanager.IN_PROGRESS),
			previous: offsetmanager.RangeOffsetState{State: offsetmanager.UNKNOWN},
			wantErr:  false,
		},
		{
			name:     "monotonic continuation after completed range",
			records:  recs(110, 111, 112),
			state:    rangeState(110, 112, offsetmanager.IN_PROGRESS),
			previous: rangeState(100, 109, offsetmanager.COMPLETED),
			wantErr:  false,
		},
		{
			name:     "gap after previous range",
			records:  recs(111, 112),
			state:    rangeState(111, 112, offsetmanager.IN_PROGRESS),
			previous: rangeState(100, 109, offsetmanager.COMPLETED),
			wantErr:  true,
		},
		{
			name:     "non-contiguous records",
			records:  recs(110, 112),
			state:    rangeState(110, 112, offsetmanager.IN_PROGRESS),
			previous: rangeState(100, 109, offsetmanager.COMPLETED),
			wantErr:  true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := r.validateNewInsertRecords(tt.records, tt.state, tt.previous)
			if (err != nil) != tt.wantErr {
				t.Fatalf("validateNewInsertRecords err = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}
