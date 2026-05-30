package lockers

import "context"

type TTLLocker interface {
	TTLLock(ctx context.Context, partition int64) error
	Unlock(ctx context.Context, partition int64) error
}
