package ops

import (
	"github.com/rmravindran/ats/series/packer"
)

type TxNegate[S packer.Number, T packer.Number] struct {
	values []S
	time   []uint64
}

// -----------------------------------------------------------------------------
// - CONSTRUCTORS
// -----------------------------------------------------------------------------

func NewTxNegate[T packer.Number](values []T) Transformable[T, T] {
	return &TxNegate[T, T]{values: values}
}

// -----------------------------------------------------------------------------
// - PUBLIC METHODS
// -----------------------------------------------------------------------------

func (tx *TxNegate[S, T]) ValueAt(idx int) T {
	return T(-tx.values[idx])
}

func (tx *TxNegate[S, T]) TimeAt(idx int) uint64 {
	if tx.time == nil {
		return 0
	}
	return tx.time[idx]
}

func (tx *TxNegate[S, T]) IsEmpty() bool {
	return len(tx.values) == 0
}

func (tx *TxNegate[S, T]) Length() int {
	return len(tx.values)
}
