package ops

import (
	"github.com/rmravindran/ats/series"
	"github.com/rmravindran/ats/series/packer"
)

type TxSeries[S packer.Number, T packer.Number] struct {
	s      *series.Series[T]
	offset int
}

// -----------------------------------------------------------------------------
// - CONSTRUCTORS
// -----------------------------------------------------------------------------

func NewTxSeries[T packer.Number](s *series.Series[T]) *TxSeries[T, T] {
	return &TxSeries[T, T]{s: s, offset: 0}
}

// -----------------------------------------------------------------------------
// - PUBLIC METHODS
// -----------------------------------------------------------------------------

func (tx *TxSeries[S, T]) ValueAt(idx int) T {
	_, v, _ := tx.s.Value(idx)
	return v
}

func (tx *TxSeries[S, T]) TimeAt(idx int) uint64 {
	t, _, _ := tx.s.Value(idx)
	return t
}

func (tx *TxSeries[S, T]) IsEmpty() bool {
	return tx.s.Size() == 0
}

func (tx *TxSeries[S, T]) Length() int {
	return tx.s.Size()
}

func (tx *TxSeries[S, T]) Offset(offset int) *TxSeries[T, T] {
	if (offset + tx.offset) >= tx.s.Size() {
		return nil
	}
	return &TxSeries[T, T]{s: tx.s, offset: offset}
}
