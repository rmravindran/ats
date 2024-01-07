package ops

import (
	"github.com/rmravindran/ats/series/packer"
)

type TxConst[S packer.Number, T packer.Number] struct {
	c    T
	t    uint64
	size int
}

// -----------------------------------------------------------------------------
// - CONSTRUCTORS
// -----------------------------------------------------------------------------

func NewTxConst[T packer.Number](c T, t uint64, size int) *TxConst[T, T] {
	return &TxConst[T, T]{c: c, t: t, size: size}
}

// -----------------------------------------------------------------------------
// - PUBLIC METHODS
// -----------------------------------------------------------------------------

func (tx *TxConst[S, T]) ValueAt(idx int) T {
	return tx.c
}

func (tx *TxConst[S, T]) TimeAt(idx int) uint64 {
	return tx.t
}

func (tx *TxConst[S, T]) IsEmpty() bool {
	return tx.size == 0
}

func (tx *TxConst[S, T]) Length() int {
	return tx.size
}
