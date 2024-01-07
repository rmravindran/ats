package ops

import (
	"github.com/rmravindran/ats/series/packer"
)

// ----------------------------------------------------------------------------
// - TxIdentity Struct
// ----------------------------------------------------------------------------

// Defines a struct for representing an identity transformation of an
// array of values in type T.
type TxIdentity[S packer.Number, T packer.Number] struct {
	values []T
	time   []uint64
}

// --------------
// - CONSTRUCTORS
// --------------

// Create a new TxIdentity struct with the specified values.
func NewTxIdentity[T packer.Number](values []T) *TxIdentity[T, T] {
	return &TxIdentity[T, T]{values: values, time: nil}
}

// Create a new TxIdentity struct with the specified values and time.
func NewTxIdentityWithTime[T packer.Number](values []T, time []uint64) *TxIdentity[T, T] {
	return &TxIdentity[T, T]{values: values, time: time}
}

// ----------------
// - PUBLIC METHODS
// ----------------

// Return the value at the specified index.
func (tx *TxIdentity[S, T]) ValueAt(idx int) T {
	return tx.values[idx]
}

// Return the time at the specified index.
func (tx *TxIdentity[S, T]) TimeAt(idx int) uint64 {
	if tx.time == nil {
		return 0
	}
	return tx.time[idx]
}

// Return true if the underlying transformable array is empty.
func (tx *TxIdentity[S, T]) IsEmpty() bool {
	return len(tx.values) == 0
}

// Return the length of the underlying transformable array.
func (tx *TxIdentity[S, T]) Length() int {
	return len(tx.values)
}
