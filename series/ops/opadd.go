package ops

import (
	"errors"

	"github.com/rmravindran/ats/series/packer"
)

// ----------------------------------------------------------------------------
// - OpAdd Struct
// ----------------------------------------------------------------------------

// Represents an add operation that can be applied on two time series. OpAdd
// invokation takes one time series as an input and return an instance of OpAdd1
// which can be curried further with another time series to generate final sum.
type OpAdd[S packer.Number, T packer.Number] struct {
}

// Represents an add operation that can be applied on two time series. OpAdd1
// is an internal representation of OpAdd which contains the first time series
// and can take the second operand to generate the final sum.
type OpAdd1[S packer.Number, T packer.Number] struct {
	a Transformable[S, T]
}

// --------------
// - CONSTRUCTORS
// --------------

// Create a new OpAdd operator
func NewOpAdd[S packer.Number, T packer.Number]() *MaybeOp[S, T] {
	return JustOp[S, T](&OpAdd[S, T]{})
}

// ----------------
// - PUBLIC METHODS
// ----------------

// Apply the OpAdd operator on the specified args and return an operator that
// can be curreried further.
func (op *OpAdd[S, T]) Apply(args Transformable[S, T]) *MaybeOp[S, T] {

	if args == nil || args.IsEmpty() {
		return ErrorOp[S, T](errors.New("OpAdd on nil/empty array"))
	}

	return JustOp[S, T](&OpAdd1[S, T]{a: args})
}

// Returns a nil TxIdentity. Add operation require two time series. OpAdd
// Apply() results in an operator which needs to be further applied on another
// time series before producing results.
func (op *OpAdd[S, T]) Values() *TxIdentity[T, T] {
	return &TxIdentity[T, T]{values: nil}
}

// Returns nil error.
func (op *OpAdd[S, T]) Error() error {
	return nil
}

// Apply the OpAdd1 operator on the specified args and returns the final result
// (sum of two series) as an operator.
func (op1 *OpAdd1[S, T]) Apply(args Transformable[S, T]) *MaybeOp[S, T] {

	if args == nil || op1.a.Length() != args.Length() {
		return ErrorOp[S, T](errors.New("invalid size for OpAdd arguments"))
	}

	result := make([]T, args.Length())

	for idx := 0; idx < op1.a.Length(); idx++ {
		result[idx] = op1.a.ValueAt(idx) + args.ValueAt(idx)
	}

	var ret = &OpResult[S, T]{
		values: NewTxIdentity[T](result),
		err:    nil,
	}

	return JustOp[S, T](ret)
}

// Returns a nil TxIdentity. Add operation require two time series. OpAdd1 is
// a curried operator which needs to be further applied on another time series
// before producing results.
func (op *OpAdd1[S, T]) Values() *TxIdentity[T, T] {
	return &TxIdentity[T, T]{values: nil}
}

// Returns nil error.
func (op *OpAdd1[S, T]) Error() error {
	return nil
}
