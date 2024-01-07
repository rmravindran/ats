package ops

import (
	"errors"

	"github.com/rmravindran/ats/series/packer"
)

// ----------------------------------------------------------------------------
// - OpAdd Struct
// ----------------------------------------------------------------------------

// Represents a multiply operation that can be applied on two time series.
// OpMul invokation takes one time series as an input and return an instance of
// OpMul1 which can be curried further with another time series to generate
// final product.
type OpMul[S packer.Number, T packer.Number] struct {
}

// Represents a multiply operation that can be applied on two time series.
// OpMul1 is an internal representation of OpAdd which contains the first time
// series and can take the second operand to generate the final product.
type OpMul1[S packer.Number, T packer.Number] struct {
	a Transformable[S, T]
}

// --------------
// - CONSTRUCTORS
// --------------

// Create a new OpMul operator
func NewOpMul[S packer.Number, T packer.Number]() *MaybeOp[S, T] {
	return JustOp[S, T](&OpMul[S, T]{})
}

// ----------------
// - PUBLIC METHODS
// ----------------

// Apply the OpMul operator on the specified args and return an operator that
// can be curreried further.
func (op *OpMul[S, T]) Apply(args Transformable[S, T]) *MaybeOp[S, T] {

	if args == nil || args.IsEmpty() {
		return ErrorOp[S, T](errors.New("OpMul on nil/empty array"))
	}

	return JustOp[S, T](&OpMul1[S, T]{a: args})
}

// Returns a nil TxIdentity. Mul operation require two time series. OpMula
// Apply() results in an operator which needs to be further applied on anothera
// time series before producing results.
func (op *OpMul[S, T]) Values() *TxIdentity[T, T] {
	return &TxIdentity[T, T]{values: nil}
}

// Returns nil error.
func (op *OpMul[S, T]) Error() error {
	return nil
}

// Apply the OpMul1 operator on the specified args and returns the final result
// (product of the two series) as an operator.
func (op1 *OpMul1[S, T]) Apply(args Transformable[S, T]) *MaybeOp[S, T] {

	if args == nil || op1.a.Length() != args.Length() {
		return ErrorOp[S, T](errors.New("invalid size for OpAdd arguments"))
	}

	result := make([]T, args.Length())

	for idx := 0; idx < op1.a.Length(); idx++ {
		result[idx] = op1.a.ValueAt(idx) * args.ValueAt(idx)
	}

	var ret = &OpResult[S, T]{
		values: NewTxIdentity[T](result),
		err:    nil,
	}

	return JustOp[S, T](ret)
}

// Returns a nil TxIdentity. Mul operation require two time series. OpMul
// a curried operator which needs to be further applied on another time series
// before producing results.
func (op *OpMul1[S, T]) Values() *TxIdentity[T, T] {
	return &TxIdentity[T, T]{values: nil}
}

// Returns nil error.
func (op *OpMul1[S, T]) Error() error {
	return nil
}
