package ops

import (
	"github.com/rmravindran/ats/series/packer"
)

// ----------------------------------------------------------------------------
// - Op Interface
// ----------------------------------------------------------------------------

// Defines an interface for functional frame operations on vector valued
// transformable data.
type Op[S packer.Number, T packer.Number] interface {

	// Applies this frame operator on the specified args and returns an
	// operator that can either represent the retulting values of this
	// operation or be curried further.
	Apply(args Transformable[S, T]) *MaybeOp[S, T]

	// Return the final values produced by the operator, otherwise returns nil
	Values() *TxIdentity[T, T]
}

// ----------------------------------------------------------------------------
// - MaybeOp Struct
// ----------------------------------------------------------------------------

// Provides functionality similar to the Maybe monad in Haskell. This is used
// to represent the result of an operation that can either be an error or a
// valid operation.
type MaybeOp[S packer.Number, T packer.Number] struct {
	op  Op[S, T]
	err error
}

// --------------
// - CONSTRUCTORS
// --------------

// Create a new MaybeOp that represents a valid operation.
func JustOp[S packer.Number, T packer.Number](op Op[S, T]) *MaybeOp[S, T] {
	return &MaybeOp[S, T]{op: op, err: nil}
}

// Create a new MaybeOp that represents an error.
func ErrorOp[S packer.Number, T packer.Number](err error) *MaybeOp[S, T] {
	return &MaybeOp[S, T]{op: nil, err: err}
}

// ----------------
// - PUBLIC METHODS
// ----------------

// Return the error if the operation failed, otherwise returns nil
func (op *MaybeOp[S, T]) Error() error {
	return op.err
}

// Return the underlying operator
func (op *MaybeOp[S, T]) Op() Op[S, T] {
	return op.op
}

// Apply the operation on the specified transformable and return a new operator
// that can either represent the resulting values of this operation or be
// further curried.
func (op *MaybeOp[S, T]) Apply(args Transformable[S, T]) *MaybeOp[S, T] {

	if op.Error() != nil {
		return op
	}

	return op.op.Apply(args)
}

// Return the final values produced by the operator, otherwise returns nil
func (op *MaybeOp[S, T]) Values() *TxIdentity[T, T] {
	if op.Error() != nil {
		return nil
	}

	return op.op.Values()
}
