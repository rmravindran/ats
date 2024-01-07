package ops

import (
	"errors"

	"github.com/rmravindran/ats/series/packer"
)

// ----------------------------------------------------------------------------
// - OpAdd Struct
// ----------------------------------------------------------------------------

// Represents an operation that can be applied on a time series to generate
// windowed sum of the values.
type OpSum[S packer.Number, T packer.Number] struct {
	initialValue T
	windowSize   int
}

// --------------
// - CONSTRUCTORS
// --------------

// Create a new OpSum operator with the specified initiatl size and windowSize.
func NewOpSum[T packer.Number](initialValue T, windowSize int) *MaybeOp[T, T] {
	return JustOp[T, T](&OpSum[T, T]{
		initialValue: initialValue,
		windowSize:   windowSize})
}

// ----------------
// - PUBLIC METHODS
// ----------------

// Apply the OpSum operator on the specified args and return an operator that
// contains the sum of the values over the specified windowSize.
func (op *OpSum[S, T]) Apply(args Transformable[S, T]) *MaybeOp[S, T] {

	if args == nil || args.IsEmpty() {
		return ErrorOp[S, T](errors.New("invalid size for OpAdd arguments"))
	}

	numValues := args.Length()
	resultSize := numValues / op.windowSize

	// If not enough values to compute the sum, return nil
	if resultSize == 0 {
		return ErrorOp[S, T](errors.New("not enough values to compute sum"))
	}

	resV := make([]T, resultSize)
	resT := make([]uint64, resultSize)

	resNdx := 0
	for idx := 0; idx < args.Length(); idx, resNdx = idx+op.windowSize, resNdx+1 {
		sumV := args.ValueAt(idx)
		if idx == 0 {
			sumV += op.initialValue
		}
		tStart := args.TimeAt(idx)

		for jdx := 1; jdx < op.windowSize; jdx++ {
			sumV += args.ValueAt(idx + jdx)
		}
		resV[resNdx] = T(sumV)
		resT[resNdx] = tStart
	}

	var ret = &OpResult[S, T]{
		values: NewTxIdentityWithTime[T](resV, resT),
		err:    nil,
	}

	return JustOp[S, T](ret)
}

// Returns a nil TxIdentity. Sum operation is the result of the Apply function.
// The result is returned as an operator from the Apply invocation.
func (op *OpSum[S, T]) Values() *TxIdentity[T, T] {
	return &TxIdentity[T, T]{values: nil}
}

// Returns a nil error.
func (op *OpSum[S, T]) Error() error {
	return nil
}
