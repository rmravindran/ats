package ops

import (
	"errors"

	"github.com/rmravindran/ats/series/packer"
)

// ----------------------------------------------------------------------------
// - OpAdd Struct
// ----------------------------------------------------------------------------

// Return an operation that return the minimum value of a time series over
// a specified window size.
type OpMin[S packer.Number, T packer.Number] struct {
	windowSize int
}

// --------------
// - CONSTRUCTORS
// --------------

// Create a new OpMin operator with the specified windowSize.
func NewOpMin[T packer.Number](windowSize int) *MaybeOp[T, T] {
	return JustOp[T, T](&OpMin[T, T]{windowSize: windowSize})
}

// ----------------
// - PUBLIC METHODS
// ----------------

// Apply the OpMin operator on the specified args and return an operator that
// contains the minimum value of the values over the specified windowSize.
func (op *OpMin[S, T]) Apply(args Transformable[S, T]) *MaybeOp[S, T] {

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
		minV := args.ValueAt(idx)
		tStart := args.TimeAt(idx)
		for jdx := 1; jdx < op.windowSize; jdx++ {
			tmp := args.ValueAt(idx + jdx)
			if tmp < minV {
				minV = tmp
			}
		}
		resV[resNdx] = T(minV)
		resT[resNdx] = tStart
	}

	var ret = &OpResult[S, T]{
		values: NewTxIdentityWithTime[T](resV, resT),
		err:    nil,
	}

	return JustOp[S, T](ret)
}

// Returns a nil TxIdentity. Min operation is the result of the Apply function.
// The result is returned as an operator from the Apply invocation.
func (op *OpMin[S, T]) Values() *TxIdentity[T, T] {
	return &TxIdentity[T, T]{values: nil}
}

// Returns nil error.
func (op *OpMin[S, T]) Error() error {
	return nil
}
