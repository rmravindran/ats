package ops

import (
	"errors"

	"github.com/rmravindran/ats/series/packer"
)

type OpSum[S packer.Number, T packer.Number] struct {
	initialValue T
	windowSize   int
}

// Create a new OpAdd operation that can be applied on two time series to generate
// a new time series which is the sum of the two.
func NewOpSum[T packer.Number](initialValue T, windowSize int) *MaybeOp[T, T] {
	return JustOp[T, T](&OpSum[T, T]{windowSize: windowSize})
}

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

func (op *OpSum[S, T]) Values() *TxIdentity[T, T] {
	return &TxIdentity[T, T]{values: nil}
}

func (op *OpSum[S, T]) Error() error {
	return nil
}
