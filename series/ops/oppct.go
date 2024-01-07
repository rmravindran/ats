package ops

import (
	"errors"

	"github.com/rmravindran/ats/series/packer"
)

// ----------------------------------------------------------------------------
// - OpAdd Struct
// ----------------------------------------------------------------------------

// Represents a percentile operation that can be applied on a transformable
// array. The percentile is computed over a window of values.
type OpPct[S packer.Number, T packer.Number] struct {
	windowSize int
	pct        float64
}

// --------------
// - CONSTRUCTORS
// --------------

// Create an new OpPct operator that has the specified windowSize and percentile
func NewOpPct[T packer.Number](windowSize int, percentile float64) *MaybeOp[T, T] {
	return JustOp[T, T](&OpPct[T, T]{windowSize: windowSize, pct: percentile})
}

// ----------------
// - PUBLIC METHODS
// ----------------

// Apply the OpPct operator on the specified args and return an operator that
// contains the percentiles computed over the specified windowSize.
func (op *OpPct[S, T]) Apply(args Transformable[S, T]) *MaybeOp[S, T] {

	if args == nil || args.IsEmpty() {
		return ErrorOp[S, T](errors.New("invalid size for OpAdd arguments"))
	}

	// Iterage over the values in args and compute the percentile for every
	// windowSize elements in the values.
	numValues := args.Length()
	resultSize := numValues / op.windowSize

	// If not enough values to compute the percentile, return nil
	if resultSize == 0 {
		return ErrorOp[S, T](errors.New("not enough values to compute percentile"))
	}

	// Copy the values from args into a slice of float64. This is needed because
	// the quickselect algorithm works on random access arrays and transformable
	// does not impose a random access accessor on the values.
	values := make([]T, args.Length())
	for idx := 0; idx < args.Length(); idx++ {
		values[idx] = args.ValueAt(idx)
	}

	// Compute the percentile for every windowSize elements in the values.
	resV := make([]T, resultSize)
	resT := make([]uint64, resultSize)
	resNdx := 0
	for idx := 0; idx < args.Length(); idx, resNdx = idx+op.windowSize, resNdx+1 {
		pct := calculatePercentile(values[idx:idx+op.windowSize], op.pct)
		resV[resNdx] = T(pct)
		resT[resNdx] = args.TimeAt(idx)
	}

	var ret = &OpResult[S, T]{
		values: NewTxIdentityWithTime[T](resV, resT),
		err:    nil,
	}

	return JustOp[S, T](ret)
}

// Return a nil TxIdentity. Pct operation require a time series. OpPct Apply()
// can be called to get the result of the operation.
func (op *OpPct[S, T]) Values() *TxIdentity[T, T] {
	return &TxIdentity[T, T]{values: nil}
}

// Returns nil error.
func (op *OpPct[S, T]) Error() error {
	return nil
}

// -----------------
// - PRIVATE METHODS
// -----------------

func calculatePercentile[T packer.Number](data []T, percentile float64) T {
	if len(data) == 0 {
		return 0
	}

	// Apply partial sort to get k-th and k+1-th largest elements.
	k := int(float64(len(data)-1) * (percentile / 100.0))

	// kth largest element.
	quickselect(data, k)
	kthValue := data[k]

	// k+1th largest element is the largest element in the remaining array.
	kplus1Value := kthValue
	if percentile > 0.0 && k+1 < len(data) {
		if (k + 1) == len(data)-1 {
			kplus1Value = data[k+1]
		} else {
			quickselect(data[k+1:], 1)
			kplus1Value = data[k+1]
		}
	}

	return (kthValue + kplus1Value) / 2
}

// Partially sort the data using quickselect algorithm.
func quickselect[T packer.Number](data []T, k int) {
	for left, right := 0, len(data)-1; ; {
		pivotIndex := partition(data, left, right)
		if k == pivotIndex {
			return
		} else if k < pivotIndex {
			right = pivotIndex - 1
		} else {
			left = pivotIndex + 1
		}
	}
}

// Partition the array
func partition[T packer.Number](data []T, left, right int) int {
	pivotIndex := left + (right-left)/2
	pivotValue := data[pivotIndex]

	// Move the pivot to the end of the array
	data[pivotIndex], data[right] = data[right], data[pivotIndex]

	// Partition the data around the pivot
	i := left
	for j := left; j < right; j++ {
		if data[j] < pivotValue {
			data[i], data[j] = data[j], data[i]
			i++
		}
	}
	data[i], data[right] = data[right], data[i]

	return i
}
