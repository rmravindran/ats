package ops

import (
	"errors"

	"github.com/rmravindran/ats/series/packer"
)

type OpAdd[T packer.Number] struct {
}

type OpAdd1[T packer.Number] struct {
	a []T
}

// Apply the operation on the specified values and return true if the value
// has been modified inplace. Otherwise, return false.
func (op *OpAdd[T]) Apply(values []T) *MaybeFrameOp[T] {

	if len(values) == 0 {
		return ErrorOp[T](errors.New("OpAdd on nil/empty array"))
	}

	return JustOp[T](&OpAdd1[T]{a: values})
}

func (op *OpAdd[T]) Values() []T {
	return nil
}

func (op *OpAdd[T]) Error() error {
	return nil
}

func (op1 *OpAdd1[T]) Apply(values []T) *MaybeFrameOp[T] {

	if len(op1.a) != len(values) {
		return ErrorOp[T](errors.New("invalid size for OpAdd arguments"))
	}

	var ret = &OpResult[T]{
		values: make([]T, len(values)),
	}

	for idx := range op1.a {
		ret.values[idx] = op1.a[idx] + values[idx]
	}

	return JustOp[T](ret)
}

func (op *OpAdd1[T]) Values() []T {
	return nil
}

func (op *OpAdd1[T]) Error() error {
	return nil
}
