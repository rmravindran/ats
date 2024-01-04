package ops

import (
	"github.com/rmravindran/ats/series/packer"
)

type OpResult[T packer.Number] struct {
	values []T
	err    error
}

func (result *OpResult[T]) Apply(values []T) *MaybeFrameOp[T] {
	return JustOp[T](result)
}

func (result *OpResult[T]) Values() []T {
	return result.values
}
