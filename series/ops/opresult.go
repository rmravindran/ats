package ops

import (
	"github.com/rmravindran/ats/series/packer"
)

type OpResult[S packer.Number, T packer.Number] struct {
	values *TxIdentity[T, T]
	err    error
}

func (result *OpResult[S, T]) Apply(args Transformable[S, T]) *MaybeOp[S, T] {
	return JustOp[S, T](result)
}

func (result *OpResult[S, T]) Values() *TxIdentity[T, T] {
	return result.values
}

func (result *OpResult[S, T]) Error() error {
	return result.err
}
