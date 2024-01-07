package ops

import (
	"errors"

	"github.com/rmravindran/ats/series/packer"
)

type OpMulAdd[S packer.Number, T packer.Number] struct {
	c T
}

type OpMulAdd1[S packer.Number, T packer.Number] struct {
	c T
	a Transformable[S, T]
}

// Create a new OpMulAdd operation with the specified constant C that can be
// applied on two time series A and B to generate a new time series which is the
// equavalent of C * A + B
func NewOpMulAdd[S packer.Number, T packer.Number](c T) *MaybeOp[S, T] {
	return JustOp[S, T](&OpMulAdd[S, T]{c: c})
}

func NewOpMulAddT[T packer.Number](c T) *MaybeOp[T, T] {
	return JustOp[T, T](&OpMulAdd[T, T]{c: c})
}

// Apply the operation on the specified values and return true if the value
// has been modified inplace. Otherwise, return false.
func (op *OpMulAdd[S, T]) Apply(args Transformable[S, T]) *MaybeOp[S, T] {

	if args == nil || args.IsEmpty() {
		return ErrorOp[S, T](errors.New("OpAdd on nil/empty array"))
	}

	return JustOp[S, T](&OpMulAdd1[S, T]{c: op.c, a: args})
}

func (op *OpMulAdd[S, T]) Values() *TxIdentity[T, T] {
	return &TxIdentity[T, T]{values: nil}
}

func (op *OpMulAdd[S, T]) Error() error {
	return nil
}

func (op1 *OpMulAdd1[S, T]) Apply(args Transformable[S, T]) *MaybeOp[S, T] {

	if args == nil || op1.a.Length() != args.Length() {
		return ErrorOp[S, T](errors.New("invalid size for OpAdd arguments"))
	}

	result := make([]T, args.Length())

	for idx := 0; idx < op1.a.Length(); idx++ {
		result[idx] = (op1.c * op1.a.ValueAt(idx)) + args.ValueAt(idx)
	}

	var ret = &OpResult[S, T]{
		values: NewTxIdentity[T](result),
		err:    nil,
	}

	return JustOp[S, T](ret)
}

func (op *OpMulAdd1[S, T]) Values() *TxIdentity[T, T] {
	return &TxIdentity[T, T]{values: nil}
}

func (op *OpMulAdd1[S, T]) Error() error {
	return nil
}
