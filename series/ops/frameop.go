package ops

import (
	"github.com/rmravindran/ats/series/frame"
	"github.com/rmravindran/ats/series/packer"
)

// Defines an interface for defining functional frame operations on vector
// valued data. Considering Go doesn't support immutable arrays, we need some
// define enforcing contracts... verbally...

type FrameOp[T packer.Number] interface {

	// Applies this frame operator on the specified args and returns an
	// operator that can either represent the retulting values of this
	// operation or be curried further.
	Apply(args []T) *MaybeFrameOp[T]

	// Return the final values produced by the operator, otherwise returns nil
	Values() []T
}

type MaybeFrameOp[T packer.Number] struct {
	frameOp FrameOp[T]
	err     error
}

func JustOp[T packer.Number](frameOp FrameOp[T]) *MaybeFrameOp[T] {
	return &MaybeFrameOp[T]{frameOp: frameOp, err: nil}
}

func ErrorOp[T packer.Number](err error) *MaybeFrameOp[T] {
	return &MaybeFrameOp[T]{frameOp: nil, err: err}
}

func (op *MaybeFrameOp[T]) Error() error {
	return op.err
}

func (op *MaybeFrameOp[T]) Op() FrameOp[T] {
	return op.frameOp
}

func (op *MaybeFrameOp[T]) Apply(frm *frame.Frame[T]) *MaybeFrameOp[T] {

	if op.Error() != nil {
		return op
	}

	return op.frameOp.Apply(frm.Values())
}

func (op *MaybeFrameOp[T]) Values() []T {
	if op.Error() != nil {
		return nil
	}

	return op.frameOp.Values()
}
