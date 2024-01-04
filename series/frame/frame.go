package frame

import (
	"bytes"
	"errors"

	"github.com/rmravindran/ats/series/packer"
)

type Frame[T packer.Number] struct {

	// Packed values
	buffer *bytes.Buffer

	// Values expanded to allow native operations
	values []T

	// State of the frame
	state FrameState

	// Packer used to pack and unpack the frame.
	packer packer.Packer[T]

	// Operation to apply during packing and unpacking
	packOp packer.PackOp

	// Operation parameter used for packing and unpacking
	packOpParam T

	// Indicates if the frame is dirty.
	isDirty bool
}

// Create an empty frame that has the capacity to hold size float64 elements
func NewEmptyFrame[T packer.Number](size uint64, p packer.Packer[T]) *Frame[T] {

	frame := &Frame[T]{
		buffer:      nil,
		values:      make([]T, size),
		state:       Native,
		packer:      p,
		packOp:      packer.NOP,
		packOpParam: 0.0,
		isDirty:     true,
	}

	return frame
}

// Create a new frame of the specified size for holding packed buffer
func NewPackedFrame[T packer.Number](buffer *bytes.Buffer, p packer.Packer[T]) *Frame[T] {

	frame := &Frame[T]{
		buffer:      buffer,
		values:      nil,
		state:       Compact,
		packer:      p,
		packOp:      packer.NOP,
		packOpParam: 0.0,
		isDirty:     true,
	}

	return frame
}

// Create an empty frame that has the specified unpacked values
func NewUnpackedFrame[T packer.Number](values []T, p packer.Packer[T]) *Frame[T] {

	frame := &Frame[T]{
		buffer:      nil,
		values:      values,
		state:       Native,
		packer:      p,
		packOp:      packer.NOP,
		packOpParam: 0.0,
		isDirty:     true,
	}

	return frame
}

// Return the value of the frame at the specified index.
func (frame *Frame[T]) Value(index int) (T, error) {

	if frame.state == Unknown {
		return 0, errors.New("Uninitialized frame")
	}

	// Unpack first

	frame.unpackIfNeeded()

	return frame.values[index], nil
}

// Set the element at the given index to the specified value.
func (frame *Frame[T]) SetValue(index int, value T) error {

	if frame.state == Unknown {
		return errors.New("Uninitialized frame")
	}

	if frame.packer.NumElements() >= uint64(index) {
		return errors.New("Index out of bound")
	}

	// Unpack first

	frame.unpackIfNeeded()

	// Set value at index

	frame.values[index] = value

	// Mark the frame as dirty

	frame.isDirty = true

	// All good

	return nil
}

// Finalize the frame by packing the data.
func (frame *Frame[T]) Finalize(reduce bool) error {

	// If frame is not dirty, the nothing to do apart from releasing
	// the memory if the options requires us to do so
	if !frame.isDirty {
		if reduce {
			frame.values = nil
		}
		return nil
	}

	if frame.buffer == nil {
		frame.buffer = &bytes.Buffer{}
	}

	err := frame.packer.Pack(
		frame.values, frame.buffer, frame.packOp, frame.packOpParam)

	if err == nil {
		frame.isDirty = false
		frame.state = Compact

		if reduce {
			frame.values = nil
		}
	}

	return err
}

// If the frame is dirty returns nil, otherwise returns the packed values
// of the frame.
func (frame *Frame[T]) Buffer() *bytes.Buffer {
	if frame.isDirty {
		return nil
	}
	return frame.buffer
}

// If the frame is dirty returns nil, otherwise returns the packed values
// of the frame.
func (frame *Frame[T]) Values() []T {

	if frame.state == Unknown {
		return nil
	}

	// Unpack first

	frame.unpackIfNeeded()

	return frame.values
}

func (frame *Frame[T]) Length() uint64 {
	if frame.state == Compact {
		return frame.packer.NumElements()
	}
	return uint64(len(frame.values))
}

func (frame *Frame[T]) Size() uint64 {
	if frame.values == nil {
		return frame.packer.PackedSize()
	}

	return frame.packer.PackedSize() + uint64(8*len(frame.values))
}

//-----------------------------------------------------------------------------
//                              PRIVATE METHODS
//-----------------------------------------------------------------------------

func (frame *Frame[T]) unpackIfNeeded() {

	if frame.state == Compact {
		frame.values = make([]T, frame.packer.NumElements())
		frame.packer.Unpack(
			frame.buffer, frame.values, frame.packOp, frame.packOpParam)
		frame.state = Native
	}
}
