package packer

import (
	"bytes"
)

type Number interface {
	int64 | uint64 | float64
}

// Packer interface specification
type Packer[T Number] interface {

	// Packs the float64 data in the src slice to the dst buffer and returns
	// nil if packing was completed successfuly. Otherwise, returns the error.
	Pack(src []T, dst *bytes.Buffer, op PackOp, opParam T) error

	// Unpacks the float64 data in the src buffer to the dst slice and returns
	// number of elements unpacked along with nil error. Otherwise, returns (0,
	// error).
	Unpack(src *bytes.Buffer, dst []T, op PackOp, opParam T) (uint64, error)

	// Return the size of the packed data
	PackedSize() uint64

	// Return the number of elements in the frame
	NumElements() uint64
}
