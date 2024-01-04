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

	/*
		// Packs the float64 data in the src slice to the dst buffer and returns
		// nil if packing was completed successfuly. Otherwise, returns the error.
		PackFloat(src []float64, dst *bytes.Buffer, op PackOp, opParam float64) error

		// Packs the int64 data in the src slice to the dst buffer and returns
		// nil if packing was completed successfuly. Otherwise, returns the error.
		PackInt(src []int64, dst *bytes.Buffer, op PackOp, opParam int64) error

		// Packs the uint64 data in the src slice to the dst buffer and returns
		// nil if packing was completed successfuly. Otherwise, returns the error.
		PackUInt(src []uint64, dst *bytes.Buffer, op PackOp, opParam uint64) error

		// Unpacks the float64 data in the src buffer to the dst slice and returns
		// number of elements unpacked along with nil error. Otherwise, returns (0,
		// error).
		UnpackFloat(src *bytes.Buffer, dst []float64, op PackOp, opParam float64) (uint64, error)

		// Unpacks the int64 data in the src buffer to the dst slice and returns
		// number of elements unpacked along with nil error. Otherwise, returns (0,
		// error).
		UnpackInt(src *bytes.Buffer, dst []int64, op PackOp, opParam int64) (uint64, error)

		// Unpacks the uint64 data in the src buffer to the dst slice and returns
		// number of elements unpacked along with nil error. Otherwise, returns (0,
		// error).
		UnpackUInt(src *bytes.Buffer, dst []uint64, op PackOp, opParam uint64) (uint64, error)
	*/

	// Return the size of the packed data
	PackedSize() uint64

	// Return the number of elements in the frame
	NumElements() uint64
}
