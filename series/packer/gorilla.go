package packer

// The core compaction logic is based ont the VLDB 2015 paper "Gorilla: A Fast,
// Scalable, In-Memory Time Series Database" by Teller, et al.

// Paper Link: http://www.vldb.org/pvldb/vol8/p1816-teller.pdf

import (
	"bytes"
	"errors"
	"math"
	"math/bits"

	"github.com/dgryski/go-bitstream"
)

type Gorilla[T Number] struct {
	storedLeadingZeros  uint64
	storedTrailingZeros uint64
	storedValue         uint64
	size                uint64
	numElements         uint64
	smallInts           bool
	first               bool
}

func NewGorilla[T Number]() *Gorilla[T] {
	return &Gorilla[T]{
		storedLeadingZeros:  math.MaxInt32,
		storedTrailingZeros: 0,
		storedValue:         0,
		size:                0,
		numElements:         0,
		smallInts:           true,
		first:               true,
	}
}

// Packs the float64 data in the src slice to the dst buffer and returns
// nil if packing was completed successfuly. Otherwise, returns the error.
func (gor *Gorilla[T]) Pack(src []T, dst *bytes.Buffer, op PackOp, opParam T) error {
	switch any(opParam).(type) {
	case int64:
		return gor.packInt(src, dst, op, opParam)
	case uint64:
		return gor.packUInt(src, dst, op, opParam)
	case float64:
		return gor.packFloat(src, dst, op, opParam)
	}

	return errors.New("unsupported type in pack")
}

// Unpacks the float64 data in the src buffer to the dst slice and returns
// number of elements unpacked along with nil error. Otherwise, returns (0,
// error).
func (gor *Gorilla[T]) Unpack(src *bytes.Buffer, dst []T, op PackOp, opParam T) (uint64, error) {

	switch any(opParam).(type) {
	case int64:
		return gor.unpackInt(src, dst, op, opParam)
	case uint64:
		return gor.unpackUInt(src, dst, op, opParam)
	case float64:
		return gor.unpackFloat(src, dst, op, opParam)
	}

	return 0, errors.New("unsupported type in unpack")
}

// Return the size of the packed data
func (gor *Gorilla[T]) PackedSize() uint64 {
	return (gor.size + 7) / 8
}

// Reutrn the umber of elements in the frame
func (gor *Gorilla[T]) NumElements() uint64 {
	return gor.numElements
}

// -----------------------------------------------------------------------------
//
//	PRIVATE METHODS
//
// -----------------------------------------------------------------------------

// Packs the float64 data in the src slice to the dst buffer and returns the buffer
// Otherwise, returns (nil, error).
func (gor *Gorilla[T]) packFloat(
	src []T, dst *bytes.Buffer, op PackOp, opParam T) error {
	gor.storedLeadingZeros = math.MaxInt64
	gor.storedValue = 0
	gor.first = true
	gor.size = 0
	gor.numElements = 0

	bitStream := bitstream.NewWriter(dst)
	for ndx := range src {
		val := T(src[ndx])
		switch op {
		case NOP:
			gor.addUIntValue(bitStream, math.Float64bits(float64(val)))
		case Offset:
			gor.addUIntValue(bitStream, math.Float64bits(float64(val+opParam)))
		case Delta:
			gor.addUIntValue(bitStream, math.Float64bits(float64(val-opParam)))
			opParam = val
		}
		gor.numElements++
	}
	bitStream.Flush(false)

	return nil
}

// Packs the uint64 data in the src slice to the dst buffer and returns
// nil if packing was completed successfuly. Otherwise, returns the error.
func (gor *Gorilla[T]) packInt(
	src []T, dst *bytes.Buffer, op PackOp, opParam T) error {
	gor.storedLeadingZeros = math.MaxInt64
	gor.storedValue = 0
	gor.first = true
	gor.size = 0
	gor.numElements = 0

	bitStream := bitstream.NewWriter(dst)

	// First pack the sign bits
	for ndx := range src {
		if src[ndx] < 0 {
			bitStream.WriteBit(true)
		} else {
			bitStream.WriteBit(false)
		}
	}
	gor.size += uint64(len(src))

	for ndx := range src {
		val := src[ndx]
		if val < 0 {
			val = T(int64(val) * -1)
		}
		switch op {
		case NOP:
			uVal := uint64(val)
			if gor.smallInts {
				uVal = (uVal << 32) | (uVal >> 32)
			}
			gor.addUIntValue(bitStream, uVal)
		case Offset:
			uVal := uint64(val + opParam)
			if gor.smallInts {
				uVal = (uVal << 32) | (uVal >> 32)
			}
			gor.addUIntValue(bitStream, uVal)
		case Delta:
			uVal := uint64(val - opParam)
			if gor.smallInts {
				uVal = (uVal << 32) | (uVal >> 32)
			}
			gor.addUIntValue(bitStream, uVal)
			opParam = val
		}
		gor.numElements++
	}
	bitStream.Flush(false)

	return nil
}

// Packs the uint64 data in the src slice to the dst buffer and returns
// nil if packing was completed successfuly. Otherwise, returns the error.
func (gor *Gorilla[T]) packUInt(
	src []T, dst *bytes.Buffer, op PackOp, opParam T) error {
	gor.storedLeadingZeros = math.MaxInt64
	gor.storedValue = 0
	gor.first = true
	gor.size = 0
	gor.numElements = 0

	bitStream := bitstream.NewWriter(dst)
	for ndx := range src {
		val := src[ndx]
		switch op {
		case NOP:
			uVal := uint64(val)
			if gor.smallInts {
				uVal = (uVal << 32) | (uVal >> 32)
			}
			gor.addUIntValue(bitStream, uVal)
		case Offset:
			uVal := uint64(val + opParam)
			if gor.smallInts {
				uVal = (uVal << 32) | (uVal >> 32)
			}
			gor.addUIntValue(bitStream, uVal)
		case Delta:
			uVal := uint64(val - opParam)
			if gor.smallInts {
				uVal = (uVal << 32) | (uVal >> 32)
			}
			gor.addUIntValue(bitStream, uVal)
			opParam = val
		}
		gor.numElements++
	}
	bitStream.Flush(false)

	return nil
}

// Unpacks the float64 data in the src buffer to the dst slice and returns the
// number of float64 elements that was unpacked. Otherwise, returns (0,
// error).
func (gor *Gorilla[T]) unpackFloat(
	src *bytes.Buffer, dst []T, op PackOp, opParam T) (uint64, error) {

	gor.storedLeadingZeros = math.MaxInt64
	gor.storedTrailingZeros = 0
	gor.storedValue = 0
	gor.first = true
	gor.size = 0

	bitStream := bitstream.NewReader(src)

	var readElements uint64 = 0
	for readElements < gor.numElements {
		gor.next(bitStream)
		switch op {
		case NOP:
			dst[readElements] = T(math.Float64frombits(gor.storedValue))
		case Offset:
			dst[readElements] = T(math.Float64frombits(gor.storedValue)) - opParam
		case Delta:
			dst[readElements] = T(math.Float64frombits(gor.storedValue)) + opParam
			opParam = dst[readElements]
		}
		readElements++
	}

	return readElements, nil

}

// Unpacks the int64 data in the src buffer to the dst slice and returns the
// number of int64 elements that was unpacked. Otherwise, returns (0,
// error).
func (gor *Gorilla[T]) unpackInt(
	src *bytes.Buffer, dst []T, op PackOp, opParam T) (uint64, error) {

	gor.storedLeadingZeros = math.MaxInt64
	gor.storedTrailingZeros = 0
	gor.storedValue = 0
	gor.first = true
	gor.size = 0

	bitStream := bitstream.NewReader(src)

	negInd := make([]int64, gor.NumElements())
	var ndx uint64 = 0
	for ndx < gor.NumElements() {
		var bits, _ = bitStream.ReadBits(1)
		v := int64(bits)
		if v == 1 {
			negInd[ndx] = -1
		} else {
			negInd[ndx] = 1
		}
		ndx++
	}

	var readElements uint64 = 0
	for readElements < gor.numElements {
		gor.next(bitStream)
		switch op {
		case NOP:
			uVal := gor.storedValue
			if gor.smallInts {
				uVal = (uVal << 32) | (uVal >> 32)
			}
			dst[readElements] = T(uVal)
		case Offset:
			uVal := gor.storedValue
			if gor.smallInts {
				uVal = (uVal << 32) | (uVal >> 32)
			}
			dst[readElements] = T(uVal) - opParam

		case Delta:
			uVal := gor.storedValue
			if gor.smallInts {
				uVal = (uVal << 32) | (uVal >> 32)
			}
			dst[readElements] = T(uVal) + opParam
			opParam = dst[readElements]
		}
		dst[readElements] *= T(negInd[readElements])
		readElements++
	}

	return readElements, nil

}

// Unpacks the float64 data in the src buffer to the dst slice and returns the
// number of float64 elements that was unpacked. Otherwise, returns (0,
// error).
func (gor *Gorilla[T]) unpackUInt(
	src *bytes.Buffer, dst []T, op PackOp, opParam T) (uint64, error) {

	gor.storedLeadingZeros = math.MaxInt64
	gor.storedTrailingZeros = 0
	gor.storedValue = 0
	gor.first = true
	gor.size = 0

	bitStream := bitstream.NewReader(src)

	var readElements uint64 = 0
	for readElements < gor.numElements {
		gor.next(bitStream)
		switch op {
		case NOP:
			uVal := gor.storedValue
			if gor.smallInts {
				uVal = (uVal << 32) | (uVal >> 32)
			}
			dst[readElements] = T(uVal)
		case Offset:
			uVal := gor.storedValue
			if gor.smallInts {
				uVal = (uVal << 32) | (uVal >> 32)
			}
			dst[readElements] = T(uVal) - opParam
		case Delta:
			uVal := gor.storedValue
			if gor.smallInts {
				uVal = (uVal << 32) | (uVal >> 32)
			}
			dst[readElements] = T(uVal) + opParam
			opParam = dst[readElements]
		}
		readElements++
	}

	return readElements, nil
}

func (gor *Gorilla[T]) addUIntValue(bitStream *bitstream.BitWriter, value uint64) {
	if gor.first {
		gor.writeFirst(bitStream, value)
	} else {
		gor.compressValue(bitStream, value)
	}
}

func (gor *Gorilla[T]) writeFirst(bitStream *bitstream.BitWriter, value uint64) {
	gor.first = false
	gor.storedValue = value
	bitStream.WriteBits(gor.storedValue, 64)
	gor.size += 64
}

func (gor *Gorilla[T]) compressValue(bitStream *bitstream.BitWriter, value uint64) {
	var xor uint64 = gor.storedValue ^ value
	if xor == 0 {
		bitStream.WriteBits(uint64(0), 1)
		gor.size += 1
	} else {
		var leadingZeros uint64 = uint64(bits.LeadingZeros64(xor))
		var trailingZeros uint64 = uint64(bits.TrailingZeros64(xor))

		if leadingZeros >= 32 {
			leadingZeros = 31
		}

		bitStream.WriteBits(uint64(1), 1)

		if leadingZeros >= gor.storedLeadingZeros && trailingZeros >= gor.storedTrailingZeros {
			bitStream.WriteBits(uint64(0), 1)
			var significantBits uint64 = 64 - gor.storedLeadingZeros - gor.storedTrailingZeros
			bitStream.WriteBits(xor>>gor.storedTrailingZeros, int(significantBits))
			gor.size += 1 + significantBits
		} else {
			bitStream.WriteBits(uint64(1), 1)
			bitStream.WriteBits(leadingZeros, 5)
			var significantBits uint64 = 64 - leadingZeros - trailingZeros
			if significantBits == 64 {
				bitStream.WriteBits(uint64(0), 6)
			} else {
				bitStream.WriteBits(significantBits, 6)
			}

			bitStream.WriteBits(xor>>trailingZeros, int(significantBits))

			gor.storedLeadingZeros = leadingZeros
			gor.storedTrailingZeros = trailingZeros

			gor.size += 1 + 5 + 6 + significantBits
		}
	}

	gor.storedValue = value
}

func (gor *Gorilla[T]) next(bitStream *bitstream.BitReader) error {
	if gor.first {
		gor.first = false
		var val, err = bitStream.ReadBits(64)
		if err != nil {
			return err
		}
		gor.storedValue = val
		return nil
	}
	return gor.nextValue(bitStream)
}

func (gor *Gorilla[T]) nextValue(bitStream *bitstream.BitReader) error {

	var significantBits uint64 = 0
	var value uint64 = 0

	// Read value
	var flag, err = bitStream.ReadBits(1)
	if err != nil {
		return err
	}

	// If 0, the value is the same as the storedValue
	if flag == 0 {
		return nil
	}

	var updatedLeadingZeros, _ = bitStream.ReadBits(1)
	if updatedLeadingZeros != 0 {
		gor.storedLeadingZeros, _ = bitStream.ReadBits(5)

		significantBits, _ = bitStream.ReadBits(6)
		if significantBits == 0 {
			significantBits = 64
		}
		gor.storedTrailingZeros = 64 - significantBits - gor.storedLeadingZeros
	}

	value, _ = bitStream.ReadBits(64 - int(gor.storedLeadingZeros+gor.storedTrailingZeros))
	value <<= gor.storedTrailingZeros
	value = gor.storedValue ^ value
	gor.storedValue = value

	return nil
}
