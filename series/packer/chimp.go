package packer

// The core compaction logic is based on the VLDB 2022 paper about the Chimp
// compression algo. This specific implementation looks at the streaming variant
// presentedin the paper.
//
// Our implementation allows both Offet and Delta pre-compact operations
// in the pack and unpack methods.
//
// Ref: Panagiotis Liakos, Katia Papakonstantinopoulou, and Yannis Kotidis.
//      Chimp: Efficient Lossless Floating Point Compression for Time Series
//      Databases. PVLDB, 15(11): 3058 - 3070, 2022
// Paper Link: https://www.vldb.org/pvldb/vol15/p3058-liakos.pdf

import (
	"bytes"
	"errors"
	"math"
	"math/bits"

	"github.com/dgryski/go-bitstream"
)

type Chimp[T Number] struct {
	storedLeadingZeros  uint64
	storedTrailingZeros uint64
	storedVal           uint64
	size                uint64
	numElements         uint64
	smallInts           bool
	first               bool
}

var threshold uint64 = 6

var leadingRepresentation = [...]uint64{
	0, 0, 0, 0, 0, 0, 0, 0,
	1, 1, 1, 1, 2, 2, 2, 2,
	3, 3, 4, 4, 5, 5, 6, 6,
	7, 7, 7, 7, 7, 7, 7, 7,
	7, 7, 7, 7, 7, 7, 7, 7,
	7, 7, 7, 7, 7, 7, 7, 7,
	7, 7, 7, 7, 7, 7, 7, 7,
	7, 7, 7, 7, 7, 7, 7, 7}

var leadingRepresentationUnpack = [...]uint64{0, 8, 12, 16, 18, 20, 22, 24}

var leadingRound = [...]uint64{
	0, 0, 0, 0, 0, 0, 0, 0,
	8, 8, 8, 8, 12, 12, 12, 12,
	16, 16, 18, 18, 20, 20, 22, 22,
	24, 24, 24, 24, 24, 24, 24, 24,
	24, 24, 24, 24, 24, 24, 24, 24,
	24, 24, 24, 24, 24, 24, 24, 24,
	24, 24, 24, 24, 24, 24, 24, 24,
	24, 24, 24, 24, 24, 24, 24, 24}

func NewChimp[T Number]() *Chimp[T] {
	return &Chimp[T]{
		storedLeadingZeros:  math.MaxInt32,
		storedTrailingZeros: 0,
		storedVal:           0,
		size:                0,
		numElements:         0,
		smallInts:           true,
		first:               true,
	}
}

// Packs the float64 data in the src slice to the dst buffer and returns
// nil if packing was completed successfuly. Otherwise, returns the error.
func (chimp *Chimp[T]) Pack(src []T, dst *bytes.Buffer, op PackOp, opParam T) error {
	switch any(opParam).(type) {
	case int64:
		return chimp.packInt(src, dst, op, opParam)
	case uint64:
		return chimp.packUInt(src, dst, op, opParam)
	case float64:
		return chimp.packFloat(src, dst, op, opParam)
	}

	return errors.New("unsupported type in pack")
}

// Unpacks the float64 data in the src buffer to the dst slice and returns
// number of elements unpacked along with nil error. Otherwise, returns (0,
// error).
func (chimp *Chimp[T]) Unpack(src *bytes.Buffer, dst []T, op PackOp, opParam T) (uint64, error) {
	switch any(opParam).(type) {
	case int64:
		return chimp.unpackInt(src, dst, op, opParam)
	case uint64:
		return chimp.unpackUInt(src, dst, op, opParam)
	case float64:
		return chimp.unpackFloat(src, dst, op, opParam)
	}

	return 0, errors.New("unsupported type in unpack")
}

// Return the size of the packed data
func (chimp *Chimp[T]) PackedSize() uint64 {
	return (chimp.size + 7) / 8
}

// Reutrn the umber of elements in the frame
func (chimp *Chimp[T]) NumElements() uint64 {
	return chimp.numElements
}

//-----------------------------------------------------------------------------
//                              PRIVATE METHODS
//-----------------------------------------------------------------------------

// Packs the float64 data in the src slice to the dst buffer and returns the buffer
// Otherwise, returns (nil, error).
func (chimp *Chimp[T]) packFloat(
	src []T, dst *bytes.Buffer, op PackOp, opParam T) error {

	chimp.storedLeadingZeros = math.MaxInt64
	chimp.storedVal = 0
	chimp.first = true
	chimp.size = 0
	chimp.numElements = 0

	bitStream := bitstream.NewWriter(dst)
	for ndx := range src {
		val := T(src[ndx])
		switch op {
		case NOP:
			chimp.addUIntValue(bitStream, math.Float64bits(float64(val)))
		case Offset:
			chimp.addUIntValue(bitStream, math.Float64bits(float64(val+opParam)))
		case Delta:
			chimp.addUIntValue(bitStream, math.Float64bits(float64(val-opParam)))
			opParam = val
		}
		chimp.numElements++
	}
	bitStream.Flush(false)

	return nil
}

// Packs the uint64 data in the src slice to the dst buffer and returns
// nil if packing was completed successfuly. Otherwise, returns the error.
func (chimp *Chimp[T]) packInt(
	src []T, dst *bytes.Buffer, op PackOp, opParam T) error {

	chimp.storedLeadingZeros = math.MaxInt64
	chimp.storedVal = 0
	chimp.first = true
	chimp.size = 0
	chimp.numElements = 0

	bitStream := bitstream.NewWriter(dst)

	// First pack the sign bits
	for ndx := range src {
		if src[ndx] < 0 {
			bitStream.WriteBit(true)
		} else {
			bitStream.WriteBit(false)
		}
	}
	chimp.size += uint64(len(src))

	for ndx := range src {
		val := src[ndx]
		if val < 0 {
			val = T(int64(val) * -1)
		}
		switch op {
		case NOP:
			uVal := uint64(val)
			if chimp.smallInts {
				uVal = (uVal << 32) | (uVal >> 32)
			}
			chimp.addUIntValue(bitStream, uVal)
		case Offset:
			uVal := uint64(val + opParam)
			if chimp.smallInts {
				uVal = (uVal << 32) | (uVal >> 32)
			}
			chimp.addUIntValue(bitStream, uVal)
		case Delta:
			uVal := uint64(val - opParam)
			if chimp.smallInts {
				uVal = (uVal << 32) | (uVal >> 32)
			}
			chimp.addUIntValue(bitStream, uVal)
			opParam = val
		}
		chimp.numElements++
	}
	bitStream.Flush(false)

	return nil
}

// Packs the uint64 data in the src slice to the dst buffer and returns
// nil if packing was completed successfuly. Otherwise, returns the error.
func (chimp *Chimp[T]) packUInt(
	src []T, dst *bytes.Buffer, op PackOp, opParam T) error {

	chimp.storedLeadingZeros = math.MaxInt64
	chimp.storedVal = 0
	chimp.first = true
	chimp.size = 0
	chimp.numElements = 0

	bitStream := bitstream.NewWriter(dst)
	for ndx := range src {
		val := src[ndx]
		switch op {
		case NOP:
			uVal := uint64(val)
			if chimp.smallInts {
				uVal = (uVal << 32) | (uVal >> 32)
			}
			chimp.addUIntValue(bitStream, uVal)
		case Offset:
			uVal := uint64(val + opParam)
			if chimp.smallInts {
				uVal = (uVal << 32) | (uVal >> 32)
			}
			chimp.addUIntValue(bitStream, uVal)
		case Delta:
			uVal := uint64(val - opParam)
			if chimp.smallInts {
				uVal = (uVal << 32) | (uVal >> 32)
			}
			chimp.addUIntValue(bitStream, uVal)
			opParam = val
		}
		chimp.numElements++
	}
	bitStream.Flush(false)

	return nil
}

// Unpacks the float64 data in the src buffer to the dst slice and returns the
// number of float64 elements that was unpacked. Otherwise, returns (0,
// error).
func (chimp *Chimp[T]) unpackFloat(
	src *bytes.Buffer, dst []T, op PackOp, opParam T) (uint64, error) {

	chimp.storedLeadingZeros = math.MaxInt64
	chimp.storedTrailingZeros = 0
	chimp.storedVal = 0
	chimp.first = true
	chimp.size = 0

	bitStream := bitstream.NewReader(src)

	var readElements uint64 = 0
	for readElements < chimp.numElements {
		chimp.next(bitStream)
		switch op {
		case NOP:
			dst[readElements] = T(math.Float64frombits(chimp.storedVal))
		case Offset:
			dst[readElements] = T(math.Float64frombits(chimp.storedVal)) - opParam
		case Delta:
			dst[readElements] = T(math.Float64frombits(chimp.storedVal)) + opParam
			opParam = dst[readElements]
		}
		readElements++
	}

	return readElements, nil
}

// Unpacks the int64 data in the src buffer to the dst slice and returns the
// number of int64 elements that was unpacked. Otherwise, returns (0,
// error).
func (chimp *Chimp[T]) unpackInt(
	src *bytes.Buffer, dst []T, op PackOp, opParam T) (uint64, error) {

	chimp.storedLeadingZeros = math.MaxInt64
	chimp.storedTrailingZeros = 0
	chimp.storedVal = 0
	chimp.first = true
	chimp.size = 0

	bitStream := bitstream.NewReader(src)

	negInd := make([]int64, chimp.NumElements())
	var ndx uint64 = 0
	for ndx < chimp.NumElements() {
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
	for readElements < chimp.numElements {
		chimp.next(bitStream)
		switch op {
		case NOP:
			uVal := chimp.storedVal
			if chimp.smallInts {
				uVal = (uVal << 32) | (uVal >> 32)
			}
			dst[readElements] = T(uVal)
		case Offset:
			uVal := chimp.storedVal
			if chimp.smallInts {
				uVal = (uVal << 32) | (uVal >> 32)
			}
			dst[readElements] = T(uVal) - opParam

		case Delta:
			uVal := chimp.storedVal
			if chimp.smallInts {
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
func (chimp *Chimp[T]) unpackUInt(
	src *bytes.Buffer, dst []T, op PackOp, opParam T) (uint64, error) {

	chimp.storedLeadingZeros = math.MaxInt64
	chimp.storedTrailingZeros = 0
	chimp.storedVal = 0
	chimp.first = true
	chimp.size = 0

	bitStream := bitstream.NewReader(src)

	var readElements uint64 = 0
	for readElements < chimp.numElements {
		chimp.next(bitStream)
		switch op {
		case NOP:
			uVal := chimp.storedVal
			if chimp.smallInts {
				uVal = (uVal << 32) | (uVal >> 32)
			}
			dst[readElements] = T(uVal)
		case Offset:
			uVal := chimp.storedVal
			if chimp.smallInts {
				uVal = (uVal << 32) | (uVal >> 32)
			}
			dst[readElements] = T(uVal) - opParam
		case Delta:
			uVal := chimp.storedVal
			if chimp.smallInts {
				uVal = (uVal << 32) | (uVal >> 32)
			}
			dst[readElements] = T(uVal) + opParam
			opParam = dst[readElements]
		}
		readElements++
	}

	return readElements, nil
}

func (chimp *Chimp[T]) addUIntValue(bitStream *bitstream.BitWriter, value uint64) {
	if chimp.first {
		chimp.writeFirst(bitStream, value)
	} else {
		chimp.compressValue(bitStream, value)
	}
}

func (chimp *Chimp[T]) writeFirst(bitStream *bitstream.BitWriter, value uint64) {
	chimp.first = false
	chimp.storedVal = value
	bitStream.WriteBits(value, 64)
	chimp.size += 64
}

func (chimp *Chimp[T]) compressValue(bitStream *bitstream.BitWriter, value uint64) {
	var xor uint64 = chimp.storedVal ^ value
	if xor == 0 {
		// Write 0
		bitStream.WriteBits(uint64(0), 2)
		chimp.size += 2
		chimp.storedLeadingZeros = 65
	} else {
		var leadingZeros uint64 = uint64(leadingRound[bits.LeadingZeros64(xor)])
		var trailingZeros uint64 = uint64(bits.TrailingZeros64(xor))

		if trailingZeros > threshold {
			var significantBits uint64 = 64 - leadingZeros - trailingZeros
			bitStream.WriteBits(uint64(1), 2)
			bitStream.WriteBits(leadingRepresentation[leadingZeros], 3)
			bitStream.WriteBits(significantBits, 6)
			bitStream.WriteBits(xor>>trailingZeros, int(significantBits))
			chimp.size += 11 + significantBits
			chimp.storedLeadingZeros = 65 //leadingRepresentation[leadingZeros]
		} else if leadingZeros == chimp.storedLeadingZeros {
			bitStream.WriteBits(uint64(2), 2)
			var significantBits uint64 = 64 - leadingZeros
			bitStream.WriteBits(xor, int(significantBits))
			chimp.size += 2 + significantBits
		} else {
			chimp.storedLeadingZeros = leadingZeros
			var significantBits uint64 = 64 - leadingZeros
			bitStream.WriteBits(uint64(3), 2)
			bitStream.WriteBits(leadingRepresentation[leadingZeros], 3)
			bitStream.WriteBits(xor, int(significantBits))
			chimp.size += 5 + significantBits
		}
	}
	chimp.storedVal = value
}

func (chimp *Chimp[T]) next(bitStream *bitstream.BitReader) error {
	if chimp.first {
		chimp.first = false
		var val, err = bitStream.ReadBits(64)
		if err != nil {
			return err
		}
		chimp.storedVal = val
		return nil
	}
	return chimp.nextValue(bitStream)
}

func (chimp *Chimp[T]) nextValue(bitStream *bitstream.BitReader) error {

	var significantBits uint64 = 0
	var value uint64 = 0

	// Read value
	var flag, err = bitStream.ReadBits(2)
	if err != nil {
		return err
	}

	switch flag {
	case 3:
		// New leading zeros
		var bits, _ = bitStream.ReadBits(3)
		chimp.storedLeadingZeros = leadingRepresentationUnpack[bits]
		significantBits = 64 - chimp.storedLeadingZeros
		if significantBits == 0 {
			significantBits = 64
		}
		value, _ = bitStream.ReadBits(64 - int(chimp.storedLeadingZeros))
		value = chimp.storedVal ^ value
		chimp.storedVal = value
	case 2:
		significantBits = 64 - chimp.storedLeadingZeros
		if significantBits == 0 {
			significantBits = 64
		}
		value, _ = bitStream.ReadBits(64 - int(chimp.storedLeadingZeros))
		value = chimp.storedVal ^ value
		chimp.storedVal = value
	case 1:
		var bits, _ = bitStream.ReadBits(3)
		chimp.storedLeadingZeros = leadingRepresentationUnpack[bits]
		significantBits, _ = bitStream.ReadBits(6)
		if significantBits == 0 {
			significantBits = 64
		}
		chimp.storedTrailingZeros = 64 - significantBits - chimp.storedLeadingZeros
		value, _ = bitStream.ReadBits(64 - int(chimp.storedLeadingZeros+chimp.storedTrailingZeros))
		value <<= chimp.storedTrailingZeros
		value = chimp.storedVal ^ value
		chimp.storedVal = value
	}

	return nil
}
