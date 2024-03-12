package packer

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGorilla_Float64_BasicPack(t *testing.T) {

	a := make([]float64, 10)
	for i := range a {
		a[i] = float64(i)
	}

	gor := NewGorilla[float64]()
	buffer := &bytes.Buffer{}

	err := gor.Pack(a, buffer, NOP, 0.0)
	assert.Nil(t, err)
	assert.NotZero(t, buffer.Len())
}

func TestGorilla_Float64_BasicUnpack(t *testing.T) {

	a := make([]float64, 10)
	res := make([]float64, 10)
	for i := range a {
		a[i] = float64(i)
	}

	gor := NewGorilla[float64]()
	buffer := &bytes.Buffer{}

	err := gor.Pack(a, buffer, NOP, 0.0)

	numElements, err := gor.Unpack(buffer, res, NOP, 0.0)
	assert.Nil(t, err)
	assert.Equal(t, len(a), int(numElements))
}

func TestGorilla_Float64_ValueCheck(t *testing.T) {

	a := make([]float64, 10)
	res := make([]float64, 10)
	for i := range a {
		a[i] = float64(i)
	}

	gor := NewGorilla[float64]()
	buffer := &bytes.Buffer{}

	gor.Pack(a, buffer, NOP, 0.0)
	gor.Unpack(buffer, res, NOP, 0.0)

	for i := range a {
		assert.Equal(t, a[i], res[i])
	}
}

// Tests the memory impact of storing a const (value of 1.0) series of size 10
func TestGorilla_Float64_CompressionCheckForConst(t *testing.T) {

	a := make([]float64, 10)
	for i := range a {
		a[i] = 1.0
	}

	gor := NewGorilla[float64]()
	buffer := &bytes.Buffer{}

	gor.Pack(a, buffer, NOP, 0.0)
	assert.Equal(t, 10, buffer.Len())     // Num bytes
	assert.Equal(t, uint64(73), gor.size) // Num bits
}

// Tests the memory impact of storing a monotonically increasing sequence of
// size 10
func TestGorilla_Float64_CompressionCheckForSequence(t *testing.T) {

	a := make([]float64, 10)
	for i := range a {
		a[i] = float64(i)
	}

	gor := NewGorilla[float64]()
	buffer := &bytes.Buffer{}

	gor.Pack(a, buffer, NOP, 0.0)
	assert.Equal(t, 26, buffer.Len())      // Num bytes
	assert.Equal(t, uint64(195), gor.size) // Num bits
}

// Tests the memory impact of storing a monotonically increasing sequence of
// size 10 with delta operation applied before compression
func TestGorilla_Float64_CompressionCheckForDeltaSequence(t *testing.T) {

	a := make([]float64, 10)
	for i := range a {
		a[i] = float64(i)
	}

	gor := NewGorilla[float64]()
	buffer := &bytes.Buffer{}

	gor.Pack(a, buffer, Delta, 0.0)
	assert.Equal(t, 12, buffer.Len())     // Num bytes
	assert.Equal(t, uint64(94), gor.size) // Num bits

	res := make([]float64, 10)
	gor.Unpack(buffer, res, Delta, 0.0)

	for i := range a {
		assert.Equal(t, a[i], res[i])
	}
}

// Tests the memory impact of storing a monotonically increasing sequence of
// size 10 with offset operation applied before compression
func TestGorilla_Float64_CompressionCheckForOffsetSequence(t *testing.T) {

	a := make([]float64, 10)
	for i := range a {
		a[i] = 9 + float64(i)
	}

	gor := NewGorilla[float64]()
	buffer := &bytes.Buffer{}

	gor.Pack(a, buffer, Offset, -9.0)
	assert.Equal(t, 26, buffer.Len())      // Num bytes
	assert.Equal(t, uint64(195), gor.size) // Num bits

	res := make([]float64, 10)
	gor.Unpack(buffer, res, Offset, -9.0)

	for i := range a {
		assert.Equal(t, a[i], res[i])
	}
}

// Tests the memory impact of storing 10 elements that ping pongs between
// two values
func TestGorilla_Float64_CompressionCheckForPingPongSequence(t *testing.T) {

	a := make([]float64, 10)
	for i := range a {
		a[i] = float64(i % 2)
	}

	gor := NewGorilla[float64]()
	buffer := &bytes.Buffer{}

	gor.Pack(a, buffer, NOP, 0.0)
	assert.Equal(t, 23, buffer.Len())      // Num bytes
	assert.Equal(t, uint64(174), gor.size) // Num bits
}

// Tests the memory impact of storing 1 million large value sequence.
func TestGorilla_Float64_CompressionCheckForRandomLargeValues(t *testing.T) {

	a := make([]float64, 1000000)
	for i := range a {
		a[i] = float64(100000 + (i % 1000000))
	}

	gor := NewGorilla[float64]()
	buffer := &bytes.Buffer{}

	gor.Pack(a, buffer, NOP, 0.0)
	var threshold int = 2400000
	assert.LessOrEqual(t, buffer.Len(), threshold) // Num bytes
}

// Benchmark testing for packing. A single iteration will pack 1 million
// constant float64 (1999.9999). So a single op in the "ns/op" refers to
// handling 1 million float64s
func BenchmarkGorillaFor_Float64_PackingConst(t *testing.B) {

	a := make([]float64, 1000000)
	for i := range a {
		a[i] = 1999.9999
	}

	t.ResetTimer()
	for l := 0; l < t.N; l++ {
		gor := NewGorilla[float64]()
		buffer := &bytes.Buffer{}

		t.StartTimer()
		gor.Pack(a, buffer, NOP, 0.0)
		t.StopTimer()
	}
}

// Benchmark testing for unpacking. A single iteration will pack 1 million
// constant float64 (1999.9999). So a single op in the "ns/op" refers to
// handling 1 million float64s
func BenchmarkGorillaFor_Float64_UnpackingConst(t *testing.B) {

	a := make([]float64, 1000000)
	res := make([]float64, 1000000)
	for i := range a {
		a[i] = 1999.9999
	}

	t.ResetTimer()
	for l := 0; l < t.N; l++ {
		gor := NewGorilla[float64]()
		buffer := &bytes.Buffer{}
		gor.Pack(a, buffer, NOP, 0.0)

		t.StartTimer()
		gor.Unpack(buffer, res, NOP, 0.0)
		t.StopTimer()
	}
}

// Benchmark testing for packing. A single iteration will pack 1 million
// floats that are monotonically increasing by 1.0. So a single op in the
// "ns/op" refers to handling 1 million float64s
func BenchmarkGorillaFor_Float64_PackingSequence(t *testing.B) {

	a := make([]float64, 1000000)
	for i := range a {
		a[i] = float64(i) + 10000
	}

	t.ResetTimer()
	for l := 0; l < t.N; l++ {
		gor := NewGorilla[float64]()
		buffer := &bytes.Buffer{}

		t.StartTimer()
		gor.Pack(a, buffer, NOP, 0.0)
		t.StopTimer()
	}
}

// Benchmark testing for unpacking. A single iteration will unpack 1 million
// floats that are monotonically increasing by 1.0. So a single op in the
// "ns/op" refers to handling 1 million float64s
func BenchmarkGorillaFor_Float64_UnpackingSequence(t *testing.B) {

	a := make([]float64, 1000000)
	res := make([]float64, 1000000)
	for i := range a {
		a[i] = float64(i) + 10000
	}

	t.ResetTimer()
	for l := 0; l < t.N; l++ {
		gor := NewGorilla[float64]()
		buffer := &bytes.Buffer{}
		gor.Pack(a, buffer, NOP, 0.0)

		t.StartTimer()
		gor.Unpack(buffer, res, NOP, 0.0)
		t.StopTimer()
	}
}

func TestGorilla_Int64_BasicPack(t *testing.T) {

	a := make([]int64, 10)
	for i := range a {
		a[i] = int64(i)
	}

	gor := NewGorilla[int64]()
	buffer := &bytes.Buffer{}

	err := gor.Pack(a, buffer, NOP, 0.0)
	assert.Nil(t, err)
	assert.NotZero(t, buffer.Len())
}

func TestGorilla_Int64_BasicUnpack(t *testing.T) {

	a := make([]int64, 10)
	res := make([]int64, 10)
	for i := range a {
		a[i] = int64(i)
	}

	gor := NewGorilla[int64]()
	buffer := &bytes.Buffer{}

	err := gor.Pack(a, buffer, NOP, 0.0)

	numElements, err := gor.Unpack(buffer, res, NOP, 0.0)
	assert.Nil(t, err)
	assert.Equal(t, len(a), int(numElements))
}

func TestGorilla_Int64_ValueCheck(t *testing.T) {

	a := make([]int64, 10)
	res := make([]int64, 10)
	for i := range a {
		a[i] = int64(i)
	}

	gor := NewGorilla[int64]()
	buffer := &bytes.Buffer{}

	gor.Pack(a, buffer, NOP, 0)
	gor.Unpack(buffer, res, NOP, 0)

	for i := range a {
		assert.Equal(t, a[i], res[i])
	}
}

// Tests the memory impact of storing a const (value of 1.0) series of size 10
func TestGorilla_Int64_CompressionCheckForConst(t *testing.T) {

	a := make([]int64, 10)
	for i := range a {
		a[i] = 1.0
	}

	gor := NewGorilla[int64]()
	buffer := &bytes.Buffer{}

	gor.Pack(a, buffer, NOP, 0.0)
	assert.Equal(t, 11, buffer.Len())     // Num bytes
	assert.Equal(t, uint64(83), gor.size) // Num bits
}

// Tests the memory impact of storing a monotonically increasing sequence of
// size 10
func TestGorilla_Int64_CompressionCheckForSequence(t *testing.T) {

	a := make([]int64, 10)
	for i := range a {
		a[i] = int64(i)
	}

	gor := NewGorilla[int64]()
	buffer := &bytes.Buffer{}

	gor.Pack(a, buffer, NOP, 0.0)
	assert.Equal(t, 21, buffer.Len())      // Num bytes
	assert.Equal(t, uint64(152), gor.size) // Num bits
}

// Tests the memory impact of storing a monotonically increasing sequence of
// size 10 with delta operation applied before compression
func TestGorilla_Int64_CompressionCheckForDeltaSequence(t *testing.T) {

	a := make([]int64, 10)
	for i := range a {
		a[i] = int64(i)
	}

	gor := NewGorilla[int64]()
	buffer := &bytes.Buffer{}

	gor.Pack(a, buffer, Delta, 0.0)
	assert.Equal(t, 12, buffer.Len())     // Num bytes
	assert.Equal(t, uint64(95), gor.size) // Num bits

	res := make([]int64, 10)
	gor.Unpack(buffer, res, Delta, 0.0)

	for i := range a {
		assert.Equal(t, a[i], res[i])
	}
}

// Tests the memory impact of storing a monotonically increasing sequence of
// size 10 with offset operation applied before compression
func TestGorilla_Int64_CompressionCheckForOffsetSequence(t *testing.T) {

	a := make([]int64, 10)
	for i := range a {
		a[i] = 9 + int64(i)
	}

	gor := NewGorilla[int64]()
	buffer := &bytes.Buffer{}

	gor.Pack(a, buffer, Offset, -9.0)
	assert.Equal(t, 21, buffer.Len())      // Num bytes
	assert.Equal(t, uint64(152), gor.size) // Num bits

	res := make([]int64, 10)
	gor.Unpack(buffer, res, Offset, -9.0)

	for i := range a {
		assert.Equal(t, a[i], res[i])
	}
}

// Tests the memory impact of storing 10 elements that ping pongs between
// two values
func TestGorilla_Int64_CompressionCheckForPingPongSequence(t *testing.T) {

	a := make([]int64, 10)
	for i := range a {
		a[i] = int64(i % 2)
	}

	gor := NewGorilla[int64]()
	buffer := &bytes.Buffer{}

	gor.Pack(a, buffer, NOP, 0.0)
	assert.Equal(t, 14, buffer.Len())      // Num bytes
	assert.Equal(t, uint64(103), gor.size) // Num bits
}

// Tests the memory impact of storing 1 million large value sequence.
func TestGorilla_Int64_CompressionCheckForLargeValueSequence(t *testing.T) {

	a := make([]int64, 1000000)
	for i := range a {
		a[i] = int64(100000 + (i % 1000000))
	}

	gor := NewGorilla[int64]()
	buffer := &bytes.Buffer{}

	gor.Pack(a, buffer, NOP, 0.0)
	var threshold int = 2800000
	assert.LessOrEqual(t, buffer.Len(), threshold) // Num bytes
}

// Benchmark testing for packing. A single iteration will pack 1 million
// constant int64 (1999). So a single op in the "ns/op" refers to
// handling 1 million int64s
func BenchmarkGorillaFor_Int64_PackingConst(t *testing.B) {

	a := make([]int64, 1000000)
	for i := range a {
		a[i] = 1999
	}

	t.ResetTimer()
	for l := 0; l < t.N; l++ {
		gor := NewGorilla[int64]()
		buffer := &bytes.Buffer{}

		t.StartTimer()
		gor.Pack(a, buffer, NOP, 0.0)
		t.StopTimer()
	}
}

// Benchmark testing for unpacking. A single iteration will pack 1 million
// constant int64 (1999). So a single op in the "ns/op" refers to
// handling 1 million int64s
func BenchmarkGorillaFor_Int64_UnpackingConst(t *testing.B) {

	a := make([]int64, 1000000)
	res := make([]int64, 1000000)
	for i := range a {
		a[i] = 1999
	}

	t.ResetTimer()
	for l := 0; l < t.N; l++ {
		gor := NewGorilla[int64]()
		buffer := &bytes.Buffer{}
		gor.Pack(a, buffer, NOP, 0.0)

		t.StartTimer()
		gor.Unpack(buffer, res, NOP, 0.0)
		t.StopTimer()
	}
}

// Benchmark testing for packing. A single iteration will pack 1 million
// int64s that are monotonically increasing by 1.0. So a single op in the
// "ns/op" refers to handling 1 million int64s
func BenchmarkGorillaFor_Int64_PackingSequence(t *testing.B) {

	a := make([]int64, 1000000)
	for i := range a {
		a[i] = int64(i) + 10000
	}

	t.ResetTimer()
	for l := 0; l < t.N; l++ {
		gor := NewGorilla[int64]()
		buffer := &bytes.Buffer{}

		t.StartTimer()
		gor.Pack(a, buffer, NOP, 0.0)
		t.StopTimer()
	}
}

// Benchmark testing for unpacking. A single iteration will unpack 1 million
// int64s that are monotonically increasing by 1.0. So a single op in the
// "ns/op" refers to handling 1 million int64s
func BenchmarkGorillaFor_Int64_UnpackingSequence(t *testing.B) {

	a := make([]int64, 1000000)
	res := make([]int64, 1000000)
	for i := range a {
		a[i] = int64(i) + 10000
	}

	t.ResetTimer()
	for l := 0; l < t.N; l++ {
		gor := NewGorilla[int64]()
		buffer := &bytes.Buffer{}
		gor.Pack(a, buffer, NOP, 0.0)

		t.StartTimer()
		gor.Unpack(buffer, res, NOP, 0.0)
		t.StopTimer()
	}
}

func TestGorilla_UInt64_BasicPack(t *testing.T) {

	a := make([]uint64, 10)
	for i := range a {
		a[i] = uint64(i)
	}

	gor := NewGorilla[uint64]()
	buffer := &bytes.Buffer{}

	err := gor.Pack(a, buffer, NOP, 0.0)
	assert.Nil(t, err)
	assert.NotZero(t, buffer.Len())
}

func TestGorilla_UInt64_BasicUnpack(t *testing.T) {

	a := make([]uint64, 10)
	res := make([]uint64, 10)
	for i := range a {
		a[i] = uint64(i)
	}

	gor := NewGorilla[uint64]()
	buffer := &bytes.Buffer{}

	err := gor.Pack(a, buffer, NOP, 0.0)

	numElements, err := gor.Unpack(buffer, res, NOP, 0.0)
	assert.Nil(t, err)
	assert.Equal(t, len(a), int(numElements))
}

func TestGorilla_UInt64_ValueCheck(t *testing.T) {

	a := make([]uint64, 10)
	res := make([]uint64, 10)
	for i := range a {
		a[i] = uint64(i)
	}

	gor := NewGorilla[uint64]()
	buffer := &bytes.Buffer{}

	gor.Pack(a, buffer, NOP, 0)
	gor.Unpack(buffer, res, NOP, 0)

	for i := range a {
		assert.Equal(t, a[i], res[i])
	}
}

// Tests the memory impact of storing a const (value of 1.0) series of size 10
func TestGorilla_Unt64_CompressionCheckForConst(t *testing.T) {

	a := make([]uint64, 10)
	for i := range a {
		a[i] = 1.0
	}

	gor := NewGorilla[uint64]()
	buffer := &bytes.Buffer{}

	gor.Pack(a, buffer, NOP, 0.0)
	assert.Equal(t, 10, buffer.Len())     // Num bytes
	assert.Equal(t, uint64(73), gor.size) // Num bits
}

// Tests the memory impact of storing a monotonically increasing sequence of
// size 10
func TestGorilla_UInt64_CompressionCheckForSequence(t *testing.T) {

	a := make([]uint64, 10)
	for i := range a {
		a[i] = uint64(i)
	}

	gor := NewGorilla[uint64]()
	buffer := &bytes.Buffer{}

	gor.Pack(a, buffer, NOP, 0.0)
	assert.Equal(t, 19, buffer.Len())      // Num bytes
	assert.Equal(t, uint64(142), gor.size) // Num bits
}

// Tests the memory impact of storing a monotonically increasing sequence of
// size 10 with delta operation applied before compression
func TestGorilla_UInt64_CompressionCheckForDeltaSequence(t *testing.T) {

	a := make([]uint64, 10)
	for i := range a {
		a[i] = uint64(i)
	}

	gor := NewGorilla[uint64]()
	buffer := &bytes.Buffer{}

	gor.Pack(a, buffer, Delta, 0.0)
	assert.Equal(t, 11, buffer.Len())     // Num bytes
	assert.Equal(t, uint64(85), gor.size) // Num bits

	res := make([]uint64, 10)
	gor.Unpack(buffer, res, Delta, 0.0)

	for i := range a {
		assert.Equal(t, a[i], res[i])
	}
}

// Tests the memory impact of storing a monotonically increasing sequence of
// size 10 with offset operation applied before compression
func TestGorilla_UInt64_CompressionCheckForOffsetSequence(t *testing.T) {

	a := make([]uint64, 10)
	for i := range a {
		a[i] = 9 + uint64(i)
	}

	gor := NewGorilla[uint64]()
	buffer := &bytes.Buffer{}

	gor.Pack(a, buffer, Offset, 9)
	assert.Equal(t, 18, buffer.Len())      // Num bytes
	assert.Equal(t, uint64(135), gor.size) // Num bits

	res := make([]uint64, 10)
	gor.Unpack(buffer, res, Offset, 9.0)

	for i := range a {
		assert.Equal(t, a[i], res[i])
	}
}

// Tests the memory impact of storing 10 elements that ping pongs between
// two values
func TestGorilla_UInt64_CompressionCheckForPingPongSequence(t *testing.T) {

	a := make([]uint64, 10)
	for i := range a {
		a[i] = uint64(i % 2)
	}

	gor := NewGorilla[uint64]()
	buffer := &bytes.Buffer{}

	gor.Pack(a, buffer, NOP, 0.0)
	assert.Equal(t, 13, buffer.Len())     // Num bytes
	assert.Equal(t, uint64(93), gor.size) // Num bits
}

// Tests the memory impact of storing 1 million large value sequence.
func TestGorilla_UInt64_CompressionCheckForLargeValueSequence(t *testing.T) {

	a := make([]uint64, 1000000)
	for i := range a {
		a[i] = uint64(100000 + (i % 1000000))
	}

	gor := NewGorilla[uint64]()
	buffer := &bytes.Buffer{}

	gor.Pack(a, buffer, NOP, 0.0)
	var threshold int = 2700000
	assert.LessOrEqual(t, buffer.Len(), threshold) // Num bytes
}

// Benchmark testing for packing. A single iteration will pack 1 million
// constant uint64 (1999). So a single op in the "ns/op" refers to
// handling 1 million uint64s
func BenchmarkGorillaFor_UInt64_PackingConst(t *testing.B) {

	a := make([]uint64, 1000000)
	for i := range a {
		a[i] = 1999
	}

	t.ResetTimer()
	for l := 0; l < t.N; l++ {
		gor := NewGorilla[uint64]()
		buffer := &bytes.Buffer{}

		t.StartTimer()
		gor.Pack(a, buffer, NOP, 0.0)
		t.StopTimer()
	}
}

// Benchmark testing for unpacking. A single iteration will pack 1 million
// constant uint64 (1999). So a single op in the "ns/op" refers to
// handling 1 million uint64s
func BenchmarkGorillaFor_UInt64_UnpackingConst(t *testing.B) {

	a := make([]uint64, 1000000)
	res := make([]uint64, 1000000)
	for i := range a {
		a[i] = 1999
	}

	t.ResetTimer()
	for l := 0; l < t.N; l++ {
		gor := NewGorilla[uint64]()
		buffer := &bytes.Buffer{}
		gor.Pack(a, buffer, NOP, 0.0)

		t.StartTimer()
		gor.Unpack(buffer, res, NOP, 0.0)
		t.StopTimer()
	}
}

// Benchmark testing for packing. A single iteration will pack 1 million
// uint64s that are monotonically increasing by 1.0. So a single op in the
// "ns/op" refers to handling 1 million uint64s
func BenchmarkGorillaFor_UInt64_PackingSequence(t *testing.B) {

	a := make([]uint64, 1000000)
	for i := range a {
		a[i] = uint64(i) + 10000
	}

	t.ResetTimer()
	for l := 0; l < t.N; l++ {
		gor := NewGorilla[uint64]()
		buffer := &bytes.Buffer{}

		t.StartTimer()
		gor.Pack(a, buffer, NOP, 0.0)
		t.StopTimer()
	}
}

// Benchmark testing for unpacking. A single iteration will unpack 1 million
// uint64s that are monotonically increasing by 1.0. So a single op in the
// "ns/op" refers to handling 1 million uint64s
func BenchmarkGorillaFor_Unt64_UnpackingSequence(t *testing.B) {

	a := make([]uint64, 1000000)
	res := make([]uint64, 1000000)
	for i := range a {
		a[i] = uint64(i) + 10000
	}

	t.ResetTimer()
	for l := 0; l < t.N; l++ {
		gor := NewGorilla[uint64]()
		buffer := &bytes.Buffer{}
		gor.Pack(a, buffer, NOP, 0.0)

		t.StartTimer()
		gor.Unpack(buffer, res, NOP, 0.0)
		t.StopTimer()
	}
}

func BenchmarkGorillaFor_StockPrice(t *testing.B) {

	prices := ReadStockPriceFile()
	if prices == nil {
		assert.FailNow(t, "Failed to read stock price data")
	}

	gor := NewGorilla[float64]()
	buffer := &bytes.Buffer{}
	buffer.Grow(50000000)
	t.StartTimer()
	gor.Pack(prices, buffer, NOP, 0.0)
	t.StopTimer()
	println("Num bytes: ", buffer.Len())
}
