package packer

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestChimp_Float64_BasicPack(t *testing.T) {

	a := make([]float64, 10)
	for i := range a {
		a[i] = float64(i)
	}

	chimp := NewChimp[float64]()
	buffer := &bytes.Buffer{}

	err := chimp.Pack(a, buffer, NOP, 0.0)
	assert.Nil(t, err)
	assert.NotZero(t, buffer.Len())
}

func TestChimp_Float64_BasicUnpack(t *testing.T) {

	a := make([]float64, 10)
	res := make([]float64, 10)
	for i := range a {
		a[i] = float64(i)
	}

	chimp := NewChimp[float64]()
	buffer := &bytes.Buffer{}

	err := chimp.Pack(a, buffer, NOP, 0.0)

	numElements, err := chimp.Unpack(buffer, res, NOP, 0.0)
	assert.Nil(t, err)
	assert.Equal(t, len(a), int(numElements))
}

func TestChimp_Float64_ValueCheck(t *testing.T) {

	a := make([]float64, 10)
	res := make([]float64, 10)
	for i := range a {
		a[i] = float64(i)
	}

	chimp := NewChimp[float64]()
	buffer := &bytes.Buffer{}

	chimp.Pack(a, buffer, NOP, 0.0)
	chimp.Unpack(buffer, res, NOP, 0.0)

	for i := range a {
		assert.Equal(t, a[i], res[i])
	}
}

// Tests the memory impact of storing a const (value of 1.0) series of size 10
func TestChimp_Float64_CompressionCheckForConst(t *testing.T) {

	a := make([]float64, 10)
	for i := range a {
		a[i] = 1.0
	}

	chimp := NewChimp[float64]()
	buffer := &bytes.Buffer{}

	chimp.Pack(a, buffer, NOP, 0.0)
	assert.Equal(t, 11, buffer.Len())       // Num bytes
	assert.Equal(t, uint64(82), chimp.size) // Num bits
}

// Tests the memory impact of storing a monotonically increasing sequence of
// size 10
func TestChimp_Float64_CompressionCheckForSequence(t *testing.T) {

	a := make([]float64, 10)
	for i := range a {
		a[i] = float64(i)
	}

	chimp := NewChimp[float64]()
	buffer := &bytes.Buffer{}

	chimp.Pack(a, buffer, NOP, 0.0)
	assert.Equal(t, 26, buffer.Len())        // Num bytes
	assert.Equal(t, uint64(208), chimp.size) // Num bits
}

// Tests the memory impact of storing a monotonically increasing sequence of
// size 10 with delta operation applied before compression
func TestChimp_Float64_CompressionCheckForDeltaSequence(t *testing.T) {

	a := make([]float64, 10)
	for i := range a {
		a[i] = float64(i)
	}

	chimp := NewChimp[float64]()
	buffer := &bytes.Buffer{}

	chimp.Pack(a, buffer, Delta, 0.0)
	assert.Equal(t, 13, buffer.Len())        // Num bytes
	assert.Equal(t, uint64(103), chimp.size) // Num bits

	res := make([]float64, 10)
	chimp.Unpack(buffer, res, Delta, 0.0)

	for i := range a {
		assert.Equal(t, a[i], res[i])
	}
}

// Tests the memory impact of storing a monotonically increasing sequence of
// size 10 with offset operation applied before compression
func TestChimp_Float64_CompressionCheckForOffsetSequence(t *testing.T) {

	a := make([]float64, 10)
	for i := range a {
		a[i] = 9 + float64(i)
	}

	chimp := NewChimp[float64]()
	buffer := &bytes.Buffer{}

	chimp.Pack(a, buffer, Offset, -9.0)
	assert.Equal(t, 26, buffer.Len())        // Num bytes
	assert.Equal(t, uint64(208), chimp.size) // Num bits

	res := make([]float64, 10)
	chimp.Unpack(buffer, res, Offset, -9.0)

	for i := range a {
		assert.Equal(t, a[i], res[i])
	}
}

// Tests the memory impact of storing 10 elements that ping pongs between
// two values
func TestChimp_Float64_CompressionCheckForPingPongSequence(t *testing.T) {

	a := make([]float64, 10)
	for i := range a {
		a[i] = float64(i % 2)
	}

	chimp := NewChimp[float64]()
	buffer := &bytes.Buffer{}

	chimp.Pack(a, buffer, NOP, 0.0)
	assert.Equal(t, 34, buffer.Len())        // Num bytes
	assert.Equal(t, uint64(271), chimp.size) // Num bits
}

// Tests the memory impact of storing 1 million large value sequence.
func TestChimp_Float64_CompressionCheckForLargeValueSequence(t *testing.T) {

	a := make([]float64, 1000000)
	for i := range a {
		a[i] = float64(100000 + (i % 1000000))
	}

	chimp := NewChimp[float64]()
	buffer := &bytes.Buffer{}

	chimp.Pack(a, buffer, NOP, 0.0)
	var threshold int = 2200000
	assert.LessOrEqual(t, buffer.Len(), threshold) // Num bytes
}

// Benchmark testing for packing. A single iteration will pack 1 million
// constant float64 (1999.9999). So a single op in the "ns/op" refers to
// handling 1 million float64s
func BenchmarkChimpFor_Float64_PackingConst(t *testing.B) {

	a := make([]float64, 1000000)
	for i := range a {
		a[i] = 1999.9999
	}

	t.ResetTimer()
	for l := 0; l < t.N; l++ {
		chimp := NewChimp[float64]()
		buffer := &bytes.Buffer{}

		t.StartTimer()
		chimp.Pack(a, buffer, NOP, 0.0)
		t.StopTimer()
	}
}

// Benchmark testing for unpacking. A single iteration will pack 1 million
// constant float64 (1999.9999). So a single op in the "ns/op" refers to
// handling 1 million float64s
func BenchmarkChimpFor_Float64_UnpackingConst(t *testing.B) {

	a := make([]float64, 1000000)
	res := make([]float64, 1000000)
	for i := range a {
		a[i] = 1999.9999
	}

	t.ResetTimer()
	for l := 0; l < t.N; l++ {
		chimp := NewChimp[float64]()
		buffer := &bytes.Buffer{}
		chimp.Pack(a, buffer, NOP, 0.0)

		t.StartTimer()
		chimp.Unpack(buffer, res, NOP, 0.0)
		t.StopTimer()
	}
}

// Benchmark testing for packing. A single iteration will pack 1 million
// floats that are monotonically increasing by 1.0. So a single op in the
// "ns/op" refers to handling 1 million float64s
func BenchmarkChimpFor_Float64_PackingSequence(t *testing.B) {

	a := make([]float64, 1000000)
	for i := range a {
		a[i] = float64(i) + 10000
	}

	t.ResetTimer()
	for l := 0; l < t.N; l++ {
		chimp := NewChimp[float64]()
		buffer := &bytes.Buffer{}

		t.StartTimer()
		chimp.Pack(a, buffer, NOP, 0.0)
		t.StopTimer()
	}
}

// Benchmark testing for unpacking. A single iteration will unpack 1 million
// floats that are monotonically increasing by 1.0. So a single op in the
// "ns/op" refers to handling 1 million float64s
func BenchmarkChimpFor_Float64_UnpackingSequence(t *testing.B) {

	a := make([]float64, 1000000)
	res := make([]float64, 1000000)
	for i := range a {
		a[i] = float64(i) + 10000
	}

	t.ResetTimer()
	for l := 0; l < t.N; l++ {
		chimp := NewChimp[float64]()
		buffer := &bytes.Buffer{}
		chimp.Pack(a, buffer, NOP, 0.0)

		t.StartTimer()
		chimp.Unpack(buffer, res, NOP, 0.0)
		t.StopTimer()
	}
}

func TestChimp_Int64_BasicPack(t *testing.T) {

	a := make([]int64, 10)
	for i := range a {
		a[i] = int64(i)
	}

	chimp := NewChimp[int64]()
	buffer := &bytes.Buffer{}

	err := chimp.Pack(a, buffer, NOP, 0.0)
	assert.Nil(t, err)
	assert.NotZero(t, buffer.Len())
}

func TestChimp_Int64_BasicUnpack(t *testing.T) {

	a := make([]int64, 10)
	res := make([]int64, 10)
	for i := range a {
		a[i] = int64(i)
	}

	chimp := NewChimp[int64]()
	buffer := &bytes.Buffer{}

	err := chimp.Pack(a, buffer, NOP, 0.0)

	numElements, err := chimp.Unpack(buffer, res, NOP, 0.0)
	assert.Nil(t, err)
	assert.Equal(t, len(a), int(numElements))
}

func TestChimp_Int64_ValueCheck(t *testing.T) {

	a := make([]int64, 10)
	res := make([]int64, 10)
	for i := range a {
		a[i] = int64(i)
	}

	chimp := NewChimp[int64]()
	buffer := &bytes.Buffer{}

	chimp.Pack(a, buffer, NOP, 0)
	chimp.Unpack(buffer, res, NOP, 0)

	for i := range a {
		assert.Equal(t, a[i], res[i])
	}
}

// Tests the memory impact of storing a const (value of 1.0) series of size 10
func TestChimp_Int64_CompressionCheckForConst(t *testing.T) {

	a := make([]int64, 10)
	for i := range a {
		a[i] = 1.0
	}

	chimp := NewChimp[int64]()
	buffer := &bytes.Buffer{}

	chimp.Pack(a, buffer, NOP, 0.0)
	assert.Equal(t, 12, buffer.Len())       // Num bytes
	assert.Equal(t, uint64(92), chimp.size) // Num bits
}

// Tests the memory impact of storing a monotonically increasing sequence of
// size 10
func TestChimp_Int64_CompressionCheckForSequence(t *testing.T) {

	a := make([]int64, 10)
	for i := range a {
		a[i] = int64(i)
	}

	chimp := NewChimp[int64]()
	buffer := &bytes.Buffer{}

	chimp.Pack(a, buffer, NOP, 0.0)
	assert.Equal(t, 31, buffer.Len())        // Num bytes
	assert.Equal(t, uint64(245), chimp.size) // Num bits
}

// Tests the memory impact of storing a monotonically increasing sequence of
// size 10 with delta operation applied before compression
func TestChimp_Int64_CompressionCheckForDeltaSequence(t *testing.T) {

	a := make([]int64, 10)
	for i := range a {
		a[i] = int64(i)
	}

	chimp := NewChimp[int64]()
	buffer := &bytes.Buffer{}

	chimp.Pack(a, buffer, Delta, 0.0)
	assert.Equal(t, 14, buffer.Len())        // Num bytes
	assert.Equal(t, uint64(109), chimp.size) // Num bits

	res := make([]int64, 10)
	chimp.Unpack(buffer, res, Delta, 0.0)

	for i := range a {
		assert.Equal(t, a[i], res[i])
	}
}

// Tests the memory impact of storing a monotonically increasing sequence of
// size 10 with offset operation applied before compression
func TestChimp_Int64_CompressionCheckForOffsetSequence(t *testing.T) {

	a := make([]int64, 10)
	for i := range a {
		a[i] = 9 + int64(i)
	}

	chimp := NewChimp[int64]()
	buffer := &bytes.Buffer{}

	chimp.Pack(a, buffer, Offset, -9.0)
	assert.Equal(t, 31, buffer.Len())        // Num bytes
	assert.Equal(t, uint64(245), chimp.size) // Num bits

	res := make([]int64, 10)
	chimp.Unpack(buffer, res, Offset, -9.0)

	for i := range a {
		assert.Equal(t, a[i], res[i])
	}
}

// Tests the memory impact of storing 10 elements that ping pongs between
// two values
func TestChimp_Int64_CompressionCheckForPingPongSequence(t *testing.T) {

	a := make([]int64, 10)
	for i := range a {
		a[i] = int64(i % 2)
	}

	chimp := NewChimp[int64]()
	buffer := &bytes.Buffer{}

	chimp.Pack(a, buffer, NOP, 0.0)
	assert.Equal(t, 31, buffer.Len())        // Num bytes
	assert.Equal(t, uint64(245), chimp.size) // Num bits
}

// Tests the memory impact of storing 1 million large value sequence.
func TestChimp_Int64_CompressionCheckForLargeValueSequence(t *testing.T) {

	a := make([]int64, 1000000)
	for i := range a {
		a[i] = int64(100000 + (i % 1000000))
	}

	chimp := NewChimp[int64]()
	buffer := &bytes.Buffer{}

	chimp.Pack(a, buffer, NOP, 0.0)
	var threshold int = 2600000
	assert.LessOrEqual(t, buffer.Len(), threshold) // Num bytes
}

// Benchmark testing for packing. A single iteration will pack 1 million
// constant int64 (1999). So a single op in the "ns/op" refers to
// handling 1 million int64s
func BenchmarkChimpFor_Int64_PackingConst(t *testing.B) {

	a := make([]int64, 1000000)
	for i := range a {
		a[i] = 1999
	}

	t.ResetTimer()
	for l := 0; l < t.N; l++ {
		chimp := NewChimp[int64]()
		buffer := &bytes.Buffer{}

		t.StartTimer()
		chimp.Pack(a, buffer, NOP, 0.0)
		t.StopTimer()
	}
}

// Benchmark testing for unpacking. A single iteration will pack 1 million
// constant int64 (1999). So a single op in the "ns/op" refers to
// handling 1 million int64s
func BenchmarkChimpFor_Int64_UnpackingConst(t *testing.B) {

	a := make([]int64, 1000000)
	res := make([]int64, 1000000)
	for i := range a {
		a[i] = 1999
	}

	t.ResetTimer()
	for l := 0; l < t.N; l++ {
		chimp := NewChimp[int64]()
		buffer := &bytes.Buffer{}
		chimp.Pack(a, buffer, NOP, 0.0)

		t.StartTimer()
		chimp.Unpack(buffer, res, NOP, 0.0)
		t.StopTimer()
	}
}

// Benchmark testing for packing. A single iteration will pack 1 million
// int64s that are monotonically increasing by 1.0. So a single op in the
// "ns/op" refers to handling 1 million int64s
func BenchmarkChimpFor_Int64_PackingSequence(t *testing.B) {

	a := make([]int64, 1000000)
	for i := range a {
		a[i] = int64(i) + 10000
	}

	t.ResetTimer()
	for l := 0; l < t.N; l++ {
		chimp := NewChimp[int64]()
		buffer := &bytes.Buffer{}

		t.StartTimer()
		chimp.Pack(a, buffer, NOP, 0.0)
		t.StopTimer()
	}
}

// Benchmark testing for unpacking. A single iteration will unpack 1 million
// int64s that are monotonically increasing by 1.0. So a single op in the
// "ns/op" refers to handling 1 million int64s
func BenchmarkChimpFor_Int64_UnpackingSequence(t *testing.B) {

	a := make([]int64, 1000000)
	res := make([]int64, 1000000)
	for i := range a {
		a[i] = int64(i) + 10000
	}

	t.ResetTimer()
	for l := 0; l < t.N; l++ {
		chimp := NewChimp[int64]()
		buffer := &bytes.Buffer{}
		chimp.Pack(a, buffer, NOP, 0.0)

		t.StartTimer()
		chimp.Unpack(buffer, res, NOP, 0.0)
		t.StopTimer()
	}
}

func TestChimp_UInt64_BasicPack(t *testing.T) {

	a := make([]uint64, 10)
	for i := range a {
		a[i] = uint64(i)
	}

	chimp := NewChimp[uint64]()
	buffer := &bytes.Buffer{}

	err := chimp.Pack(a, buffer, NOP, 0.0)
	assert.Nil(t, err)
	assert.NotZero(t, buffer.Len())
}

func TestChimp_UInt64_BasicUnpack(t *testing.T) {

	a := make([]uint64, 10)
	res := make([]uint64, 10)
	for i := range a {
		a[i] = uint64(i)
	}

	chimp := NewChimp[uint64]()
	buffer := &bytes.Buffer{}

	err := chimp.Pack(a, buffer, NOP, 0.0)

	numElements, err := chimp.Unpack(buffer, res, NOP, 0.0)
	assert.Nil(t, err)
	assert.Equal(t, len(a), int(numElements))
}

func TestChimp_UInt64_ValueCheck(t *testing.T) {

	a := make([]uint64, 10)
	res := make([]uint64, 10)
	for i := range a {
		a[i] = uint64(i)
	}

	chimp := NewChimp[uint64]()
	buffer := &bytes.Buffer{}

	chimp.Pack(a, buffer, NOP, 0)
	chimp.Unpack(buffer, res, NOP, 0)

	for i := range a {
		assert.Equal(t, a[i], res[i])
	}
}

// Tests the memory impact of storing a const (value of 1.0) series of size 10
func TestChimp_Unt64_CompressionCheckForConst(t *testing.T) {

	a := make([]uint64, 10)
	for i := range a {
		a[i] = 1.0
	}

	chimp := NewChimp[uint64]()
	buffer := &bytes.Buffer{}

	chimp.Pack(a, buffer, NOP, 0.0)
	assert.Equal(t, 11, buffer.Len())       // Num bytes
	assert.Equal(t, uint64(82), chimp.size) // Num bits
}

// Tests the memory impact of storing a monotonically increasing sequence of
// size 10
func TestChimp_UInt64_CompressionCheckForSequence(t *testing.T) {

	a := make([]uint64, 10)
	for i := range a {
		a[i] = uint64(i)
	}

	chimp := NewChimp[uint64]()
	buffer := &bytes.Buffer{}

	chimp.Pack(a, buffer, NOP, 0.0)
	assert.Equal(t, 30, buffer.Len())        // Num bytes
	assert.Equal(t, uint64(235), chimp.size) // Num bits
}

// Tests the memory impact of storing a monotonically increasing sequence of
// size 10 with delta operation applied before compression
func TestChimp_UInt64_CompressionCheckForDeltaSequence(t *testing.T) {

	a := make([]uint64, 10)
	for i := range a {
		a[i] = uint64(i)
	}

	chimp := NewChimp[uint64]()
	buffer := &bytes.Buffer{}

	chimp.Pack(a, buffer, Delta, 0.0)
	assert.Equal(t, 13, buffer.Len())       // Num bytes
	assert.Equal(t, uint64(99), chimp.size) // Num bits

	res := make([]uint64, 10)
	chimp.Unpack(buffer, res, Delta, 0.0)

	for i := range a {
		assert.Equal(t, a[i], res[i])
	}
}

// Tests the memory impact of storing a monotonically increasing sequence of
// size 10 with offset operation applied before compression
func TestChimp_UInt64_CompressionCheckForOffsetSequence(t *testing.T) {

	a := make([]uint64, 10)
	for i := range a {
		a[i] = 9 + uint64(i)
	}

	chimp := NewChimp[uint64]()
	buffer := &bytes.Buffer{}

	chimp.Pack(a, buffer, Offset, 9)
	assert.Equal(t, 30, buffer.Len())        // Num bytes
	assert.Equal(t, uint64(235), chimp.size) // Num bits

	res := make([]uint64, 10)
	chimp.Unpack(buffer, res, Offset, 9.0)

	for i := range a {
		assert.Equal(t, a[i], res[i])
	}
}

// Tests the memory impact of storing 10 elements that ping pongs between
// two values
func TestChimp_UInt64_CompressionCheckForPingPongSequence(t *testing.T) {

	a := make([]uint64, 10)
	for i := range a {
		a[i] = uint64(i % 2)
	}

	chimp := NewChimp[uint64]()
	buffer := &bytes.Buffer{}

	chimp.Pack(a, buffer, NOP, 0.0)
	assert.Equal(t, 30, buffer.Len())        // Num bytes
	assert.Equal(t, uint64(235), chimp.size) // Num bits
}

// Tests the memory impact of storing 1 million large value sequence.
func TestChimp_UInt64_CompressionCheckForLargeValueSequence(t *testing.T) {

	a := make([]uint64, 1000000)
	for i := range a {
		a[i] = uint64(100000 + (i % 1000000))
	}

	chimp := NewChimp[uint64]()
	buffer := &bytes.Buffer{}

	chimp.Pack(a, buffer, NOP, 0.0)
	var threshold int = 2450000
	assert.LessOrEqual(t, buffer.Len(), threshold) // Num bytes
}

// Benchmark testing for packing. A single iteration will pack 1 million
// constant uint64 (1999). So a single op in the "ns/op" refers to
// handling 1 million uint64s
func BenchmarkChimpFor_UInt64_PackingConst(t *testing.B) {

	a := make([]uint64, 1000000)
	for i := range a {
		a[i] = 1999
	}

	t.ResetTimer()
	for l := 0; l < t.N; l++ {
		chimp := NewChimp[uint64]()
		buffer := &bytes.Buffer{}

		t.StartTimer()
		chimp.Pack(a, buffer, NOP, 0.0)
		t.StopTimer()
	}
}

// Benchmark testing for unpacking. A single iteration will pack 1 million
// constant uint64 (1999). So a single op in the "ns/op" refers to
// handling 1 million uint64s
func BenchmarkChimpFor_UInt64_UnpackingConst(t *testing.B) {

	a := make([]uint64, 1000000)
	res := make([]uint64, 1000000)
	for i := range a {
		a[i] = 1999
	}

	t.ResetTimer()
	for l := 0; l < t.N; l++ {
		chimp := NewChimp[uint64]()
		buffer := &bytes.Buffer{}
		chimp.Pack(a, buffer, NOP, 0.0)

		t.StartTimer()
		chimp.Unpack(buffer, res, NOP, 0.0)
		t.StopTimer()
	}
}

// Benchmark testing for packing. A single iteration will pack 1 million
// uint64s that are monotonically increasing by 1.0. So a single op in the
// "ns/op" refers to handling 1 million uint64s
func BenchmarkChimpFor_UInt64_PackingSequence(t *testing.B) {

	a := make([]uint64, 1000000)
	for i := range a {
		a[i] = uint64(i) + 10000
	}

	t.ResetTimer()
	for l := 0; l < t.N; l++ {
		chimp := NewChimp[uint64]()
		buffer := &bytes.Buffer{}

		t.StartTimer()
		chimp.Pack(a, buffer, NOP, 0.0)
		t.StopTimer()
	}
}

// Benchmark testing for unpacking. A single iteration will unpack 1 million
// uint64s that are monotonically increasing by 1.0. So a single op in the
// "ns/op" refers to handling 1 million uint64s
func BenchmarkChimpFor_Unt64_UnpackingSequence(t *testing.B) {

	a := make([]uint64, 1000000)
	res := make([]uint64, 1000000)
	for i := range a {
		a[i] = uint64(i) + 10000
	}

	t.ResetTimer()
	for l := 0; l < t.N; l++ {
		chimp := NewChimp[uint64]()
		buffer := &bytes.Buffer{}
		chimp.Pack(a, buffer, NOP, 0.0)

		t.StartTimer()
		chimp.Unpack(buffer, res, NOP, 0.0)
		t.StopTimer()
	}
}

func BenchmarkChimpFor_StockPrice(t *testing.B) {

	prices := ReadStockPriceFile()
	if prices == nil {
		assert.FailNow(t, "Failed to read stock price data")
	}

	chimp := NewChimp[float64]()
	buffer := &bytes.Buffer{}
	buffer.Grow(50000000)
	t.StartTimer()
	chimp.Pack(prices, buffer, NOP, 0.0)
	t.StopTimer()
	println("Num bytes: ", buffer.Len())
}
