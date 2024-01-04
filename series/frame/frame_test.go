package frame

import (
	"testing"

	"github.com/rmravindran/ats/series/packer"

	"github.com/stretchr/testify/assert"
)

func TestFrame_EmptyFrame(t *testing.T) {

	// Create an empty frame
	pA := packer.NewChimp[float64]()
	fA := NewEmptyFrame[float64](10, pA)

	for i := 0; i < 10; i++ {
		err := fA.SetValue(i, float64(i))
		assert.Nil(t, err)
	}

	// Length of the frame should return the number of elements in the frame
	assert.Equal(t, fA.Length(), uint64(10))

	// Finalize without releasing the unpacked array
	fA.Finalize(false)

	// Frame size should be packed size + unpacked array size
	assert.Equal(t, uint64(10*8)+pA.PackedSize(), fA.Size())

	// Finalize the frame with release option.
	fA.Finalize(true)

	// Frame size should be just the packed size
	assert.Equal(t, pA.PackedSize(), fA.Size())
}

func TestFrame_UnPackedFrame(t *testing.T) {

	// Create an empty frame
	pA := packer.NewChimp[float64]()
	values := make([]float64, 10)

	for i := 0; i < 10; i++ {
		values[i] = float64(i)
	}
	fA := NewUnpackedFrame[float64](values, pA)

	// Length of the frame should return the number of elements in the frame
	assert.Equal(t, fA.Length(), uint64(10))

	// Finalize without releasing the unpacked array
	fA.Finalize(false)

	// Frame size should be packed size + unpacked array size
	assert.Equal(t, uint64(10*8)+pA.PackedSize(), fA.Size())

	// Finalize the frame with release option.
	fA.Finalize(true)

	// Frame size should be just the packed size
	assert.Equal(t, pA.PackedSize(), fA.Size())
}

func TestFrame_PackedFrame(t *testing.T) {

	// Create an empty frame
	pA := packer.NewChimp[float64]()
	values := make([]float64, 10)

	for i := 0; i < 10; i++ {
		values[i] = float64(i)
	}
	fA := NewUnpackedFrame[float64](values, pA)
	fA.Finalize(false)
	buffer := fA.Buffer()

	fB := NewPackedFrame[float64](buffer, pA)

	// Length of the frame should return the number of elements in the frame
	assert.Equal(t, fB.Length(), uint64(10))

	// Check every value in the unpacked frame.
	for i := 0; i < 10; i++ {
		v, err := fB.Value(i)
		assert.Nil(t, err)
		assert.Equal(t, float64(i), v)
	}

	// Asking for buffer on dirty frame should return nil
	fB.SetValue(1, 99.0)
	assert.Nil(t, fB.Buffer())

	// Finalize without releasing the unpacked array
	fA.Finalize(false)

	// Frame size should be packed size + unpacked array size
	assert.Equal(t, uint64(10*8)+pA.PackedSize(), fA.Size())

	// Finalize the frame with release option.
	fA.Finalize(true)

	// Frame size should be just the packed size
	assert.Equal(t, pA.PackedSize(), fA.Size())
}
