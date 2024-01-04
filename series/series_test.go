package series

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSeries_BasicPack(t *testing.T) {

	s := NewSeries[float64](10)

	assert.Zero(t, s.Size())
	assert.Equal(t, 10, s.FrameSize())
}

func TestSeries_ValueCheck(t *testing.T) {

	s := NewSeries[float64](10)

	for i := 0; i < 10; i++ {
		err := s.AppendValue(uint64(i), float64(i))
		assert.Nil(t, err)
	}

	for i := 0; i < 10; i++ {
		time, v, err := s.Value(i)
		assert.Equal(t, uint64(i), time)
		assert.Equal(t, float64(i), v)
		assert.Nil(t, err)
	}

}
