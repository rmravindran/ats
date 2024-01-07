package ops

import (
	"testing"

	"github.com/rmravindran/ats/series"
	"github.com/rmravindran/ats/series/frame"
	"github.com/rmravindran/ats/series/packer"

	"github.com/stretchr/testify/assert"
)

func TestOpSum_Basic(t *testing.T) {

	p := packer.NewChimp[float64]()
	f := frame.NewEmptyFrame[float64](10, p)

	for i := 0; i < 10; i++ {
		f.SetValue(i, float64(i))
	}

	var opSum = NewOpSum[float64](0, 2)
	tx := NewTxIdentity[float64](f.Values())
	res := opSum.Apply(tx)
	assert.Nil(t, res.Error())
	assert.Equal(t, 5, res.Values().Length())

	exp := []float64{1, 5, 9, 13, 17}
	for i := 0; i < 5; i++ {
		assert.Equal(t, exp[i], res.Values().ValueAt(i))
	}
}
func TestOpSum_Series(t *testing.T) {

	s := series.NewSeries[float64](10)

	for i := 0; i < 10; i++ {
		err := s.AppendValue(uint64(i), float64(i))
		assert.Nil(t, err)
	}

	var opSum = NewOpSum[float64](0, 2)
	tx := NewTxSeries[float64](s)
	res := opSum.Apply(tx)
	assert.Nil(t, res.Error())
	assert.Equal(t, 5, res.Values().Length())

	exp := []float64{1, 5, 9, 13, 17}
	for i := 0; i < 5; i++ {
		assert.Equal(t, exp[i], res.Values().ValueAt(i))
		assert.Equal(t, uint64(i*2), res.Values().TimeAt(i))
	}
}
