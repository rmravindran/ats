package ops

import (
	"testing"

	"github.com/rmravindran/ats/series"

	"github.com/stretchr/testify/assert"
)

func TestOpPct_Pct50(t *testing.T) {
	s := series.NewSeries[float64](100)

	// Create a series with 100 values from 0 to 99
	for i := 0; i < 100; i++ {
		err := s.AppendValue(uint64(i), float64(i))
		assert.Nil(t, err)
	}

	// Create a new OpPct with a window of 10 and pct of 50
	var op = NewOpPct[float64](10, 50)
	tx := NewTxSeries[float64](s)

	// This will compute the 50th percentile within each of the 10 windows
	res := op.Apply(tx)
	assert.Nil(t, res.Error())

	// Check the 50th percentile in each of the 10 values in the res.
	exp := []float64{4.5, 14.5, 24.5, 34.5, 44.5, 54.5, 64.5, 74.5, 84.5, 94.5}
	for i, v := range exp {
		assert.Equal(t, v, res.Values().ValueAt(i))
		assert.Equal(t, uint64(i*10), res.Values().TimeAt(i))
	}
}

func TestOpPct_SmallArray(t *testing.T) {
	s := series.NewSeries[float64](100)
	s.AppendValue(0, 1.5)

	// Create a new OpPct with a window of 1 and a pct of 50
	var op = NewOpPct[float64](1, 50)
	tx := NewTxSeries[float64](s)

	// This will compute the 50th percentile of the only window
	res := op.Apply(tx)
	assert.Nil(t, res.Error())

	// Check the 50th percentile in each of the 10 values in the res.
	assert.Equal(t, 1.5, res.Values().ValueAt(0))
	assert.Equal(t, uint64(0), res.Values().TimeAt(0))
}

func TestOpPct_PctAll(t *testing.T) {
	s := series.NewSeries[float64](100)

	// Create a series with 100 values from 0 to 99
	for i := 0; i < 100; i++ {
		err := s.AppendValue(uint64(i), float64(i))
		assert.Nil(t, err)
	}
	tx := NewTxSeries[float64](s)

	for pct := 0; pct <= 100; pct++ {
		var op = NewOpPct[float64](10, float64(pct))
		res := op.Apply(tx)
		assert.Nil(t, res.Error())

		i1 := int(9 * (float64(pct) / 100.0))
		i2 := i1
		if pct > 0 && i1+1 < 10 {
			i2 = i1 + 1
		}
		_, v1, _ := s.Value(i1)
		_, v2, _ := s.Value(i2)
		refV := (v1 + v2) / 2.0
		exp := []float64{
			refV,
			10 + refV,
			20 + refV,
			30 + refV,
			40 + refV,
			50 + refV,
			60 + refV,
			70 + refV,
			80 + refV,
			90 + refV}

		for i, v := range exp {
			assert.Equal(t, v, res.Values().ValueAt(i))
			assert.Equal(t, uint64(i*10), res.Values().TimeAt(i))
		}
	}
}
