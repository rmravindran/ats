package ops

import (
	"testing"

	"github.com/rmravindran/ats/series/frame"
	"github.com/rmravindran/ats/series/packer"

	"github.com/stretchr/testify/assert"
)

func TestOpMin_Basic(t *testing.T) {

	p := packer.NewChimp[float64]()
	f := frame.NewEmptyFrame[float64](10, p)

	for i := 0; i < 10; i++ {
		f.SetValue(i, float64(i))
	}

	var op = NewOpMin[float64](2)
	tx := NewTxIdentity[float64](f.Values())
	res := op.Apply(tx)
	assert.Nil(t, res.Error())
	assert.Equal(t, 5, res.Values().Length())

	exp := []float64{0, 2, 4, 6, 8}
	for i := 0; i < 5; i++ {
		assert.Equal(t, exp[i], res.Values().ValueAt(i))
	}
}
