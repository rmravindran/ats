package ops

import (
	"testing"

	"github.com/rmravindran/ats/series/frame"
	"github.com/rmravindran/ats/series/packer"

	"github.com/stretchr/testify/assert"
)

func TestOpMul_Multiply(t *testing.T) {

	pA := packer.NewChimp[float64]()
	fA := frame.NewEmptyFrame[float64](10, pA)

	for i := 0; i < 10; i++ {
		fA.SetValue(i, float64(i))
	}

	pB := packer.NewChimp[float64]()
	fB := frame.NewEmptyFrame[float64](10, pB)

	for i := 0; i < 10; i++ {
		fB.SetValue(i, float64(i))
	}

	var op = NewOpMul[float64, float64]()
	txFa := NewTxIdentity[float64](fA.Values())
	txFb := NewTxIdentity[float64](fB.Values())
	res := op.Apply(txFa).Apply(txFb)
	assert.Nil(t, res.Error())

	for i := 0; i < 10; i++ {
		assert.Equal(t, fA.Values()[i]*fB.Values()[i], res.Values().ValueAt(i))
	}
}
