package ops

import (
	"testing"

	"github.com/rmravindran/ats/series/frame"
	"github.com/rmravindran/ats/series/packer"

	"github.com/stretchr/testify/assert"
)

func TestOpMulAdd_Basic(t *testing.T) {

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

	var opAdd = NewOpMulAddT[float64](2.0)
	txFa := NewTxIdentity[float64](fA.Values())
	txFb := NewTxIdentity[float64](fB.Values())
	res := opAdd.Apply(txFa).Apply(txFb)
	assert.Nil(t, res.Error())

	for i := 0; i < 10; i++ {
		assert.Equal(t, (2.0*fA.Values()[i])+fB.Values()[i], res.Values().ValueAt(i))
	}
}

func TestOpMulAdd_NegativeA(t *testing.T) {

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

	var op = NewOpMulAddT[float64](2.0)
	txFa := NewTxNegate[float64](fA.Values())
	txFb := NewTxIdentity[float64](fB.Values())
	res := op.Apply(txFa).Apply(txFb)
	assert.Nil(t, res.Error())

	for i := 0; i < 10; i++ {
		assert.Equal(t, (2.0*-fA.Values()[i])+fB.Values()[i], res.Values().ValueAt(i))
	}
}

func TestOpMulAdd_NegativeB(t *testing.T) {

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

	var op = NewOpMulAddT[float64](2.0)
	txFa := NewTxIdentity[float64](fA.Values())
	txFb := NewTxNegate[float64](fB.Values())
	res := op.Apply(txFa).Apply(txFb)
	assert.Nil(t, res.Error())

	for i := 0; i < 10; i++ {
		assert.Equal(t, (2.0*fA.Values()[i])-fB.Values()[i], res.Values().ValueAt(i))
	}
}

func TestOpMulAdd_NegativeAB(t *testing.T) {

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

	var op = NewOpMulAddT[float64](2.0)
	txFa := NewTxNegate[float64](fA.Values())
	txFb := NewTxNegate[float64](fB.Values())
	res := op.Apply(txFa).Apply(txFb)
	assert.Nil(t, res.Error())

	for i := 0; i < 10; i++ {
		assert.Equal(t, (2.0*-fA.Values()[i])-fB.Values()[i], res.Values().ValueAt(i))
	}
}
