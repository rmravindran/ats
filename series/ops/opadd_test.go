package ops

import (
	"github.com/rmravindran/ats/series/frame"
	"github.com/rmravindran/ats/series/packer"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFrame_EmptyFrame(t *testing.T) {

	pA := packer.NewChimp[float64]()
	fA := frame.NewEmptyFrame[float64](10, pA)

	for i := 0; i < 10; i++ {
		fA.SetValue(uint64(i), float64(i))
	}

	pB := packer.NewChimp[float64]()
	fB := frame.NewEmptyFrame[float64](10, pB)

	for i := 0; i < 10; i++ {
		fB.SetValue(uint64(i), float64(i))
	}

	var opAdd = JustOp[float64](&OpAdd[float64]{})
	res := opAdd.Apply(fA).Apply(fB)
	assert.Nil(t, res.Error())

	for i := 0; i < 10; i++ {
		assert.Equal(t, fA.Values()[i]+fB.Values()[i], res.Values()[i])
	}
}
