package ops

import (
	"github.com/rmravindran/ats/series/packer"
)

// Defines an interface for representing transformable data.
type Transformable[S packer.Number, T packer.Number] interface {

	// Tranform the values of the underlying transformable array and return the
	// value at the specified index.
	ValueAt(idx int) T

	// Return the time at the specified index.
	TimeAt(idx int) uint64

	// Return true if the underlying transformable array is empty.
	IsEmpty() bool

	// Return the length of the underlying transformable array.
	Length() int
}
