package series

import (
	"errors"

	"github.com/rmravindran/ats/series/frame"
	"github.com/rmravindran/ats/series/packer"
)

type Series[T packer.Number] struct {

	// Start Time (includes this time)
	startTime uint64

	// End Time (excludes this time)
	endTime uint64

	// Frames for time
	timeFrames []*frame.Frame[uint64]

	// Frames for values
	valueFrames []*frame.Frame[T]

	// Frame size
	frameSize int

	// Size of the series
	size int

	// Last frame offset
	lastFrameOffset int
}

// Creates a new series where every frame is of the specified fameSize
func NewSeries[T packer.Number](frameSize int) *Series[T] {
	return &Series[T]{
		startTime:       0,
		endTime:         0,
		timeFrames:      nil,
		valueFrames:     nil,
		frameSize:       frameSize,
		size:            0,
		lastFrameOffset: 0,
	}
}

// Appends a value to the series
func (series *Series[T]) AppendValue(time uint64, value T) error {
	if series.timeFrames == nil || len(series.timeFrames) == 0 {
		series.appendFrame()
	}
	frameIndex := series.size / series.frameSize
	if frameIndex >= len(series.timeFrames) {
		series.appendFrame()
	}

	errT := series.timeFrames[frameIndex].SetValue(series.lastFrameOffset, time)
	if errT != nil {
		return errT
	}

	errV := series.valueFrames[frameIndex].SetValue(series.lastFrameOffset, value)
	if errV != nil {
		return errV
	}

	series.lastFrameOffset++
	series.size++

	return nil
}

// Set value at the specified index
func (series *Series[T]) SetValue(index int, time uint64, value T) error {
	if index >= series.size {
		return errors.New("index out of bound")
	}

	frameIndex := index / series.frameSize
	localIndex := index - (frameIndex * series.frameSize)
	errT := series.timeFrames[frameIndex].SetValue(localIndex, time)
	if errT != nil {
		return errT
	}

	errV := series.valueFrames[frameIndex].SetValue(localIndex, value)
	if errV != nil {
		return errV
	}

	return nil
}

// Set value at the specified index
func (series *Series[T]) Value(index int) (uint64, T, error) {
	if index >= series.size {
		return 0, T(0), errors.New("index out of bound")
	}

	frameIndex := index / series.frameSize
	localIndex := index - (frameIndex * series.frameSize)
	t, errT := series.timeFrames[frameIndex].Value(localIndex)
	if errT != nil {
		return 0, T(0), errT
	}
	v, errV := series.valueFrames[frameIndex].Value(localIndex)
	if errT != nil {
		return 0, T(0), errV
	}

	return t, v, nil
}

func (series *Series[T]) Size() int {
	return series.size
}

func (series *Series[T]) FrameSize() int {
	return series.frameSize
}

//-----------------------------------------------------------------------------
//                              PRIVATE METHODS
//-----------------------------------------------------------------------------

func (series *Series[T]) appendFrame() {
	pT := packer.NewChimp[uint64]()
	fT := frame.NewEmptyFrame[uint64](uint64(series.frameSize), pT)
	series.timeFrames = append(series.timeFrames, fT)

	pV := packer.NewChimp[T]()
	fV := frame.NewEmptyFrame[T](uint64(series.frameSize), pV)
	series.valueFrames = append(series.valueFrames, fV)

	series.lastFrameOffset = 0
}
