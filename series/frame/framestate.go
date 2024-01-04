package frame

type FrameState int64

const (
	Unknown FrameState = iota
	Native
	Compact
)

func (s FrameState) String() string {
	switch s {
	case Unknown:
		return "Unknown"
	case Native:
		return "Native"
	case Compact:
		return "Compact"
	}
	return "Invalid"
}
