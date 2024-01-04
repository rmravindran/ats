package packer

type PackOp int64

const (
	NOP PackOp = iota
	Offset
	Delta
)

func (s PackOp) String() string {
	switch s {
	case NOP:
		return "NOP"
	case Offset:
		return "Offset"
	case Delta:
		return "Delta"
	}
	return "Invalid"
}
