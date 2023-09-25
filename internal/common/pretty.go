package common

import "fmt"

func FormatByte(b uint64) string {
	switch {
	case b < Kibi:
		return fmt.Sprintf("%4v B", b)
	case b < Mebi:
		return fmt.Sprintf("%4v KiB", b/Kibi)
	case b < Gibi:
		return fmt.Sprintf("%4v MiB", b/Mebi)
	case b < Tebi:
		return fmt.Sprintf("%4v GiB", b/Gibi)
	}
	return fmt.Sprintf("%4v TiB", b/Tebi)
}
