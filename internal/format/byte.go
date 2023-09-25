package format

import (
	"fmt"

	"github.com/Milover/post/internal/common"
)

func Byte(b uint64) string {
	switch {
	case b < common.Kibi:
		return fmt.Sprintf("%4v B", b)
	case b < common.Mebi:
		return fmt.Sprintf("%4v KiB", b/common.Kibi)
	case b < common.Gibi:
		return fmt.Sprintf("%4v MiB", b/common.Mebi)
	case b < common.Tebi:
		return fmt.Sprintf("%4v GiB", b/common.Gibi)
	}
	return fmt.Sprintf("%4v TiB", b/common.Tebi)
}
