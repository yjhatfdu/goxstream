package scn

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type SCN uint64

func (s SCN) String() string {
	return fmt.Sprintf("%X/%X", int64(s>>32), int64(s&0xFFFFFFFF))
}

func Parse(s string) (SCN, error) {
	segs := strings.Split(strings.ToLower(s), "/")
	if len(segs) != 2 {
		return 0, errors.New("invalid scn format")
	}
	hi, err := strconv.ParseUint(segs[0], 16, 64)
	if err != nil {
		return 0, err
	}
	lo, err := strconv.ParseUint(segs[1], 16, 64)
	if err != nil {
		return 0, err
	}
	return SCN(hi<<32 + lo), nil
}
