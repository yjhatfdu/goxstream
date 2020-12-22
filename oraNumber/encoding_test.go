package oraNumber

import (
	"fmt"
	"testing"
)

func TestFromUint(t *testing.T) {
	a := uint64(0)
	n := FromUint(a)
	fmt.Println(n.AsUint())
	if n.AsUint() != a {
		t.Fail()
	}
}
func TestFromInt(t *testing.T) {
	a := int64(-12345)
	n := FromInt(a)
	fmt.Println(n.AsInt())
	if n.AsInt() != a {
		t.Fail()
	}
}