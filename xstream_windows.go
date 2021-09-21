// +build windows

package goxstream

// #cgo CFLAGS: -I./include -fPIC
// #cgo LDFLAGS: -loci
/* #
#include "xstrm.c"
*/
import "C"
