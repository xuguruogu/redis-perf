package main

import (
	"fmt"
	"sync/atomic"
	"testing"
	"unsafe"
)

func TestAtomic(t *testing.T) {
	var uptr unsafe.Pointer
	var f1, f2 float64

	f1 = 12.34
	f2 = 56.78
	// Original values
	fmt.Println(f1, f2)
	uptr = unsafe.Pointer(&f1)
	o := uptr
	swap := atomic.SwapPointer(&uptr, unsafe.Pointer(&f2))
	fmt.Println(*(*float64)(o), *(*float64)(swap))
}
