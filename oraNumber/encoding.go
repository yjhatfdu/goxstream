// ported from https://github.com/felipenoris/Oracle.jl/blob/master/src/oranumbers/encoding.jl
package oraNumber

import "unsafe"

type Number [22]byte

const tRAILING_BYTE_ON_NEGATIVE_NUMBERS = 0x66

func (o *Number) isNegative() bool {
	return o.exp()&0x80 == 0x00
}

func (o *Number) isZero() bool {
	return o.sizeofMantissa() == 0 && !o.isNegative()
}

func (o *Number) isNegative1e126() bool {
	return o.sizeofMantissa() == 0 && o.isNegative()
}

func (o *Number) isNull() bool {
	return o.len() == 0xff
}

func (o *Number) mantissa() []uint8 {
	return o[2:22]
}

func (o *Number) sizeofMantissa() uint8 {
	lenByte := o.len()
	if o.isNegative() && o.mantissa()[lenByte-1] == tRAILING_BYTE_ON_NEGATIVE_NUMBERS {
		return lenByte - 2
	} else {
		return lenByte - 1
	}
}

func (o *Number) decodeExpByte() int8 {
	if o.isZero() {
		return 0
	}
	if o.isNegative1e126() {
		return 126
	}
	b := o.exp()
	if o.isNegative() {
		b = ^b
	}
	b = b - 0xc1
	return *(*int8)(unsafe.Pointer(&b))
}

func decodeMantissaByte(bytes []uint8, l uint8, index uint8, isNegative bool) uint8 {
	if index > l {
		return 0
	}
	if isNegative {
		if index == l-1 && bytes[index] == tRAILING_BYTE_ON_NEGATIVE_NUMBERS {
			return 0
		} else {
			return 0x65 - bytes[index]
		}
	} else {
		if bytes[index] == 0 {
			return 0
		}
		return bytes[index] - 1
	}
}

func (o *Number) len() uint8 {
	return o[0]
}

func (o *Number) exp() uint8 {
	return o[1]
}

var pow10 = [10]int64{
	1e0, 1e2, 1e4, 1e6, 1e8, 1e10, 1e12, 1e14, 1e16, 1e18,
}

func (o Number) AsInt() int64 {
	isn := o.isNegative()
	exp := o.decodeExpByte()
	l := o.sizeofMantissa()
	bytes := o.mantissa()
	tmp := int64(0)
	for i := int8(0); i <= exp; i++ {
		tmp += int64(decodeMantissaByte(bytes, l, uint8(i), isn)) * pow10[exp-i]
	}
	if isn {
		return -tmp
	} else {
		return tmp
	}
}

func (o Number) AsUint() uint64 {
	isn := o.isNegative()
	exp := o.decodeExpByte()
	l := o.sizeofMantissa()
	bytes := o.mantissa()
	tmp := uint64(0)
	for i := int8(0); i <= exp; i++ {
		tmp += uint64(decodeMantissaByte(bytes, l, uint8(i), isn)) * uint64(pow10[exp-i])
	}
	if isn {
		return 0
	} else {
		return tmp
	}
}

func encodeMantissaByte(b uint8, isNegative bool) uint8 {
	if isNegative {
		return 0x65 - b
	} else {
		return b + 1
	}
}

func encodeExpByte(ex int8, isNegative bool) uint8 {
	b := *(*uint8)(unsafe.Pointer(&ex)) + 0xc1
	if isNegative {
		return ^b
	} else {
		return b
	}
}

func FromUint(i uint64) Number {
	exp := int8(0)
	buf := make([]uint8, 20)
	v := i
	for v != 0 {
		b := uint8(v % 100)
		v = v / 100
		buf[20-exp-1] = encodeMantissaByte(b, false)
		exp++
	}
	ret := Number{}
	ret[0] = uint8(exp)
	ret[1] = encodeExpByte(exp-1, false)
	copy(ret[2:2+exp], buf[20-exp:20])
	return ret
}

func FromInt(i int64) Number {
	exp := int8(0)
	buf := make([]uint8, 20)
	v := i
	isN := false
	if v < 0 {
		isN = true
		v = -v
	}
	for v != 0 {
		b := uint8(v % 100)
		v = v / 100
		buf[20-exp-1] = encodeMantissaByte(b, isN)
		exp++
	}
	ret := Number{}
	ret[0] = uint8(exp)
	ret[1] = encodeExpByte(exp-1, isN)
	copy(ret[2:2+exp], buf[20-exp:20])
	return ret
}
