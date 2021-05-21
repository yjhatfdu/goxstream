package goxstream

// #cgo CFLAGS: -I./include -fPIC
// #cgo LDFLAGS: -lclntsh
/* #
#include "xstrm.c"
*/
import "C"
import (
	"fmt"
	"github.com/chai2010/cgo"
	"github.com/yjhatfdu/goxstream/oraNumber"
	"github.com/yjhatfdu/goxstream/scn"
	"golang.org/x/text/encoding/simplifiedchinese"
	"log"
	"reflect"
	"time"
	"unicode/utf16"
	"unsafe"
)

var decoders = map[int]func(b []byte) (string, error){}

func init() {
	decoders[2000] = func(b []byte) (string, error) {
		buf := make([]uint16, len(b)/2)
		for i := range buf {
			buf[i] = uint16(b[2*i]) + uint16(b[2*i+1])<<8
		}
		return string(utf16.Decode(buf)), nil
	}
	gbkDecoder := simplifiedchinese.GBK.NewDecoder()
	decoders[852] = func(b []byte) (string, error) {
		s, err := gbkDecoder.Bytes(b)
		return string(s), err
	}
	decoders[0] = func(b []byte) (string, error) {
		return string(b), nil
	}
}

func toOciStr(s string) (*C.uchar, C.uint, func()) {
	uchars := cgo.NewUInt8N(len(s))
	l := uint32(len(s))
	cs := uchars.Slice(len(s))
	copy(cs, s)
	return (*C.uchar)(uchars), C.uint(l), func() {
		uchars.Free()
	}
}

type XStreamConn struct {
	ocip  *C.struct_oci
	csid  int
	ncsid int
}

func Open(username, password, dbname, servername string) (*XStreamConn, error) {
	var info C.struct_conn_info
	usernames, usernamel, free := toOciStr(username)
	defer free()
	info.user = usernames
	info.userlen = usernamel
	psws, pswl, free2 := toOciStr(password)
	defer free2()
	info.passw = psws
	info.passwlen = pswl
	dbs, dbl, free3 := toOciStr(dbname)
	defer free3()
	info.dbname = dbs
	info.dbnamelen = dbl
	svrs, svrl, free4 := toOciStr(servername)
	defer free4()
	info.svrnm = svrs
	info.svrnmlen = svrl
	var oci *C.struct_oci
	var char_csid, nchar_csid C.ushort
	C.get_db_charsets(&info, &char_csid, &nchar_csid)
	C.connect_db(&info, &oci, char_csid, nchar_csid)
	C.attach(oci, &info, C.int(1))
	return &XStreamConn{
		ocip:  oci,
		csid:  int(char_csid),
		ncsid: int(nchar_csid),
	}, nil
}

func (x *XStreamConn) Close() error {
	C.detach(x.ocip)
	C.disconnect_db(x.ocip)
	C.free(unsafe.Pointer(x.ocip))
	return nil
}

func (x *XStreamConn) SetSCNLwm(s scn.SCN) error {
	pos, posl := scn2pos(x.ocip, s)
	defer C.free(unsafe.Pointer(pos))
	status := C.OCIXStreamOutProcessedLWMSet(x.ocip.svcp, x.ocip.errp, pos, posl, C.OCI_DEFAULT)
	if status == C.OCI_ERROR {
		errstr, errcode := getError(x.ocip.errp)
		return fmt.Errorf("set position lwm failed, code:%d, %s", errcode, errstr)
	}
	return nil
}

func (x *XStreamConn) GetRecord() (Message, error) {
	var lcr unsafe.Pointer = C.malloc(C.size_t(1))
	var lcrType C.uchar
	var flag C.ulong
	var fetchlwm = (*C.uchar)(C.calloc(C.OCI_LCR_MAX_POSITION_LEN, 8))
	defer C.free(unsafe.Pointer(fetchlwm))
	var fetchlwm_len C.ushort
	status := C.OCIXStreamOutLCRReceive(x.ocip.svcp, x.ocip.errp, &lcr, &lcrType,
		&flag, fetchlwm, &fetchlwm_len, C.OCI_DEFAULT)
	if status == C.OCI_STILL_EXECUTING {
		return getLcrRecords(x.ocip, lcr, x.csid, x.ncsid)
	}
	if status == C.OCI_ERROR {
		errstr, errcode := getError(x.ocip.errp)
		return nil, fmt.Errorf("OCIXStreamOutLCRReceive failed, code:%d, %s", errcode, errstr)
	}
	C.OCILCRFree(x.ocip.svcp, x.ocip.errp, lcr, C.OCI_DEFAULT)
	C.free(lcr)
	return nil, nil
}

func tostring(p *C.uchar, l C.ushort) string {
	return string(*(*[]byte)((unsafe.Pointer)(&reflect.SliceHeader{Data: uintptr(unsafe.Pointer(p)), Len: int(uint16(l)), Cap: int(uint16(l))})))
}

func toStringEnc(p *C.uchar, l C.ushort, codepage int) (string, error) {
	b := *(*[]byte)((unsafe.Pointer)(&reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(p)),
		Len:  int(uint16(l)),
		Cap:  int(uint16(l)),
	}))
	dec := decoders[codepage]
	if dec == nil {
		log.Panicf("code page %d not defined", codepage)
	} else {
		return dec(b)
	}
	return "", nil
}

func tobytes(p *C.uchar, l C.ushort) []byte {
	ret := make([]byte, uint16(l))
	copy(ret, *(*[]byte)((unsafe.Pointer)(&reflect.SliceHeader{
		Data: uintptr(unsafe.Pointer(p)),
		Len:  int(uint16(l)),
		Cap:  int(uint16(l)),
	})))
	return ret
}

func getLcrRecords(ocip *C.struct_oci, lcr unsafe.Pointer, csid, ncsid int) (Message, error) {
	var cmd_type, owner, oname, txid *C.oratext
	var cmd_type_len, ownerl, onamel, txidl C.ub2
	var src_db_name **C.oratext
	var src_db_name_l *C.ub2
	var ret C.sword
	var ltag, lpos *C.ub1
	var ltagl, lposl, oldCount, newCount C.ub2
	var dummy C.oraub8
	var t C.OCIDate
	ret = C.OCILCRHeaderGet(ocip.svcp, ocip.errp,
		src_db_name, src_db_name_l, &cmd_type, &cmd_type_len,
		&owner, &ownerl, &oname, &onamel, &ltag, &ltagl, &txid, &txidl,
		&t, &oldCount, &newCount,
		&lpos, &lposl, &dummy, lcr,
		C.OCI_DEFAULT)
	if ret != C.OCI_SUCCESS {
		C.ocierror(ocip, C.CString("OCILCRHeaderGet failed"))
	} else {
		cmd := tostring(cmd_type, cmd_type_len)
		s := pos2SCN(ocip, lpos, lposl)
		var err error
		switch cmd {
		case "COMMIT":
			m := Commit{SCN: s}
			return &m, nil
		case "DELETE":
			m := Delete{SCN: s, Table: tostring(oname, onamel), Owner: tostring(owner, ownerl)}
			m.OldColumn, m.OldRow, err = getLcrRowData(ocip, lcr, valueTypeOld, csid, ncsid)
			return &m, err
		case "INSERT":
			m := Insert{SCN: s, Table: tostring(oname, onamel), Owner: tostring(owner, ownerl)}
			m.NewColumn, m.NewRow, err = getLcrRowData(ocip, lcr, valueTypeNew, csid, ncsid)
			return &m, err
		case "UPDATE":
			m := Update{SCN: s, Table: tostring(oname, onamel), Owner: tostring(owner, ownerl)}
			m.OldColumn, m.OldRow, err = getLcrRowData(ocip, lcr, valueTypeOld, csid, ncsid)
			if err != nil {
				return nil, err
			}
			m.NewColumn, m.NewRow, err = getLcrRowData(ocip, lcr, valueTypeNew, csid, ncsid)
			return &m, err
		}
	}
	return nil, nil
}

type valueType C.ub2

var valueTypeOld valueType = C.OCI_LCR_ROW_COLVAL_OLD
var valueTypeNew valueType = C.OCI_LCR_ROW_COLVAL_NEW

func getLcrRowData(ocip *C.struct_oci, lcrp unsafe.Pointer, valueType valueType, csid, ncsid int) ([]string, []interface{}, error) {
	const colCount int = 256
	var result C.sword
	var num_cols C.ub2
	var col_names = (**C.oratext)(unsafe.Pointer(cgo.NewIntN(colCount)))
	var col_names_lens = (*C.ub2)(cgo.NewUInt16N(colCount))
	var col_dtype [colCount]C.ub2
	var column_valuesp [colCount]*C.void
	var column_indp = (*C.OCIInd)(C.calloc(C.size_t(colCount), C.size_t(unsafe.Sizeof(C.OCIInd(0)))))
	var column_alensp [colCount]C.ub2
	var column_csetfp = (*C.ub1)(cgo.NewUInt8N(colCount))
	var column_flags = (*C.oraub8)(cgo.NewUInt64N(colCount))
	var column_csid [colCount]C.ub2
	defer func() {
		C.free(unsafe.Pointer(col_names))
		C.free(unsafe.Pointer(col_names_lens))
		C.free(unsafe.Pointer(column_indp))
		C.free(unsafe.Pointer(column_csetfp))
		C.free(unsafe.Pointer(column_flags))
	}()
	result = C.OCILCRRowColumnInfoGet(
		ocip.svcp, ocip.errp,
		C.ushort(valueType), &num_cols,
		col_names, col_names_lens,
		(*C.ub2)(unsafe.Pointer(&col_dtype)),
		(*unsafe.Pointer)((unsafe.Pointer)(&column_valuesp)),
		column_indp,
		(*C.ub2)(unsafe.Pointer(&column_alensp)),
		column_csetfp,
		column_flags,
		(*C.ub2)(unsafe.Pointer(&column_csid)),
		lcrp,
		C.ushort(colCount),
		C.OCI_DEFAULT,
	)
	if result != C.OCI_SUCCESS {
		errstr, errcode := getError(ocip.errp)
		return nil, nil, fmt.Errorf("OCIXStreamOutLCRReceive failed, code:%d, %s", errcode, errstr)
	} else {
		columnNames := make([]string, 0)
		columnValues := make([]interface{}, 0)
		for i := 0; i < int(uint16(num_cols)); i++ {
			colName := tostring((*C.uchar)((*[colCount]unsafe.Pointer)(unsafe.Pointer(col_names))[i]),
				C.ushort((*[colCount]uint16)(unsafe.Pointer(col_names_lens))[i]))
			columnNames = append(columnNames, colName)
			colValuep := column_valuesp[i]
			colDtype := col_dtype[i]
			csid_l := int(column_csid[i])
			if csid_l == 0 {
				csid_l = csid
			}
			colValue := value2interface(colValuep, column_alensp[i], csid_l, colDtype)
			columnValues = append(columnValues, colValue)
		}
		return columnNames, columnValues, nil
	}
}

func value2interface(valuep *C.void, valuelen C.ub2, csid int, dtype C.ub2) interface{} {
	switch dtype {
	//todo support more types
	case C.SQLT_CHR, C.SQLT_AFC:
		val, err := toStringEnc((*C.uchar)(unsafe.Pointer(valuep)), valuelen, int(csid))
		if err != nil {
			panic(err)
		}
		return val
	case C.SQLT_VNU:
		v := (*C.OCINumber)(unsafe.Pointer(valuep))
		if v == nil {
			return nil
		}
		valBytes := *(*[22]byte)(unsafe.Pointer(&v.OCINumberPart))
		return oraNumber.Number(valBytes).AsInt()
	case C.SQLT_ODT:
		v := (*C.OCIDate)(unsafe.Pointer(valuep))
		yy := int16(v.OCIDateYYYY)
		mm := uint8(v.OCIDateMM)
		dd := uint8(v.OCIDateDD)
		dt := v.OCIDateTime
		hh := uint8(dt.OCITimeHH)
		min := uint8(dt.OCITimeMI)
		ss := uint8(dt.OCITimeSS)
		return time.Date(int(yy), time.Month(mm), int(dd), int(hh), int(min), int(ss), 0, time.Local)
	}
	return nil
}

func getError(oci_err *C.OCIError) (string, int32) {
	errCode := C.sb4(0)
	text := [4096]C.text{}
	C.OCIErrorGet(unsafe.Pointer(oci_err), C.uint(1),
		(*C.text)(unsafe.Pointer(nil)), &errCode, (*C.uchar)(unsafe.Pointer(&text)), 4096, C.OCI_HTYPE_ERROR)
	return cgo.GoString((*cgo.Char)(unsafe.Pointer(&text))), int32(errCode)
}

func pos2SCN(ocip *C.struct_oci, pos *C.ub1, pos_len C.ub2) scn.SCN {
	var s C.struct_OCINumber
	var commit_scn C.struct_OCINumber
	var result C.sword
	result = C.OCILCRSCNsFromPosition(ocip.svcp, ocip.errp, pos, pos_len, &s, &commit_scn, C.OCI_DEFAULT)
	if result != C.OCI_SUCCESS {
		// todo
		C.ocierror(ocip, C.CString("OCILCRHeaderGet failed"))
		return 0
	} else {
		valBytes := *(*[22]byte)(unsafe.Pointer(&s.OCINumberPart))
		return scn.SCN(oraNumber.Number(valBytes).AsInt())
	}
}

func scn2pos(ocip *C.struct_oci, s scn.SCN) (*C.ub1, C.ub2) {
	v := oraNumber.FromUint(uint64(s))
	var number C.struct_OCINumber
	number.OCINumberPart = *(*[22]C.uchar)(unsafe.Pointer(&v))
	pos := (*C.ub1)(C.calloc(33, 1))
	var posl C.ub2
	result := C.OCILCRSCNToPosition2(ocip.svcp, ocip.errp, pos, &posl, &number, C.OCI_LCRID_V2, C.OCI_DEFAULT)
	if result != C.OCI_SUCCESS {
		// todo
		C.ocierror(ocip, C.CString("OCILCRHeaderGet failed"))
		return nil, 0
	} else {
		return pos, posl
	}
}
