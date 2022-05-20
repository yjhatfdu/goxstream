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
	"github.com/yjhatfdu/goxstream/scn"
	"golang.org/x/text/encoding/simplifiedchinese"
	"golang.org/x/text/encoding/unicode"
	"log"
	"reflect"
	"time"
	"unicode/utf16"
	"unsafe"
)

type OCI_LCRID_VERSION int

const (
	V1 = 1
	V2 = 2
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
	utf8Decoder := unicode.UTF8.NewDecoder()
	decoders[873] = func(b []byte) (string, error) {
		s, err := utf8Decoder.Bytes(b)
		return string(s), err
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
	ocip     *C.struct_oci
	csid     int
	ncsid    int
	lcridVer OCI_LCRID_VERSION
}

func Open(username, password, dbname, servername string, oracleVer int) (*XStreamConn, error) {
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
	r := C.attach0(oci, &info, C.int(1))
	if int(r) != 0 {
		errstr, errcode, err := getErrorEnc(oci.errp, int(char_csid))
		if err != nil {
			return nil, fmt.Errorf("failed to parse oci error after calling Open function failed: %s", err.Error())
		}
		return nil, fmt.Errorf("attach to XStream server specified in connection info failed, code:%d, %s", errcode, errstr)
	}

	var version OCI_LCRID_VERSION
	if oracleVer >= 12 {
		version = V2
	} else {
		version = V1
	}

	return &XStreamConn{
		ocip:     oci,
		csid:     int(char_csid),
		ncsid:    int(nchar_csid),
		lcridVer: version,
	}, nil
}

func (x *XStreamConn) Close() error {
	C.detach(x.ocip)
	C.disconnect_db(x.ocip)
	C.free(unsafe.Pointer(x.ocip))
	return nil
}

func ociNumberToInt(errp *C.OCIError, number *C.OCINumber) int64 {
	var i int64
	C.OCINumberToInt(errp, number, 8, C.OCI_NUMBER_SIGNED, unsafe.Pointer(&i))
	return i
}

func ociNumberFromInt(errp *C.OCIError, i int64) *C.OCINumber {
	var n C.OCINumber
	C.OCINumberFromInt(errp, unsafe.Pointer(&i), 8, C.OCI_NUMBER_SIGNED, &n)
	return &n
}

func ToCUCharString(str string) (*C.uchar, C.ushort, func()) {
	charString := cgo.NewCharString(str)
	return (*C.uchar)(unsafe.Pointer(charString)), C.ushort(len(str)), func() {
		charString.Free()
	}
}

func (x *XStreamConn) SetSCNLwm(s scn.SCN) error {
	pos, posl := x.scn2pos(x.ocip, s)
	defer pos.Free()
	status := C.OCIXStreamOutProcessedLWMSet(x.ocip.svcp, x.ocip.errp, (*C.ub1)(pos), posl, C.OCI_DEFAULT)
	if status == C.OCI_ERROR {
		errstr, errcode := getError(x.ocip.errp)
		return fmt.Errorf("set position lwm failed, code:%d, %s", errcode, errstr)
	}
	return nil
}

func (x *XStreamConn) GetRecord() (Message, error) {
	var lcr = unsafe.Pointer(nil)
	var lcrtype C.ub1
	var flag C.oraub8
	var fetchlwm = cgo.NewUInt8N(C.OCI_LCR_MAX_POSITION_LEN)
	defer fetchlwm.Free()
	var fetchlwm_len C.ushort
	status := C.OCIXStreamOutLCRReceive(x.ocip.svcp, x.ocip.errp, &lcr, &lcrtype,
		&flag, (*C.ub1)(fetchlwm), &fetchlwm_len, C.OCI_DEFAULT)
	if status == C.OCI_STILL_EXECUTING {
		msg, err := x.getLcrRecords(x.ocip, lcr, x.csid, x.ncsid)
		if err != nil {
			return nil, fmt.Errorf("failed to call getLcrRecords function %s", err.Error())
		}

		/* If LCR has chunked columns (i.e, has LOB/Long/XMLType columns) */
		if flag != 0 && C.OCI_XSTREAM_MORE_ROW_DATA != 0 {
			C.travel_chunks(x.ocip)
		}

		//C.OCILCRFree(x.ocip.svcp, x.ocip.errp, lcr, C.OCI_DEFAULT)
		return msg, nil
	}
	if status == C.OCI_ERROR {
		errstr, errcode := getError(x.ocip.errp)
		return nil, fmt.Errorf("OCIXStreamOutLCRReceive failed, code:%d, %s", errcode, errstr)
	} else { // status == C.SUCCESS
		s := x.pos2SCN(x.ocip, (*C.ub1)(fetchlwm), fetchlwm_len)
		//C.OCILCRFree(x.ocip.svcp, x.ocip.errp, lcr, C.OCI_DEFAULT)
		return &HeartBeat{SCN: s}, nil
	}
}

func (conn *XStreamConn) getColumnChunkData(owner string) (map[string]interface{}, error) {
	var colname *C.oratext
	var colname_len C.ub2
	var coldty C.ub2
	var col_flags C.oraub8
	var col_csid C.ub2
	var chunk_len C.ub4
	var chunk_ptr *C.ub1
	var row_flag C.oraub8

	var chunkData = map[string]interface{}{}
	status := C.OCIXStreamOutChunkReceive(conn.ocip.svcp, conn.ocip.errp,
		&colname, &colname_len, &coldty,
		&col_flags, &col_csid, &chunk_len,
		&chunk_ptr, &row_flag, C.OCI_DEFAULT)
	for status == C.OCI_SUCCESS {
		column, _ := toStringEnc(colname, C.ushort(colname_len), conn.csid)
		s, err := toStringEnc(chunk_ptr, C.ushort(chunk_len), conn.csid)
		if err != nil {
			return nil, err
		}
		chunkData[column] = s
		if row_flag != 0 && C.OCI_XSTREAM_MORE_ROW_DATA != 0 {
			status = C.OCIXStreamOutChunkReceive(conn.ocip.svcp, conn.ocip.errp,
				&colname, &colname_len, &coldty,
				&col_flags, &col_csid, &chunk_len,
				&chunk_ptr, &row_flag, C.OCI_DEFAULT)
		} else {
			status = -1
		}
	}

	return chunkData, nil
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

func (x *XStreamConn) getLcrRecords(ocip *C.struct_oci, lcr unsafe.Pointer, csid, ncsid int) (Message, error) {
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
		s := x.pos2SCN(ocip, lpos, lposl)
		switch cmd {
		case "COMMIT":
			m := Commit{SCN: s}
			return &m, nil
		case "DELETE":
			stringEnc, err := toStringEnc(oname, onamel, csid)
			if err != nil {
				return nil, err
			}
			m := Delete{SCN: s, Table: stringEnc, Owner: tostring(owner, ownerl)}
			m.OldColumn, m.OldRow, err = getLcrRowData(ocip, lcr, valueTypeOld, csid, ncsid, m.Owner+"."+m.Table)
			return &m, err
		case "INSERT":
			stringEnc, err := toStringEnc(oname, onamel, csid)
			if err != nil {
				return nil, err
			}
			m := Insert{SCN: s, Table: stringEnc, Owner: tostring(owner, ownerl)}
			m.NewColumn, m.NewRow, err = getLcrRowData(ocip, lcr, valueTypeNew, csid, ncsid, m.Owner+"."+m.Table)
			return &m, err
		case "UPDATE":
			stringEnc, err := toStringEnc(oname, onamel, csid)
			if err != nil {
				return nil, err
			}
			m := Update{SCN: s, Table: stringEnc, Owner: tostring(owner, ownerl)}
			m.OldColumn, m.OldRow, err = getLcrRowData(ocip, lcr, valueTypeOld, csid, ncsid, m.Owner+"."+m.Table)
			if err != nil {
				return nil, err
			}
			m.NewColumn, m.NewRow, err = getLcrRowData(ocip, lcr, valueTypeNew, csid, ncsid, m.Owner+"."+m.Table)
			return &m, err
		}
	}
	return nil, nil
}

type valueType C.ub2

var valueTypeOld valueType = C.OCI_LCR_ROW_COLVAL_OLD
var valueTypeNew valueType = C.OCI_LCR_ROW_COLVAL_NEW

var ownerm = make(map[string]columnInfo)

type columnInfo struct {
	count     int
	names     **C.oratext
	namesLens *C.ub2
	inDP      *C.OCIInd
	cSetFP    *C.ub1
	flags     *C.oraub8
}

func getLcrRowData(ocip *C.struct_oci, lcrp unsafe.Pointer, valueType valueType, csid, ncsid int, owner string) ([]string, []interface{}, error) {
	var row *C.oci_lcr_row_t
	var column_length C.ub2
	status := C.get_lcr_row_data(ocip, lcrp, C.ub2(valueType), &row, &column_length)
	if status != C.OCI_SUCCESS {
		errstr, errcode := getError(ocip.errp)
		return nil, nil, fmt.Errorf("get_lcr_row_data failed, code:%d, %s", errcode, errstr)
	} else {
		if status == C.OCI_SUCCESS {
			columnNames := make([]string, 0)
			columnValues := make([]interface{}, 0)

			for i := 0; i < int(column_length); i++ {
				var column_name *C.char
				var column_name_len C.ub2
				var column_value = unsafe.Pointer(nil)
				var column_value_len C.ub2
				var column_csid C.ub2
				var column_data_type C.ub2
				status = C.iterate_row_data(ocip, row, C.ub2(i), &column_name, &column_name_len, &column_value, &column_value_len, &column_csid, &column_data_type)
				if status != C.OCI_SUCCESS {
					errstr, errcode := getError(ocip.errp)
					return nil, nil, fmt.Errorf("iterate_row_data failed, code:%d, %s", errcode, errstr)
				}

				columnNames = append(columnNames, tostring((*C.uchar)(unsafe.Pointer(column_name)), column_name_len))

				csid_l := int(column_csid)
				if csid_l == 0 {
					csid_l = csid
				}
				colValue := value2interface(ocip.errp, (*C.void)(column_value), column_value_len, csid_l, column_data_type)
				columnValues = append(columnValues, colValue)
			}

			C.free_lcr_row_data(row)
			return columnNames, columnValues, nil
		} else {
			errstr, errcode := getError(ocip.errp)
			return nil, nil, fmt.Errorf("get_lcr_row_data failed, code:%d, %s", errcode, errstr)
		}
	}
}

func value2interface(errp *C.OCIError, valuep *C.void, valuelen C.ub2, csid int, dtype C.ub2) interface{} {
	if valuelen == 0 {
		return nil
	}
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
		return ociNumberToInt(errp, v)
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

func getErrorEnc(oci_err *C.OCIError, csid int) (string, int32, error) {
	errCode := C.sb4(0)
	text := [4096]C.text{}
	C.OCIErrorGet(unsafe.Pointer(oci_err), C.uint(1),
		(*C.text)(unsafe.Pointer(nil)), &errCode, (*C.uchar)(unsafe.Pointer(&text)), 4096, C.OCI_HTYPE_ERROR)
	var l int
	for i, c := range text {
		if int(C.ushort(c)) == 0 {
			l = i
			break
		}
	}
	val, err := toStringEnc((*C.uchar)(unsafe.Pointer(&text)), C.ushort(l), csid)
	return val, int32(errCode), err
}

func (x *XStreamConn) pos2SCN(ocip *C.struct_oci, pos *C.ub1, pos_len C.ub2) scn.SCN {
	if pos_len == 0 {
		return 0
	}
	var s C.struct_OCINumber
	var commit_scn C.struct_OCINumber
	var result C.sword
	result = C.OCILCRSCNsFromPosition(ocip.svcp, ocip.errp, pos, pos_len, &s, &commit_scn, C.OCI_DEFAULT)
	if result != C.OCI_SUCCESS {
		// todo
		C.ocierror(ocip, C.CString("OCILCRHeaderGet failed"))
		return 0
	} else {
		return scn.SCN(ociNumberToInt(ocip.errp, &s))
	}
}

func (x *XStreamConn) scn2pos(ocip *C.struct_oci, s scn.SCN) (*cgo.UInt8, C.ub2) {
	var status C.int
	var number *C.OCINumber = ociNumberFromInt(ocip.errp, int64(s))
	var buf = cgo.NewUInt8N(C.OCI_LCR_MAX_POSITION_LEN)
	var posl C.ub2
	if x.lcridVer == V1 {
		status = C.OCILCRSCNToPosition(ocip.svcp, ocip.errp, (*C.ub1)(buf), &posl, number, C.OCI_DEFAULT)
	} else {
		status = C.OCILCRSCNToPosition2(ocip.svcp, ocip.errp, (*C.ub1)(buf), &posl, number, C.OCI_LCRID_V2, C.OCI_DEFAULT)
	}
	if status != C.OCI_SUCCESS {
		// todo
		C.ocierror(ocip, C.CString("OCILCRHeaderGet failed"))
		return nil, 0
	} else {
		return buf, posl
	}
}
