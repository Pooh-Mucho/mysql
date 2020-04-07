// Copyright 2020 huiyi<yi.webmaster@hotmail.com>. All rights reserved.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.
// $Id: zlibcompress_cgo.go

// Using zlib c version to (de)compress traffic packets could got performance improvements and less GC pressure.
//
// Klaus Spot's implementation (https://github.com/klauspost/compress) may be better than the golang standard version.

package mysql

/*
#include <zlib.h>
*/
import "C"

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"unsafe"
)

// Compressed size upper bound
func zlibCompressBound(srcLen int) int {
	return int(C.compressBound(C.ulong(srcLen)))
}

// zlib error messages.
func zlibError(errCode C.int) string {
	var cMsg *C.char = C.zError(errCode)
	return C.GoString(cMsg)
}

// The len(dst) should >= zlibCompressBound(len(src)).
func zlibCompress(dst []byte, src []byte, level int) (int, error) {
	var pDst *C.uchar
	var dstLength C.ulong
	var pSrc *C.uchar
	var srcLength C.ulong
	var zRet C.int
	var hDst *reflect.SliceHeader
	var hSrc *reflect.SliceHeader

	if len(dst) == 0 {
		return 0, errors.New(fmt.Sprint("zlib error[%d]: %s", zRet, zlibError(C.Z_BUF_ERROR)))
	}

	hDst = (*reflect.SliceHeader)(unsafe.Pointer(&dst))
	hSrc = (*reflect.SliceHeader)(unsafe.Pointer(&src))
	pDst = (*C.uchar)(unsafe.Pointer(hDst.Data))
	dstLength = C.ulong(hDst.Len)
	pSrc = (*C.uchar)(unsafe.Pointer(hSrc.Data))
	srcLength = C.ulong(hSrc.Len)

	zRet = C.compress2(pDst, &dstLength, pSrc, srcLength, C.int(level))
	if zRet != C.Z_OK {
		return 0, errors.New(fmt.Sprint("zlib error[%d]: %s", zRet, zlibError(zRet)))
	}
	return int(dstLength), nil
}

// In mysql compress protocol, I always know the uncompressed size of packet.
func zlibDecompress(dst []byte, src []byte) (int, error) {
	var pDst *C.uchar
	var dstLength C.ulong
	var pSrc *C.uchar
	var srcLength C.ulong
	var zRet C.int
	var hDst *reflect.SliceHeader
	var hSrc *reflect.SliceHeader

	if len(dst) == 0 {
		return 0, errors.New(fmt.Sprint("zlib error[%d]: %s", zRet, zlibError(C.Z_BUF_ERROR)))
	}

	hDst = (*reflect.SliceHeader)(unsafe.Pointer(&dst))
	hSrc = (*reflect.SliceHeader)(unsafe.Pointer(&src))
	pDst = (*C.uchar)(unsafe.Pointer(hDst.Data))
	dstLength = C.ulong(hDst.Cap)
	pSrc = (*C.uchar)(unsafe.Pointer(hSrc.Data))
	srcLength = C.ulong(hSrc.Len)

	zRet = C.uncompress(pDst, &dstLength, pSrc, srcLength)
	if zRet != C.Z_OK {
		return 0, errors.New(fmt.Sprint("zlib error[%d]: %s", zRet, zlibError(zRet)))
	}
	return int(dstLength), nil
}

type cgoZLibCompressor struct{}

type cgoZLibDecompressor struct{}

func (zc *cgoZLibCompressor) compress(input []byte, output *bytes.Buffer) (int, error) {
	var err error
	var buffer []byte
	var compressed []byte
	var compressedLength int
	var compressBound int = zlibCompressBound(len(input))

	// Allocate enough memory
	output.Grow(compressBound)

	// Get the internal buffer
	buffer = output.Bytes()
	compressed = buffer[len(buffer):cap(buffer)]

	// Compress input
	compressedLength, err = zlibCompress(compressed, input, defaultPacketCompressLevel)
	if err != nil {
		return 0, err
	}

	// Grow len of output.
	// bytes.Buffer's Write using copy(), which will do nothing when dst and src are same.
	output.Write(compressed[:compressedLength])

	return len(compressed), nil
}

// In mysql compress protocol, we always know the decompressed length for input
func (zd *cgoZLibDecompressor) decompress(input []byte, output []byte) error {
	var err error
	_, err = zlibDecompress(output, input)
	return err
}
