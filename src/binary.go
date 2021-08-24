// Copyright 2019 Santhosh Kumar Tekuri
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"encoding/binary"
	"io"
)

// byteOrder used for encode/decode
// var LittleEndian littleEndian
// type littleEndian struct{}
var byteOrder = binary.LittleEndian

func readUint64(r io.Reader) (uint64, error) {
	b := make([]byte, 8)
	if _, err := io.ReadFull(r, b); err != nil {
		return 0, err
	}
	return byteOrder.Uint64(b), nil
}

func readUint32(r io.Reader) (uint32, error) {
	// 创建4字节容量的byte切片
	b := make([]byte, 4)
	// r调用Read()读取4字节放到b中
	if _, err := io.ReadFull(r, b); err != nil {
		return 0, err
	}
	// 返回小端序，网络默认是大端序列
	// func (littleEndian) Uint32(b []byte) uint32
	return byteOrder.Uint32(b), nil
}

func readUint8(r io.Reader) (uint8, error) {
	if r, ok := r.(io.ByteReader); ok {
		return r.ReadByte()
	}
	b := make([]byte, 1)
	if _, err := io.ReadFull(r, b); err != nil {
		return 0, err
	}
	return b[0], nil
}

func readBool(r io.Reader) (bool, error) {
	b, err := readUint8(r)
	return b > 0, err
}

func readBytes(r io.Reader) ([]byte, error) {
	// 获取小端序 uint32，数据封装，开头表示数据大小
	size, err := readUint32(r)
	if err != nil {
		return nil, err
	}
	// 创建size大小的byte切片
	b := make([]byte, size)
	// 从r中调用Read读取b容量大小的数据
	if _, err := io.ReadFull(r, b); err != nil {
		return nil, err
	}
	// 此处读取为何不考虑大小端序列？
	return b, nil
}

func readString(r io.Reader) (string, error) {
	// 从conn中读取数据，函数要考虑黏包问题以及大小端问题
	b, err := readBytes(r)
	return string(b), err
}

// -----------------------------------------------------

func writeUint64(w io.Writer, v uint64) error {
	b := make([]byte, 8)
	byteOrder.PutUint64(b, v)
	_, err := w.Write(b)
	return err
}

func writeUint32(w io.Writer, v uint32) error {
	b := make([]byte, 4)
	byteOrder.PutUint32(b, v)
	_, err := w.Write(b)
	return err
}

func writeUint8(w io.Writer, v uint8) error {
	if w, ok := w.(io.ByteWriter); ok {
		return w.WriteByte(v)
	}
	b := []byte{v}
	_, err := w.Write(b)
	return err
}

func writeBool(w io.Writer, v bool) error {
	if v {
		return writeUint8(w, 1)
	}
	return writeUint8(w, 0)
}

func writeBytes(w io.Writer, b []byte) error {
	if err := writeUint32(w, uint32(len(b))); err != nil {
		return err
	}
	_, err := w.Write(b)
	return err
}

func writeString(w io.Writer, s string) error {
	if err := writeUint32(w, uint32(len(s))); err != nil {
		return err
	}
	// func WriteString(w Writer, s string) (n int, err error)
	_, err := io.WriteString(w, s)
	return err
}
