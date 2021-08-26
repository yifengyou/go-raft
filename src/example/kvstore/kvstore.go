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

package main

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"io"

	"github.com/santhosh-tekuri/raft"
)

// TODO: sync.RWMutex
// 简单的缓存服务器，用一个map来保存数据，提供get、set、del操作
type kvStore struct {
	data map[string]string
}

func newKVStore() *kvStore {
	return &kvStore{make(map[string]string)}
}

var _ raft.FSM = (*kvStore)(nil)

// type FSM interface
// 应用程序kvStore实现FSM接口
// 为何数据读写不加锁？
func (s *kvStore) Update(b []byte) interface{} {
	cmd, err := decodeCmd(b)
	if err != nil {
		return err
	}
	switch cmd := cmd.(type) {
	case set:
		// 需不需要加锁？
		s.data[cmd.Key] = cmd.Val
		return nil
	case del:
		delete(s.data, cmd.Key)
		return nil
	}
	return fmt.Errorf("unknown cmd: %T", cmd)
}

// type FSM interface
func (s *kvStore) Read(cmd interface{}) interface{} {
	switch cmd := cmd.(type) {
	case get:
		return s.data[cmd.Key]
	}
	return fmt.Errorf("unknown cmd: %T", cmd)
}

//type FSM interface
func (s *kvStore) Snapshot() (raft.FSMState, error) {
	data := make(map[string]string)
	for k, v := range s.data {
		data[k] = v
	}
	return &kvState{data}, nil
}

//type FSM interface
func (s *kvStore) Restore(r io.Reader) error {
	var data map[string]string
	if err := gob.NewDecoder(r).Decode(&data); err != nil {
		return err
	}
	s.data = data
	return nil
}

// commands --------------------------------------------------

type cmdType byte

const (
	cmdSet cmdType = iota
	cmdDel
)

type set struct {
	Key, Val string
}

type get struct {
	Key string
}

type del struct {
	Key string
}

// kvStore类型每次调用Update时，需要decodeCmd
// func (s *kvStore) Update(b []byte) interface{}
func decodeCmd(b []byte) (interface{}, error) {
	if len(b) == 0 {
		return nil, errors.New("no data")
	}
	decoder := gob.NewDecoder(bytes.NewReader(b[1:]))
	switch cmdType(b[0]) {
	case cmdSet:
		cmd := set{}
		if err := decoder.Decode(&cmd); err != nil {
			return nil, err
		}
		return cmd, nil
	case cmdDel:
		cmd := del{}
		if err := decoder.Decode(&cmd); err != nil {
			return nil, err
		}
		return cmd, nil
	default:
		return nil, fmt.Errorf("unknown cmd: %d", b[0])
	}
}

// raft.UpdateFSM(encodeCmd(set{key, string(b)}))
// raft.UpdateFSM(encodeCmd(del{key}))
func encodeCmd(cmd interface{}) []byte {
	var typ cmdType
	switch cmd.(type) {
	case set:
		typ = cmdSet
	case del:
		typ = cmdDel
	default:
		panic(fmt.Errorf("encodeCmd: %T", cmd))
	}
	buf := new(bytes.Buffer)
	buf.WriteByte(byte(typ))
	if err := gob.NewEncoder(buf).Encode(cmd); err != nil {
		panic(err)
	}
	return buf.Bytes()
}

// state --------------------------------------------------

type kvState struct {
	data map[string]string
}

// 这个变量没卵用
var _ raft.FSMState = (*kvState)(nil)

// type FSMState interface
// 应用程序kvStore实现FSMState接口
func (s *kvState) Persist(w io.Writer) error {
	return gob.NewEncoder(w).Encode(s.data)
}

// type FSMState interface
// 应用程序kvStore实现FSMState接口
func (s *kvState) Release() {}
