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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// todo: add md5 check
type snapshots struct {
	dir    string
	retain int

	mu    sync.RWMutex
	index uint64
	term  uint64

	usedMu sync.RWMutex
	used   map[uint64]int // map[index]numUses
}

func openSnapshots(dir string, opt Options) (*snapshots, error) {
	// 创建snapshot目录，如果已经存在则不会创建
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	// 检索存储目录，子目录snapshot下的.meta文件
	snaps, err := findSnapshots(dir)
	if err != nil {
		return nil, err
	}
	s := &snapshots{
		dir:    dir,
		// DefaultOptions 中定义 SnapshotsRetain=1
		retain: opt.SnapshotsRetain,
		used:   make(map[uint64]int),
	}
	if len(snaps) > 0 {
		// 如果有多个.meta，只取第一个，存放在s.index
		s.index = snaps[0]
		// 关键：从.meta中提取快照信息
		meta, err := s.meta()
		if err != nil {
			return nil, err
		}
		// 任期调整为快照任期
		s.term = meta.term
	}
	// 若没有状态快照，则重新开始，任期从0开始
	return s, nil
}

func (s *snapshots) latest() (index, term uint64) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.index, s.term
}

func (s *snapshots) meta() (snapshotMeta, error) {
	if s.index == 0 {
		return snapshotMeta{index: 0, term: 0}, nil
	}
	// 打开s.index对应的meta文件
	// filepath.Join(dir, fmt.Sprintf("%d.meta", index))
	f, err := os.Open(metaFile(s.dir, s.index))
	if err != nil {
		return snapshotMeta{}, err
	}
	defer f.Close()
	meta := snapshotMeta{}
	// 关键：解析.meta文件内容，写到meta中
	return meta, meta.decode(f)
}

func (s *snapshots) applyRetain() error {
	snaps, err := findSnapshots(s.dir)
	if err != nil {
		return err
	}
	s.usedMu.RLock()
	defer s.usedMu.RUnlock()
	for i, index := range snaps {
		if i >= s.retain && s.used[index] == 0 {
			if e := os.Remove(metaFile(s.dir, index)); e == nil {
				if e := os.Remove(snapFile(s.dir, index)); err == nil {
					err = e
				}
			} else if err == nil {
				err = e
			}
		}
	}
	return err
}

// snapshot ----------------------------------------------------

func (s *snapshots) open() (*snapshot, error) {
	meta, err := s.meta()
	if err != nil {
		return nil, err
	}
	file := snapFile(s.dir, meta.index)

	// validate file size
	info, err := os.Stat(file)
	if err != nil {
		return nil, err
	}
	if info.Size() != meta.size {
		return nil, fmt.Errorf("raft: size of %q is %d, want %d", file, info.Size(), meta.size)
	}

	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	s.used[meta.index]++
	return &snapshot{
		snaps: s,
		meta:  meta,
		file:  f,
	}, nil
}

type snapshot struct {
	snaps *snapshots
	meta  snapshotMeta
	file  *os.File
}

func (s *snapshot) release() {
	_ = s.file.Close()
	s.snaps.usedMu.Lock()
	defer s.snaps.usedMu.Unlock()
	if s.snaps.used[s.meta.index] == 1 {
		delete(s.snaps.used, s.meta.index)
	} else {
		s.snaps.used[s.meta.index]--
	}
}

// snapshotSink ----------------------------------------------------

func (s *snapshots) new(index, term uint64, config Config) (*snapshotSink, error) {
	f, err := os.Create(snapFile(s.dir, index))
	if err != nil {
		return nil, err
	}
	return &snapshotSink{
		snaps: s,
		meta:  snapshotMeta{index: index, term: term, config: config},
		file:  f,
	}, nil
}

type snapshotSink struct {
	snaps *snapshots
	meta  snapshotMeta
	file  *os.File
}

func (s *snapshotSink) done(err error) (snapshotMeta, error) {
	if err != nil {
		_ = s.file.Close()
		_ = os.Remove(s.file.Name())
		return s.meta, err
	}
	defer func() {
		if err != nil {
			_ = os.Remove(s.file.Name())
		}
	}()
	if err = s.file.Close(); err != nil {
		return s.meta, err
	}
	info, err := os.Stat(s.file.Name())
	if err != nil {
		return s.meta, err
	}
	s.meta.size = info.Size()

	file := filepath.Join(s.snaps.dir, "meta.tmp")
	temp, err := os.OpenFile(file, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return s.meta, err
	}
	defer func() {
		if temp != nil {
			_ = temp.Close()
			_ = os.RemoveAll(temp.Name())
		}
	}()
	if err = s.meta.encode(temp); err != nil {
		return s.meta, err
	}
	if err = temp.Close(); err != nil {
		return s.meta, err
	}
	file = metaFile(s.snaps.dir, s.meta.index)
	if err = os.Rename(temp.Name(), file); err != nil {
		return s.meta, err
	}
	temp = nil
	s.snaps.mu.Lock()
	s.snaps.index, s.snaps.term = s.meta.index, s.meta.term
	s.snaps.mu.Unlock()
	_ = s.snaps.applyRetain() // todo: trace error
	return s.meta, nil
}

// snapshotMeta ----------------------------------------------------

type snapshotMeta struct {
	index  uint64
	term   uint64
	config Config
	size   int64
}

func (m *snapshotMeta) encode(w io.Writer) error {
	if err := writeUint64(w, m.index); err != nil {
		return err
	}
	if err := writeUint64(w, m.term); err != nil {
		return err
	}
	if err := m.config.encode().encode(w); err != nil {
		return err
	}
	return writeUint64(w, uint64(m.size))
}

func (m *snapshotMeta) decode(r io.Reader) (err error) {
	// 读取64位，小端序，uint64，作为index
	if m.index, err = readUint64(r); err != nil {
		return err
	}
	// 读取64位，小端序，uint64，作为term
	if m.term, err = readUint64(r); err != nil {
		return err
	}
	// 创建条目，读取，decode
	e := &entry{}
	if err = e.decode(r); err != nil {
		return err
	}
	if err = m.config.decode(e); err != nil {
		return err
	}
	size, err := readUint64(r)
	if err != nil {
		return err
	}
	m.size = int64(size)
	return nil
}

// helpers ----------------------------------------------------

func metaFile(dir string, index uint64) string {
	// 根据所给index定位到.meta所在路径
	return filepath.Join(dir, fmt.Sprintf("%d.meta", index))
}
func snapFile(dir string, index uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%d.snap", index))
}

// findSnapshots returns list of snapshots from latest to oldest
func findSnapshots(dir string) ([]uint64, error) {
	// func Glob(pattern string) (matches []string, err error)
	// 模式匹配获取meta文件
	matches, err := filepath.Glob(filepath.Join(dir, "*.meta"))
	if err != nil {
		return nil, err
	}
	var snaps []uint64
	for _, m := range matches {
		m = filepath.Base(m)
		m = strings.TrimSuffix(m, ".meta")
		i, err := strconv.ParseUint(m, 10, 64)
		if err != nil {
			return nil, err
		}
		snaps = append(snaps, i)
	}
	// type decrUint64Slice []uint64
	// 使用快排排序snpas，从大到小
	// 此处sort包的排序，data必须实现Interface接口的方法
	// decrUint64Slice 就是对Interface接口的实现
	// 原地调整，没有返回值
	// func Sort(data Interface)
	sort.Sort(decrUint64Slice(snaps))
	return snaps, nil
}
