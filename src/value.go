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
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// 用于解析存储目录下的.id结尾的文件信息
type value struct {
	dir string // 存储目录
	ext string // 扩展名
	v1  uint64 // cid - cluster id
	v2  uint64 // nid - node id
}

func openValue(dir, ext string) (*value, error) {
	// func Glob(pattern string) (matches []string, err error)
	// 根据通配符匹配获取目录下的文件列表
	matches, err := filepath.Glob(filepath.Join(dir, "*"+ext))
	if err != nil {
		return nil, err
	}
	if len(matches) == 0 {
		// 如果该文件不存在，则创建一个
		f, err := os.OpenFile(valueFile(dir, ext, 0, 0), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
		if err != nil {
			return nil, err
		}
		if err := f.Close(); err != nil {
			return nil, err
		}
		if err := syncDir(dir); err != nil {
			return nil, err
		}
		matches = []string{f.Name()}
	}
	// ext结尾的文件在存储目录下只能有一个
	if len(matches) != 1 {
		return nil, fmt.Errorf("raft: more than one file with ext %s in dir %s", ext, dir)
	}
	// 以文件名 1234-3.id为例
	// 移除.id即为 1234-3
	// func TrimSuffix(s, suffix string) string
	s := strings.TrimSuffix(filepath.Base(matches[0]), ext)
	// 获取 - 字符所在的索引
	// func IndexByte(s string, c byte) int
	i := strings.IndexByte(s, '-')
	if i == -1 {
		return nil, fmt.Errorf("raft: invalid value file %s", matches[0])
	}
	// 索引前的部分为cid，索引后的部分为nid，都需要转换为10进制的int64类型数据
	// func ParseInt(s string, base int, bitSize int) (i int64, err error)
	v1, err := strconv.ParseInt(s[:i], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("raft: invalid value file %s", matches[0])
	}
	v2, err := strconv.ParseInt(s[i+1:], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("raft: invalid value file %s", matches[0])
	}
	return &value{
		dir: dir,
		ext: ext,
		v1:  uint64(v1),
		v2:  uint64(v2),
	}, nil
}

func (v *value) get() (uint64, uint64) {
	return v.v1, v.v2
}

func (v *value) set(v1, v2 uint64) error {
	// v1 - cid - cluster id
	// v2 - nid - node id
	if v1 == v.v1 && v2 == v.v2 {
		return nil
	}
	curPath := valueFile(v.dir, v.ext, v.v1, v.v2)
	newPath := valueFile(v.dir, v.ext, v1, v2)
	if err := os.Rename(curPath, newPath); err != nil {
		return err
	}
	if err := syncDir(v.dir); err != nil {
		return err
	}
	v.v1, v.v2 = v1, v2
	return nil
}

func valueFile(dir, ext string, v1, v2 uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%d-%d%s", v1, v2, ext))
}
