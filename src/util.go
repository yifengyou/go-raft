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
	crand "crypto/rand"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"math/big"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

func min(a, b uint64) uint64 {
	if a <= b {
		return a
	}
	return b
}

func safeClose(ch chan struct{}) {
	select {
	case <-ch:
	default:
		close(ch)
	}
}

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func syncDir(dir string) error {
	// 如果是windows系统，没有sync的要求，直接返回
	if runtime.GOOS == "windows" {
		return nil
	}
	// 只读方式打开目录
	// func Open(name string) (*File, error)
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	// func (f *File) Sync() error
	// 类似于sync命令，缓存落盘
	err = d.Sync()
	// func (f *File) Close() error
	e := d.Close()
	if err != nil {
		return err
	}
	return e
}

func size(buffs net.Buffers) int64 {
	var size int64
	for _, buff := range buffs {
		size += int64(len(buff))
	}
	return size
}

// safeTimer ------------------------------------------------------

type safeTimer struct {
	timer *time.Timer
	C     <-chan time.Time

	// active is true if timer is started, but not yet received from channel.
	// NOTE: must be set to false, after receiving from channel
	active bool
}

// newSafeTimer creates stopped timer
func newSafeTimer() *safeTimer {
	t := time.NewTimer(0)
	if !t.Stop() {
		<-t.C
	}
	return &safeTimer{t, t.C, false}
}

func (t *safeTimer) stop() {
	if !t.timer.Stop() {
		if t.active {
			<-t.C
		}
	}
	t.active = false
}

func (t *safeTimer) reset(d time.Duration) {
	t.stop()
	t.timer.Reset(d)
	t.active = true
}

// backOff ------------------------------------------------

const (
	maxFailureScale = 12
	failureWait     = 10 * time.Millisecond
)

// backOff is used to compute an exponential backOff
// duration. Base time is scaled by the current round,
// up to some maximum scale factor. If backOff exceeds
// given max, it returns max
func backOff(round uint64, max time.Duration) time.Duration {
	base, limit := failureWait, uint64(maxFailureScale)
	power := min(round, limit)
	for power > 2 {
		base *= 2
		power--
	}
	if base > max {
		return max
	}
	return base
}

// randTime -----------------------------------------------------------------

type randTime struct {
	r *rand.Rand
}

func newRandTime() randTime {
	var seed int64
	if r, err := crand.Int(crand.Reader, big.NewInt(math.MaxInt64)); err != nil {
		seed = time.Now().UnixNano()
	} else {
		seed = r.Int64()
	}
	return randTime{rand.New(rand.NewSource(seed))}
}

func (rt randTime) duration(min time.Duration) time.Duration {
	return min + time.Duration(rt.r.Int63())%min
}

func (rt randTime) deadline(min time.Duration) time.Time {
	return time.Now().Add(rt.duration(min))
}

func (rt randTime) after(min time.Duration) <-chan time.Time {
	return time.After(rt.duration(min))
}

// -------------------------------------------------------------------------

func lockDir(dir string) error {
	// func Abs(path string) (string, error)
	// 获取绝对路径
	dir, err := filepath.Abs(dir)
	if err != nil {
		return fmt.Errorf("raft.lockDir: %v", err)
	}
	// dir目录下创建临时文件
	// func TempFile(dir, pattern string) (f *os.File, err error)
	tempFile, err := ioutil.TempFile(dir, "lock*.tmp")
	if err != nil {
		return fmt.Errorf("raft.lockDir: %v", err)
	}
	defer func() {
		// 关闭锁文件，并移除
		_ = tempFile.Close()
		// func (f *File) Name() string
		_ = os.Remove(tempFile.Name())
	}()
	// func WriteString(w Writer, s string) (n int, err error)
	// 将进程pid写到锁文件中
	if _, err := io.WriteString(tempFile, fmt.Sprintf("%d\n", os.Getpid())); err != nil {
		return fmt.Errorf("raft.lockDir: %v", err)
	}
	// 真正的锁文件路径
	lockFile := filepath.Join(dir, "lock")
	// 将创建的 lock*.tmp 的硬链接，路径即为lockFile
	// func Link(oldname, newname string) error
	if err := os.Link(tempFile.Name(), lockFile); err != nil {
		// 如果锁已经存在，返回锁存在错误
		if os.IsExist(err) {
			// type plainError string
			// ErrLockExists = plainError("raft: lock file exists in storageDir")
			return ErrLockExists
		}
		// 否则，创建error错误返回
		// func Errorf(format string, a ...interface{}) error
		return fmt.Errorf("raft.lockDir: %v", err)
	}
	// func Lstat(name string) (FileInfo, error)
	// os.lstat() 方法用于类似 stat() 返回文件的信息,但是解析符号链接
	tempInfo, err := os.Lstat(tempFile.Name())
	if err != nil {
		return fmt.Errorf("raft.lockDir: %v", err)
	}
	// 重新检查一遍锁文件
	lockInfo, err := os.Lstat(lockFile)
	if err != nil {
		return fmt.Errorf("raft.lockDir: %v", err)
	}
	// func SameFile(fi1, fi2 FileInfo) bool
	// 如果两个文件不相同，则锁异常
	if !os.SameFile(tempInfo, lockInfo) {
		return ErrLockExists
	}
	return nil
}

func unlockDir(dir string) error {
	// 移除目录中的锁文件即为解锁目录
	return os.RemoveAll(filepath.Join(dir, "lock"))
}

// -------------------------------------------------------------------------

type decrUint64Slice []uint64
func (s decrUint64Slice) Len() int           { return len(s) }
func (s decrUint64Slice) Less(i, j int) bool { return s[i] > s[j] }
func (s decrUint64Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

// -------------------------------------------------------------------------

func durationFor(bandwidth int64, n int64) time.Duration {
	seconds := float64(n) / float64(bandwidth)
	return time.Duration(1e9 * seconds)
}

// -------------------------------------------------------------------------

func trimPrefix(err error) string {
	return strings.TrimPrefix(err.Error(), "raft: ")
}
