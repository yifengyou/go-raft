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

// Package kvstore demonstrates how to use raft package to implement simple kvstore.
//
//   usage: kvstore <storage-dir> <raft-addr> <http-addr>
//
//   storage-dir: where raft stores its data. Created if the directory does not exist
//   raft-addr:   raft server listens on this address
//   http-addr:   http server listens on this address
//
// in addition to this you should pass cluster-id and node-id using environment
// variables CID, NID respectively. value is uint64 greater than zero.
//
// Launch 3 nodes as shown below:
//   $ CID=1234 NID=1 kvstore data1 localhost:7001 localhost:8001
//   $ CID=1234 NID=2 kvstore data2 localhost:7002 localhost:8002
//   $ CID=1234 NID=3 kvstore data3 localhost:7003 localhost:8003
//
// Note: we are using Node.Data to store http-addr, which enables us to enable http redirects.
//
// to bootstrap cluster:
//  $ RAFT_ADDR=localhost:7001 ./raftctl config apply \
//       +nid=1,voter=true,addr=localhost:7001,data=localhost:8001 \
//       +nid=2,voter=true,addr=localhost:7002,data=localhost:8002 \
//       +nid=3,voter=true,addr=localhost:7003,data=localhost:8003
//  $ RAFT_ADDR=localhost:7001 ./raftctl config get
//
// to test kvstore:
//   $ curl -v -X POST localhost:8001/k1 -d v1
//   $ curl -v localhost:8001/k1
//   $ curl -v -L localhost:8002/k1
//   $ curl -v 'localhost:8002/k1?dirty'
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/santhosh-tekuri/raft"
)

func main() {
	// 必须要有四个参数（包括命令本身）
	if len(os.Args) != 4 {
		errln("usage: kvstore <storage-dir> <raft-addr> <http-addr>")
		os.Exit(1)
	}
	// 参数有序，依次是 存储路径，监听raft通信地址:端口，监听的http通信地址:端口
	storageDir, raftAddr, httpAddr := os.Args[1], os.Args[2], os.Args[3]
	// func MkdirAll(path string, perm FileMode) error
	// 根据存储路径创建目录。如果目录已经存在，不会清空继续使用，但是权限不会修改
	if err := os.MkdirAll(storageDir, 0700); err != nil {
		panic(err)
	}
	// CID = cluster-id  NID = node-id 两个值都必须是非0正整数
	// 检索环境变量中是否存在CID、NID，如果不存在直接退出。这两变量也是不可缺少的参数
	// 例如 CID=1234 NID=1 ./kvstore data1 localhost:7001 localhost:8001
	// shell特点，变量可以放在命令前。则变量作用域仅当前命令。注入到bash子命令
	// 父进程不会保存这些临时变量，仅子进程中使用
	cid, nid := lookupEnv("CID"), lookupEnv("NID")
	// 在存储目录下以特定文件名作为仓库标识，来判断存储目录是否可以复用
	if err := raft.SetIdentity(storageDir, cid, nid); err != nil {
		panic(err)
	}

	// 初始化key value数据
	store := newKVStore()
	// 初始化raft默认参数，返回Options参数结构体
	// TODO : 这里应该支持从配置文件更新配置
	opt := raft.DefaultOptions()
	// 根据 默认参数 + frontend内存缓存区 + backend存储持久化 创建一个raft节点
	// 关键句柄r，后续node操作都会用到r数据及其方法
	r, err := raft.New(opt, store, storageDir)
	if err != nil {
		panic(err)
	}

	// 开启http请求监听
	// func ListenAndServe(addr string, handler Handler) error
	// httpAddr = "localhost:8001"
	// handler结构需要实现接口 ServeHTTP(ResponseWriter, *Request)
	go http.ListenAndServe(httpAddr, handler{r})

	// always shutdown raft, otherwise lock file remains in storageDir
	// 结束时务必要执行锁清理，否则仓库无法复用
	go func() {
		ch := make(chan os.Signal, 2)
		// func Notify(c chan<- os.Signal, sig ...os.Signal)
		//Interrupt Signal = syscall.SIGINT
		//Kill      Signal = syscall.SIGKILL
		// os.Interrupt 等价于 syscall.SIGINT
		signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
		// 阻塞在通道，知道有信号到达
		<-ch
		// 结束阻塞时，Shutdown关闭raft节点
		_ = r.Shutdown(context.Background())
	}()

	// 启动raft集群通信监听
	err = r.ListenAndServe(raftAddr)
	if err != raft.ErrServerClosed && err != raft.ErrNodeRemoved {
		panic(err)
	}
	fmt.Printf("Normal shutdown raft node %s-%s\n", cid, nid)
}

func lookupEnv(key string) uint64 {
	// 检索环境变量
	// func LookupEnv(key string) (string, bool)
	s, ok := os.LookupEnv(key)
	if !ok {
		errln("environment variable", key, "not set")
		// 若环境变量不存在，则直接退出进程
		os.Exit(1)
	}
	// 将字符串转为10进制的int64位整数
	// func ParseInt(s string, base int, bitSize int) (i int64, err error)
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		panic(err)
	}
	// int64强转为uint64，因为不存在负数编号的节点
	return uint64(i)
}

func errln(v ...interface{}) {
	// 格式化输出到标准错误输出os.Stderr
	// Stderr = NewFile(uintptr(syscall.Stderr), "/dev/stderr")
	// func Fprintln(w io.Writer, a ...interface{}) (n int, err error)
	// Write(p []byte) (n int, err error)
	_, _ = fmt.Fprintln(os.Stderr, v...)
}
