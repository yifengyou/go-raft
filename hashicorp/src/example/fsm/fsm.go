package fsm

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/hashicorp/raft"
)

type database map[string]string

func (d database) Persist(sink raft.SnapshotSink) error {
	data, err := json.Marshal(d)
	if err != nil {
		return err
	}
	sink.Write(data)
	sink.Close()
	return nil
}

func (d database) Release() {}

// Fsm : finite state machine 有限状态机
type Fsm struct {
	mu   sync.Mutex
	Data database
}

// Apply : 当raft内部（更新）commit了一个log entry后，会记录在上面说过的logStore里面，
// 被commit的log entry需要被执行（提交），就stcache来说，执行log entry就是把数据写入缓存，
// 即执行set操作。我们改造doSet方法， 这里不再直接写缓存，而是调用raft的Apply方式，
// 为这次set操作生成一个log entry，这里面会根据raft的内部协议，在各个节点之间进行通信协作，
// 确保最后这条log 会在整个集群的节点里面提交或者失败。
func (f *Fsm) Apply(l *raft.Log) interface{} {
	fmt.Println("apply data:", string(l.Data))
	data := strings.Split(string(l.Data), ",")
	op := data[0]
	f.mu.Lock()
	if op == "set" {
		key := data[1]
		value := data[2]
		f.Data[key] = value
	}
	f.mu.Unlock()

	return nil
}

// Snapshot : 生成一个快照结构
func (f *Fsm) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.Data, nil
}

// Restore : 从快照中回复
func (f *Fsm) Restore(io.ReadCloser) error {
	return nil
}
