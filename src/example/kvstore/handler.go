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
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/santhosh-tekuri/raft"
)

// 定义raft节点处理句柄
type handler struct {
	r *raft.Raft
}

func (h handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// 将请求路径据'/'分割为字符串切片
	// func Split(s, sep string) []string
	parts := strings.Split(r.URL.Path, "/")
	// curl -v localhost:8001/k1  则r.URL.Path="/k1"
	if len(parts) != 2 || parts[1] == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	// r.URL.Path="/k1" 则k1表示key。key在rest请求不可缺少
	key := parts[1]
	switch r.Method {
	case http.MethodGet:
		// 若为get请求，则获取key值
		// curl http://localhost:8001/k1
		task := raft.ReadFSM(get{key})
		if _, ok := r.URL.Query()["dirty"]; ok {
			// 如果有dirty参数，那么默认采用脏读方式读取
			// 此任务可以提交给非选民
			task = raft.DirtyReadFSM(get{key})
		}
		// 提交task执行
		res, err := h.execute(task)
		if err != nil {
			// 可能非leader节点，则重定向到leader节点获取数据
			h.replyErr(w, r, err)
		} else {
			// 执行ok，正常返回
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(res.(string)))
		}
	case http.MethodPost:
		// 若为post请求，其中数据为value，key=value。相当于赋值操作
		// curl -X POST localhost:8001/k1 -d v1
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			// 极少会错误吧
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		// 直接提交任务
		_, err = h.execute(raft.UpdateFSM(encodeCmd(set{key, string(b)})))
		if err != nil {
			// 可能非leader节点，则重定向到leader节点获取数据
			h.replyErr(w, r, err)
		} else {
			// 执行成功，只返回状态码 204
			w.WriteHeader(http.StatusNoContent)
		}
	case http.MethodDelete:
		// 如果是DELETE请求，则删除键值
		// curl -X DELETE  localhost:8001/k1
		// raft.UpdateFSM(encodeCmd(del{key}))
		_, err := h.execute(raft.UpdateFSM(encodeCmd(del{key})))
		if err != nil {
			// 可能非leader节点，则重定向到leader节点获取数据
			h.replyErr(w, r, err)
		} else {
			// 执行成功，只返回状态码 204
			w.WriteHeader(http.StatusNoContent)
		}
	default:
		//其他rest类型请求直接报错返回 405
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (h handler) replyErr(w http.ResponseWriter, r *http.Request, err error) {
	// 若当前raft节点非leader，则重定向到leader
	if err, ok := err.(raft.NotLeaderError); ok && err.Leader.ID != 0 {
		url := fmt.Sprintf("http://%s%s", err.Leader.Data, r.URL.Path)
		// func Redirect(w ResponseWriter, r *Request, url string, code int)
		http.Redirect(w, r, url, http.StatusPermanentRedirect)
		return
	}
	//否则为内部服务错误
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusInternalServerError) // 500
	w.Write([]byte(err.Error()))
}

func (h handler) execute(t raft.FSMTask) (interface{}, error) {
	select {
	case <-h.r.Closed():
		return nil, raft.ErrServerClosed
	// 传递任务t
	case h.r.FSMTasks() <- t:
	}
	<-t.Done()
	return t.Result(), t.Err()
}
