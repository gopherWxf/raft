package raft

import (
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/rpc"
	"strconv"
)

func (rf *Raft) httpListen() {
	//创建getRequest()回调方法
	http.HandleFunc("/req", rf.getRequest)
	port, _ := strconv.Atoi(rf.node.Port[1:])
	port += 2000
	fmt.Println("监听", strconv.Itoa(port), "端口")
	http.ListenAndServe("127.0.0.1:"+strconv.Itoa(port), nil)
}

func (rf *Raft) getRequest(writer http.ResponseWriter, request *http.Request) {
	defer request.Body.Close()
	err := request.ParseForm()
	if err != nil {
		log.Panicln(err)
	}
	if len(request.Form["message"]) > 0 && rf.currentLeader != "-1" {
		message := request.Form["message"][0]
		//封装消息
		m := new(Message)
		m.MsgBody = message
		m.MsgID = getRandom()

		//连接leader节点的RPC
		port := nodePool[rf.currentLeader]
		conn, err := rpc.DialHTTP("tcp", "127.0.0.1"+port)
		if err != nil {
			log.Panicln(err)
		}
		//接收到消息后，直接转发到领导者
		var bo bool
		err = conn.Call("Raft.LeaderReceiveMessage", m, &bo)
		if err != nil {
			log.Panicln(err)
		}
		fmt.Println("消息是否已发送到领导者：", bo)
		writer.Write([]byte("ok!"))
	}
}

//返回一个十位数的随机数，作为msg.id
func getRandom() int {
	id := rand.Intn(1000000000) + 1000000000
	return id
}
