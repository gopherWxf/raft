package raft

import (
	"log"
	"os"
)

//定义节点数量
var nodeCount = 3

//节点池
var nodePool map[string]string

//选举超时时间（秒）
var electionTimeout = 3

//心跳检测超时时间
var heartBeatTimeout = 7

//心跳检测频率（秒）
var heartBeatRate = 3

// MessageStore 存储信息
var MessageStore = make(map[int]string)

func Start(nodeNum int, nodeTable map[string]string) {
	nodeCount = nodeNum
	nodePool = nodeTable

	if len(os.Args) < 1 {
		log.Panicln("缺少程序运行的参数")
	}

	//即 A、B、C其中一个
	id := os.Args[1]

	//传入节点编号，端口号，创建raft实例
	raft := NewRaft(id, nodePool[id])

	//注册rpc服务绑定http协议上开启监听
	go rpcRegister(raft)

	//发送心跳包
	go raft.sendHeartPacket()

	//开启一个Http监听client发来的信息
	go raft.httpListen()

	//尝试成为候选人并选举
	go raft.tryToBeCandidateWithElection()

	//进行心跳超时检测
	go raft.heartTimeoutDetection()

	//TODO
	select {}
}
