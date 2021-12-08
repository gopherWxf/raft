package raft

import (
	"fmt"
	"log"
	"os"
	"time"
)

//定义节点数量
var nodeCount = 3

//节点池
var nodePool map[string]string

//选举超时时间（秒）
var timeout = 3

//心跳检测超时时间
var heartBeatTimeout = 7

//心跳检测频率（秒）
var heartBeatTimes = 3

// MessageStore 存储信息
var MessageStore = make(map[int]string)

func Start(nodeNum int, nodeTable map[string]string) {
	nodeCount = nodeNum
	nodePool = nodeTable

	if len(os.Args) < 1 {
		log.Panicln("缺少程序运行的参数")
	}
	id := os.Args[1] //即 A、B、C其中一个
	//传入节点编号，端口号，创建raft实例
	raft := NewRaft(id, nodePool[id])
	//启用RPC,注册raft
	go rpcRegister(raft)
	//开启心跳检测
	go raft.heartbeat()

	//开启一个Http监听
	go raft.httpListen()

	//开启选举
	election := func() {
		for {
			//成为候选人节点
			if raft.becomeCandidate() {
				//成为后选人节点后 向其他节点要选票来进行选举
				if raft.election() {
					break
				} else {
					continue //领导者选举超时,重新称为候选人进行选举
				}
			} else {
				//没有变成候选人，则退出
				//不是跟随者，或者有领导，或者为别人投票
				break
			}
		}
	}
	go election()
	//进行心跳检测
	for {
		//0.5秒检测一次
		time.Sleep(time.Millisecond * 5000)
		//心跳超时
		if raft.lastHeartBeatTime != 0 && (millisecond()-raft.lastHeartBeatTime) > int64(raft.timeout*1000) {
			fmt.Printf("心跳检测超时，已超过%d秒\n", raft.timeout)
			fmt.Println("即将重新开启选举")
			raft.reDefault()
			raft.setCurrentLeader("-1")
			raft.lastHeartBeatTime = 0
			go election()
		}
	}
}
