package main

import (
	"fmt"
	"log"
	"net/http"
	"net/rpc"
)

//rpc服务注册
func rpcRegister(raft *Raft) {
	//注册一个RPC服务器
	if err := rpc.Register(raft); err != nil {
		log.Panicln("注册RPC失败", err)
	}
	port := raft.node.Port
	//把RPC服务绑定到http协议上
	rpc.HandleHTTP()
	//127.0.0.1:6870|6871|6872
	if err := http.ListenAndServe(port, nil); err != nil {
		log.Panicln("注册RPC失败", err)
	}
}

//广播
func (rf *Raft) broadcast(method string, args interface{}, fun func(ok bool)) {
	//不广播自己
	for nodeID, nodePort := range nodePool {
		if nodeID == rf.me {
			continue
		}
		//连接远程节点的rpc
		conn, err := rpc.DialHTTP("tcp", "127.0.0.1"+nodePort)
		if err != nil {
			//连接失败，调用回调
			fun(false)
			continue
		}
		var bo bool
		err = conn.Call(method, args, &bo)
		if err != nil {
			//调用失败，调用回调
			fun(false)
			continue
		}
		//回调
		fun(bo)
	}
}

//心跳检测回复
func (rf *Raft) HeartBeatResponse(node NodeInfo, b *bool) error {
	//因为发送心跳的一定是leader
	rf.setCurrentLeader(node.ID)
	//最后一次心跳的时间
	rf.lastHeartBeatTime = millisecond()
	fmt.Printf("收到来自leader[%s]节点的心跳检测\n", node.ID)
	*b = true
	return nil
}

//确认领导者
func (rf *Raft) ConfirmationLeader(node NodeInfo, b *bool) error {
	rf.setCurrentLeader(node.ID)
	*b = true
	fmt.Println("已发现网络中的领导节点，", node.ID, "成为了领导者！\n")
	rf.reDefault()
	return nil
}

//投票
func (rf *Raft) Vote(node NodeInfo, b *bool) error {
	if rf.votedFor == "-1" && rf.currentLeader == "-1" {
		rf.setVoteFor(node.ID)
		fmt.Printf("投票成功，已投%s节点\n", node.ID)
		*b = true
	} else {
		*b = false
	}
	return nil
}
