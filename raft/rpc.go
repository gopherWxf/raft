package raft

import (
	"fmt"
	"log"
	"net/http"
	"net/rpc"
	"time"
)

//注册rpc服务绑定http协议上开启监听
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

// HeartBeatResponse 心跳检测回复
func (rf *Raft) HeartBeatResponse(node NodeInfo, b *bool) error {
	//因为发送心跳的一定是leader
	rf.setCurrentLeader(node.ID)
	//最后一次心跳的时间
	rf.lastHeartBeatTime = millisecond()
	fmt.Printf("收到来自leader[%s]节点的心跳检测\n", node.ID)
	*b = true
	return nil
}

// ConfirmationLeader 确认领导者
func (rf *Raft) ConfirmationLeader(node NodeInfo, b *bool) error {
	rf.setCurrentLeader(node.ID)
	*b = true
	fmt.Println("已发现网络中的领导节点，", node.ID, "成为了领导者！")
	rf.reDefault()
	return nil
}

// Vote 投票
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

// LeaderReceiveMessage 领导者接收到跟随者节点转发过来的消息
func (rf *Raft) LeaderReceiveMessage(message Message, b *bool) error {
	fmt.Printf("领导者节点接收到转发过来的消息，id为:%d\n", message.MsgID)
	MessageStore[message.MsgID] = message.MsgBody
	*b = true
	fmt.Println("准备将消息进行广播...")
	//广播给其他跟随者
	var rec int
	go rf.broadcast("Raft.ReceiveMessage", message, func(ok bool) {
		if ok {
			rec++
		}
	})
	for {
		if rec >= nodeCount/2+1 {
			fmt.Printf("大部分节点接收到消息id:%d\n", message.MsgID)
			fmt.Printf("raft验证通过,可以打印消息,id为:[%d],消息为:[%s]\n", message.MsgID, MessageStore[message.MsgID])
			rf.lastMessageTime = millisecond()
			fmt.Println("准备将消息提交信息发送至客户端...")
			go rf.broadcast("Raft.ConfirmationMessage", message, func(ok bool) {
			})
			break
		} else {
			//可能别的节点还没回复，等待一会
			time.Sleep(time.Millisecond * 100)
		}
	}
	return nil
}

// ReceiveMessage 跟随者节点用来接收消息，然后存储到消息池中，待领导者确认后打印
func (rf *Raft) ReceiveMessage(message Message, b *bool) error {
	fmt.Printf("接收到领导者节点发来的信息，id:%d\n", message.MsgID)
	MessageStore[message.MsgID] = message.MsgBody
	*b = true
	fmt.Println("已回复接收到消息，待领导者确认后打印")
	return nil
}

// ConfirmationMessage 追随者节点的反馈得到领导者节点的确认，开始打印消息
func (rf *Raft) ConfirmationMessage(message Message, b *bool) error {
	go func() {
		for {
			if _, ok := MessageStore[message.MsgID]; ok {
				fmt.Printf("raft验证通过,可以打印消息,id为:[%d],消息为:[%s]\n", message.MsgID, MessageStore[message.MsgID])
				rf.lastMessageTime = millisecond()
				break
			} else {
				//可能这个节点的网络传输很慢，等一会
				time.Sleep(time.Millisecond * 10)
			}
		}
	}()
	*b = true
	return nil
}
