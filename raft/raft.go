package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

type NodeInfo struct {
	ID   string
	Port string
}

type Message struct {
	MsgBody string
	MsgID   int
}

type Raft struct {
	//本节点信息
	node *NodeInfo
	//本节点获得的投票数
	vote int
	//互斥锁
	lock sync.Mutex
	//本节点编号
	me string
	//当前任期
	currentTerm int
	//为哪个节点投票
	votedFor string
	//当前节点状态0 follower  1 candidate  2 leader
	state int
	//发送最后一条消息的时间
	lastSendMessageTime int64
	//发送最后一次心跳的时间
	lastSendHeartBeatTime int64
	//当前节点的领导
	currentLeader string
	//心跳超时时间（秒）
	heartBeatTimeout int
	//接收投票成功通道
	voteChan chan bool
	//心跳信号
	heartChan chan bool
}

func NewRaft(id, port string) *Raft {
	rf := new(Raft)
	rf.node = &NodeInfo{
		ID:   id,
		Port: port,
	}
	//当前节点获得票数
	rf.setVote(0)
	//编号
	rf.me = id
	//给0  1  2三个节点投票，给谁都不投
	rf.setVoteFor("-1")
	//设置节点状态 0 follower
	rf.setStatus(0)
	//最后一次心跳检测时间
	rf.lastSendHeartBeatTime = 0
	//心跳超时时间
	rf.heartBeatTimeout = heartBeatTimeout
	//最初没有领导
	rf.setCurrentLeader("-1")
	//设置任期
	rf.setTerm(0)
	//投票通道
	rf.voteChan = make(chan bool)
	//心跳通道
	rf.heartChan = make(chan bool)
	return rf
}

//设置投票数量
func (rf *Raft) setVote(num int) {
	rf.lock.Lock()
	rf.vote = num
	rf.lock.Unlock()
}

//设置为谁投票
func (rf *Raft) setVoteFor(id string) {
	rf.lock.Lock()
	rf.votedFor = id
	rf.lock.Unlock()
}

//设置当前节点状态
func (rf *Raft) setStatus(state int) {
	rf.lock.Lock()
	rf.state = state
	rf.lock.Unlock()
}

//设置当前领导者
func (rf *Raft) setCurrentLeader(leader string) {
	rf.lock.Lock()
	rf.currentLeader = leader
	rf.lock.Unlock()
}

//设置任期
func (rf *Raft) setTerm(term int) {
	rf.lock.Lock()
	rf.currentTerm = term
	rf.lock.Unlock()
}

//投票累加
func (rf *Raft) voteAdd() {
	rf.lock.Lock()
	rf.vote++
	rf.lock.Unlock()
}

//任期累加
func (rf *Raft) termAdd() {
	rf.lock.Lock()
	rf.currentTerm++
	rf.lock.Unlock()
}

//获取当前时间的毫秒数
func millisecond() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}

//产生随机值
func randRange(min, max int64) int64 {
	//用于心跳信号的时间
	rand.Seed(time.Now().UnixNano())
	return rand.Int63n(max-min) + min
}

//恢复默认设置
func (rf *Raft) reDefault() {
	rf.setVote(0)
	rf.setVoteFor("-1")
	rf.setStatus(0)
}

//给跟随者节点发送心跳包
func (rf *Raft) sendHeartPacket() {
	//如果收到通道开启的消息，将会向其他节点进行固定频率的心跳检测
	<-rf.heartChan //没有收到channel就会阻塞等待
	for {
		fmt.Println("本节点开始发送心跳检测")
		rf.broadcast("Raft.HeartBeatResponse", rf.node, func(ok bool) {
			fmt.Println("收到心跳检测", ok)
		})
		//最后一次心跳的时间
		rf.lastSendHeartBeatTime = millisecond()
		//休眠 --》心跳检测频率的时间
		time.Sleep(time.Second * time.Duration(heartBeatRate))
	}
}

//修改节点为候选人状态
func (rf *Raft) becomeCandidate() bool {
	r := randRange(1500, 5000)
	//休眠随机时间后，再开始成为候选人
	time.Sleep(time.Duration(r) * time.Millisecond)
	//如果当前节点是跟随者，并且没有领导，也没有为别人投票
	if rf.state == 0 && rf.currentLeader == "-1" && rf.votedFor == "-1" {
		//将节点状态变成候选者
		rf.setStatus(1)
		//设置为自己投了票
		rf.setVoteFor(rf.me)
		//自己的投票数量增加
		rf.voteAdd()
		//节点任期加1
		rf.termAdd()

		fmt.Println("本节点已经变成候选人状态")
		fmt.Printf("当前获得的票数：%d\n", rf.vote)
		//开启选举通道
		return true
	}
	return false
}

//进行选举
func (rf *Raft) election() bool {
	fmt.Println("开始进行领导者选举，向其他节点进行广播")
	go rf.broadcast("Raft.Vote", rf.node, func(ok bool) {
		rf.voteChan <- ok
	})
	for {
		select {
		//选举超时
		case <-time.After(time.Second * time.Duration(electionTimeout)):
			fmt.Println("领导者选举超时，节点变更为追随者状态")
			rf.reDefault()
			return false
		case ok := <-rf.voteChan:
			if ok {
				rf.voteAdd()
				fmt.Printf("获得来自其他节点的投票，当前得票数：%d\n", rf.vote)
			}
			if rf.vote >= nodeCount/2+1 && rf.currentLeader == "-1" {
				fmt.Println("获得大多数节点的同意，本节点被选举成为了leader")
				//节点状态变为2，代表leader
				rf.setStatus(2)
				//当前领导者为自己
				rf.setCurrentLeader(rf.me)
				fmt.Println("向其他节点进行广播本节点成为了leader...")
				go rf.broadcast("Raft.ConfirmationLeader", rf.node, func(ok bool) {
					fmt.Println("其他节点:是否同意[", rf.node.ID, "]为领导者", ok)
				})
				//有leader了，可以发送心跳包了
				rf.heartChan <- true
				return true
			}
		}
	}
}

//尝试成为候选人并选举
func (rf *Raft) tryToBeCandidateWithElection() {
	for {
		//尝试成为候选人节点
		if rf.becomeCandidate() {
			//成为后选人节点后 向其他节点要选票来进行选举
			if rf.election() {
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

//心跳超时检测
func (rf *Raft) heartTimeoutDetection() {
	for {
		//0.5秒检测一次
		time.Sleep(time.Millisecond * 5000)
		//心跳超时
		if rf.lastSendHeartBeatTime != 0 && (millisecond()-rf.lastSendHeartBeatTime) > int64(rf.heartBeatTimeout*1000) {
			fmt.Printf("心跳检测超时，已超过%d秒\n", rf.heartBeatTimeout)
			fmt.Println("即将重新开启选举")
			rf.reDefault()
			rf.setCurrentLeader("-1")
			rf.lastSendHeartBeatTime = 0
			go rf.tryToBeCandidateWithElection()
		}
	}
}
