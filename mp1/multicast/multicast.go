package multicast

import (
	"container/heap"
	"context"
	"cs-425-mp1/gen-go/demo/rpc"
	"cs-425-mp1/storage"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type Msg struct {
	Op    string
	Acct1 string
	Acct2 string
	Amt   int64
}

type TOMgr struct {
	ClientGroup   map[string]*rpc.TxServiceClient
	Logger        sync.Mutex
	LogFile       *os.File
	nodeId        string
	multicastLock sync.Mutex
	taskChan      chan string
	storage       *storage.TxStorage
	counter       int64
	counterLock   sync.Mutex
	proposedSeq   int64
	proposeLock   sync.Mutex
	txQueue       TxHeap
	txMap         map[string]*QueueElem
	queueLock     sync.RWMutex
}

func NewTOMgr(nodeId string) *TOMgr {
	s := storage.NewTxStorage("balances.txt")
	f, _ := os.Create("log.txt")
	return &TOMgr{
		ClientGroup:   nil,
		Logger:        sync.Mutex{},
		LogFile:       f,
		nodeId:        nodeId,
		counter:       0,
		counterLock:   sync.Mutex{},
		multicastLock: sync.Mutex{},
		taskChan:      make(chan string, 10),
		storage:       s,
		proposedSeq:   0,
		proposeLock:   sync.Mutex{},
		txQueue:       TxHeap{},
		txMap:         map[string]*QueueElem{},
		queueLock:     sync.RWMutex{},
	}
}

func (mgr *TOMgr) SetClientGroup(g map[string]*rpc.TxServiceClient) {
	mgr.multicastLock.Lock()
	defer mgr.multicastLock.Unlock()
	mgr.ClientGroup = g
}

func (mgr *TOMgr) RunTask() {
	for {
		node := <-mgr.taskChan
		mgr.multicastLock.Lock()
		delete(mgr.ClientGroup, node)
		mgr.multicastLock.Unlock()
	}
}

func (mgr *TOMgr) Log(log string) {
	mgr.Logger.Lock()
	defer mgr.Logger.Unlock()
	_, err := io.WriteString(mgr.LogFile, log)
	if err != nil {
		panic(err)
	}
}

func (mgr *TOMgr) count() int64 {
	return atomic.AddInt64(&mgr.counter, 1)
}

func (mgr *TOMgr) propose() int64 {
	return atomic.AddInt64(&mgr.proposedSeq, 1)
}

func (mgr *TOMgr) updatePropose(seq int64) {
	mgr.proposeLock.Lock()
	defer mgr.proposeLock.Unlock()
	if seq > mgr.proposedSeq {
		mgr.proposedSeq = seq
	}
}

func (mgr *TOMgr) TOMulticast(ctx context.Context, msg *Msg) time.Time {
	mgr.multicastLock.Lock()
	defer mgr.multicastLock.Unlock()

	var wg sync.WaitGroup
	var lk sync.Mutex
	tx := &rpc.Tx{Op: msg.Op, Acct1: msg.Acct1, Acct2: msg.Acct2, Amt: msg.Amt}

	// multicast
	count := mgr.count()
	mgr.Log(fmt.Sprintf("multicast tx %v\n", tx))
	agreedSeq := int64(0)
	decisionMaker := mgr.nodeId
	for node, cli := range mgr.ClientGroup {
		wg.Add(1)
		go func(node string, cli *rpc.TxServiceClient) {
			defer wg.Done()
			resp, err := cli.SendData(ctx, &rpc.DataMsg{
				Msg:    tx,
				MsgId:  count,
				Sender: mgr.nodeId,
			})
			if err != nil {
				mgr.taskChan <- node
				log.Printf("%v crashed: %v\n", node, err)
				return
			}
			lk.Lock()
			if resp.ProposedSeq > agreedSeq || (resp.ProposedSeq == agreedSeq && node > decisionMaker) {
				agreedSeq = resp.ProposedSeq
				decisionMaker = node
			}
			lk.Unlock()
		}(node, cli)
	}
	wg.Wait()

	// re-multicast after collecting all proposed seq
	mgr.Log(fmt.Sprintf("re-multicast tx %v with final seq %v\n", tx, agreedSeq))
	endTime := time.Now()
	timeLock := sync.Mutex{}
	for node, cli := range mgr.ClientGroup {
		wg.Add(1)
		go func(node string, cli *rpc.TxServiceClient) {
			defer wg.Done()
			resp, err := cli.SendSeq(ctx, &rpc.SeqMsg{
				MsgId:         count,
				Sender:        mgr.nodeId,
				AgreedSeq:     agreedSeq,
				DecisionMaker: decisionMaker,
			})
			if err != nil {
				mgr.taskChan <- node
				log.Printf("%v crashed: %v\n", node, err)
				return
			}
			deliverTime, _ := time.Parse(resp.DeliverTime, "2006-01-02 15:04:05")
			timeLock.Lock()
			if endTime.Before(deliverTime) {
				endTime = deliverTime
			}
			timeLock.Unlock()
		}(node, cli)
	}
	wg.Wait()
	return endTime
}

func (mgr *TOMgr) Receive(msg *Msg, msgId int64, sender string) int64 {
	propose := mgr.propose()
	mgr.Log(fmt.Sprintf("received tx: %v, reply with proposed seq: %v\n", msg, propose))
	elem := &QueueElem{
		Op:        msg.Op,
		Acct1:     msg.Acct1,
		Acct2:     msg.Acct2,
		Amt:       msg.Amt,
		MsgId:     msgId,
		Sender:    sender,
		Proposer:  mgr.nodeId,
		Seq:       propose,
		Delivered: false,
	}
	mgr.queueLock.Lock()
	elemKey := elem.Sender + ":" + strconv.Itoa(int(elem.MsgId))
	mgr.txMap[elemKey] = elem
	heap.Push(&mgr.txQueue, elem)
	mgr.queueLock.Unlock()
	return propose
}

func (mgr *TOMgr) Deliver(msgId, seq int64, sender, proposer string) {
	mgr.updatePropose(seq)
	mgr.queueLock.Lock()
	elemKey := sender + ":" + strconv.Itoa(int(msgId))
	elem, ok := mgr.txMap[elemKey]
	if !ok {
		panic(mgr.txMap)
	}
	elem.Delivered = true
	elem.Seq = seq
	elem.Proposer = proposer
	sort.Stable(mgr.txQueue)
	for cur, ok := mgr.txQueue.Top(); ok && cur.(*QueueElem).Delivered; cur, ok = mgr.txQueue.Top() {
		q := ""
		for _, e := range mgr.txQueue {
			q += fmt.Sprintf("%v ", e)
		}
		q += "\n"
		mgr.Log(q)
		t := heap.Pop(&mgr.txQueue).(*QueueElem)
		k := t.Sender + ":" + strconv.Itoa(int(t.MsgId))
		mgr.Log(fmt.Sprintf("delivered msg %v with final seq %v: %v\n", k, t.Seq, t))
		_ = mgr.storage.Deliver(t.Op, t.Acct1, t.Acct2, t.Amt)
	}
	mgr.queueLock.Unlock()
}

func (mgr *TOMgr) IsDelivered(msgId int64, nodeId string) bool {
	mgr.queueLock.RLock()
	defer mgr.queueLock.RUnlock()

	elemKey := nodeId + ":" + strconv.Itoa(int(msgId))
	elem, ok := mgr.txMap[elemKey]
	if ok && elem.Delivered == true {
		return true
	}
	return false
}

func (mgr *TOMgr) BMulticast(ctx context.Context, msgId, seq int64, sender, proposer string) {
	mgr.multicastLock.Lock()
	defer mgr.multicastLock.Unlock()

	var wg sync.WaitGroup
	for node, cli := range mgr.ClientGroup {
		wg.Add(1)
		go func(node string, cli *rpc.TxServiceClient) {
			defer wg.Done()
			_, err := cli.SendSeq(ctx, &rpc.SeqMsg{
				MsgId:         msgId,
				Sender:        sender,
				AgreedSeq:     seq,
				DecisionMaker: proposer,
			})
			if err != nil {
				mgr.taskChan <- node
				log.Printf("%v crashed: %v\n", node, err)
				return
			}
		}(node, cli)
	}
	wg.Wait()
}
