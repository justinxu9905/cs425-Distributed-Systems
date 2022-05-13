package transaction

import (
	"cs-425-mp3/common"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type TentativeWrite struct {
	timestamp 	int64
	value 		int
}

type ReadTimestamps []int64

func (rts ReadTimestamps) Len() int { return len(rts) }
func (rts ReadTimestamps) Less(i, j int) bool {
	return rts[i] < rts[j]
}
func (rts ReadTimestamps) Swap(i, j int) { rts[i], rts[j] = rts[j], rts[i] }

type ObjectState struct {
	committedValue     int
	committedTimestamp int64
	readTimestamps     ReadTimestamps
	tentativeWrites    []*TentativeWrite
}

type TransactionMgr struct {
	counter 			int64
	lock 				sync.Mutex
	perObjectState		map[string]*ObjectState
	stableObject		map[string]bool
}

func NewTransactionMgr() *TransactionMgr {
	return &TransactionMgr{
		counter: 0,
		lock: sync.Mutex{},
		perObjectState: make(map[string]*ObjectState),
		stableObject: make(map[string]bool),
	}
}

type Operation struct {
	Op   string
	Acct string
	Amt  int
}

func (mgr *TransactionMgr) Log(log string) {
	//fmt.Print(log)
}

func (mgr *TransactionMgr) ShowState() {
	/*
		for obj, state := range mgr.perObjectState {
			fmt.Printf("obj: %v, committed value: %v, committed ts: %v\n", obj, state.committedValue, state.committedTimestamp)
			for _, tw := range state.tentativeWrites {
				fmt.Printf("tw val: %v, ts: %v\n", tw.value, tw.timestamp)
			}
			fmt.Println()
		}*/
}

func (mgr *TransactionMgr) Transaction(ops []Operation) bool {
	timestamp := atomic.AddInt64(&mgr.counter, 1)
	for _, op := range ops {
		if ok, _ := mgr.execute(timestamp, op); ok != nil {
			mgr.abort(timestamp)
			return false
		}
	}
	if !mgr.checkConsistency(timestamp) {
		mgr.abort(timestamp)
		return false
	}
	mgr.commit(timestamp)
	return true
}

func (mgr *TransactionMgr) Begin() int64 {
	return atomic.AddInt64(&mgr.counter, 1)
}

func (mgr *TransactionMgr) Execute(timestamp int64, op Operation) (error, int) {
	err, res := mgr.execute(timestamp, op)
	if err != nil {
		mgr.abort(timestamp)
		return err, 0
	}
	return nil, res
}

func (mgr *TransactionMgr) Committable(timestamp int64) bool {
	return mgr.checkConsistency(timestamp)
}

func (mgr *TransactionMgr) Commit(timestamp int64) bool {
	if !mgr.checkConsistency(timestamp) {
		mgr.abort(timestamp)
		return false
	}
	mgr.commit(timestamp)
	return true
}

func (mgr *TransactionMgr) Abort(timestamp int64) {
	mgr.abort(timestamp)
}

func (mgr *TransactionMgr) execute(timestamp int64, op Operation) (error, int) {
	mgr.Log(fmt.Sprintf("\n[execute] executing op: [%v %v %v] in tx %v\n", op.Op, op.Acct, op.Amt, timestamp))
	if op.Op == "BALANCE" {
		res, ok := mgr.read(timestamp, op.Acct)
		if !ok {
			return common.ErrorIsolationViolated, 0
		}
		if res == -1 {
			return common.ErrorNotFound, 0
		}
		fmt.Printf("%v = %v\n", op.Acct, res)
		return nil, res
	} else if op.Op == "DEPOSIT" {
		prev, ok := mgr.read(timestamp, op.Acct)
		if !ok {
			return common.ErrorIsolationViolated, 0
		}
		if prev == -1 {
			prev = 0
		}
		ok = mgr.write(timestamp, op.Acct, prev + op.Amt)
		if !ok {
			return common.ErrorIsolationViolated, 0
		}
	} else if op.Op == "WITHDRAW" {
		prev, ok := mgr.read(timestamp, op.Acct)
		if !ok {
			return common.ErrorIsolationViolated, 0
		}
		if prev == -1 {
			return common.ErrorNotFound, 0
		}
		ok = mgr.write(timestamp, op.Acct, prev - op.Amt)
		if !ok {
			return common.ErrorIsolationViolated, 0
		}
	}
	return nil, 0
}


func (mgr *TransactionMgr) read(timestamp int64, obj string) (int, bool) {
	defer mgr.lock.Unlock()
	mgr.Log(fmt.Sprintf("[read] reading obj %v in tx %v\n", obj, timestamp))
read:
	mgr.lock.Lock()
	state, ok := mgr.perObjectState[obj]
	if !ok {
		mgr.perObjectState[obj] = &ObjectState{
			committedValue: 0,
			committedTimestamp: 0,
			readTimestamps: []int64{},
			tentativeWrites: []*TentativeWrite{},
		}
		state = mgr.perObjectState[obj]
	}

	if timestamp > state.committedTimestamp {
		latestWriteTimestamp := state.committedTimestamp
		latestValue := state.committedValue
		for _, tw := range state.tentativeWrites {
			if tw.timestamp <= timestamp && tw.timestamp > latestWriteTimestamp {
				latestWriteTimestamp = tw.timestamp
				latestValue = tw.value
			}
		}

		if latestWriteTimestamp <= state.committedTimestamp {
			state.readTimestamps = append(state.readTimestamps, timestamp)
			sort.Sort(state.readTimestamps)
			if latestWriteTimestamp == 0 {
				mgr.Log(fmt.Sprintf("[read] non-exist object, and current rts = %v\n", state.readTimestamps))
				return -1, true
			}
			mgr.Log(fmt.Sprintf("[read] get previously committed value %v = %v, and current rts = %v\n", obj, latestValue, state.readTimestamps))
			return latestValue, true
		} else {
			if latestWriteTimestamp == timestamp {
				mgr.Log(fmt.Sprintf("[read] get value current tx tentatively write %v = %v, and current rts = %v\n", obj, latestValue, state.readTimestamps))
				return latestValue, true
			} else {
				mgr.lock.Unlock()
				time.Sleep(5 * time.Millisecond)
				goto read
			}
		}
	} else {
		mgr.Log(fmt.Sprintf("[read] too late to read, abort\n"))
		return 0, false
	}
}

func (mgr *TransactionMgr) write(timestamp int64, obj string, val int) bool {
	mgr.lock.Lock()
	defer mgr.lock.Unlock()
	mgr.Log(fmt.Sprintf("[write] writing obj %v with val %v in tx %v\n", obj, val, timestamp))
	if _, ok := mgr.perObjectState[obj]; !ok {
		mgr.perObjectState[obj] = &ObjectState{
			committedValue: 0,
			committedTimestamp: 0,
			readTimestamps: []int64{},
			tentativeWrites: []*TentativeWrite{},
		}
	}
	state, _ := mgr.perObjectState[obj]
	if (len(state.readTimestamps) <= 0 || timestamp >= state.readTimestamps[len(state.readTimestamps)-1]) && timestamp > state.committedTimestamp {
		for _, tw := range state.tentativeWrites {
			if tw.timestamp == timestamp {
				tw.value = val
				mgr.Log(fmt.Sprintf("[write] update value current tx tentatively write\n"))
				return true
			}
		}
		state.tentativeWrites = append(state.tentativeWrites, &TentativeWrite{
			timestamp: timestamp,
			value: val,
		})
		mgr.Log(fmt.Sprintf("[write] append to tw\n"))
		return true
	}
	mgr.Log(fmt.Sprintf("[write] too late to write\n"))
	return false
}

func (mgr *TransactionMgr) commit(timestamp int64) {
	defer mgr.lock.Unlock()
	mgr.Log(fmt.Sprintf("\n[commit] committing tx %v\n", timestamp))
	commit:
	mgr.lock.Lock()
	for obj, state := range mgr.perObjectState {
		mgr.stableObject[obj] = true
		for i, tw := range state.tentativeWrites {
			if tw.timestamp < timestamp {
				mgr.lock.Unlock()
				time.Sleep(5 * time.Millisecond)
				goto commit
			}
			if tw.timestamp == timestamp {
				state.tentativeWrites = append(state.tentativeWrites[:i], state.tentativeWrites[i+1:]...)
				state.committedTimestamp = tw.timestamp
				state.committedValue = tw.value
			}
		}
	}
	mgr.ShowState()
}

func (mgr *TransactionMgr) abort(timestamp int64) {
	mgr.lock.Lock()
	defer mgr.lock.Unlock()
	mgr.Log(fmt.Sprintf("[abort] aborting tx %v\n", timestamp))
	for obj, state := range mgr.perObjectState {
		for i, tw := range state.tentativeWrites {
			if tw.timestamp == timestamp {
				state.tentativeWrites = append(state.tentativeWrites[:i], state.tentativeWrites[i+1:]...)
			}
		}
		if stable, ok := mgr.stableObject[obj]; (!stable || !ok) && len(state.tentativeWrites) == 0 {
			mgr.Log(fmt.Sprintf("[abort] object %v got deleted\n", obj))
			delete(mgr.perObjectState, obj)
		}
	}
	mgr.ShowState()
}

func (mgr *TransactionMgr) checkConsistency(timestamp int64) bool {
	mgr.Log(fmt.Sprintf("\n[checkConsistency] check tx %v started\n", timestamp))
	mgr.lock.Lock()
	mgr.Log(fmt.Sprintf("\n[checkConsistency] check tx %v get lock\n", timestamp))
	defer mgr.lock.Unlock()
	for _, state := range mgr.perObjectState {
		for _, tw := range state.tentativeWrites {
			if tw.timestamp == timestamp {
				if tw.value < 0 {
					return false
				}
				break
			}
		}
	}
	return true
}