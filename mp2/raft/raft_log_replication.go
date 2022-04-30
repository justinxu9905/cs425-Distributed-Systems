package raft

import "time"

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PervLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term      int
	Success   bool
	NextIndex int
}

func (rf *Raft) getNextIndex() int {
	_, idx := rf.lastLogTermIndex()
	return idx + 1
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	reply.Term = rf.currentTerm

	// refuse if term in req is smaller
	if rf.currentTerm > args.Term {
		rf.mu.Unlock()
		return
	}

	// after receiving heartbeat, reset election timer
	rf.currentTerm = args.Term
	rf.changeRole(Follower)
	rf.resetElectionTimer()

	_, lastLogIndex := rf.lastLogTermIndex()
	if args.PrevLogIndex > lastLogIndex { // log doesn’t contain an entry at prevLogIndex
		reply.Success = false
		reply.NextIndex = rf.getNextIndex()
	} else if rf.log[args.PrevLogIndex].Term == args.PervLogTerm { // log at prevLogIndex matches
		reply.Success = true
		rf.log = append(rf.log[0:args.PrevLogIndex+1], args.Entries...) // delete  existing entries conflicting with a new one
		reply.NextIndex = rf.getNextIndex()
	} else { // log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
		reply.Success = false
		term := rf.log[args.PrevLogIndex].Term
		idx := args.PrevLogIndex
		for idx > rf.commitIndex && rf.log[idx].Term == term { // search for log in last term to re-append logs in this term
			idx -= 1
		}
		reply.NextIndex = idx + 1
	}
	if reply.Success {
		if rf.commitIndex < args.LeaderCommit { // if leader has committed new logs
			rf.commitIndex = args.LeaderCommit
			rf.notifyApplyCh <- struct{}{}
		}
	}

	rf.mu.Unlock()
}

// reset all heartbeat timer to 0
func (rf *Raft) resetAllHeartbeatTimers() {
	for i, _ := range rf.heartbeatTimers {
		rf.heartbeatTimers[i].Stop()
		rf.heartbeatTimers[i].Reset(0)
	}
}

// set specific timer to HeartBeatTimeout
func (rf *Raft) setHeartbeatTimer(peerIdx int) {
	rf.heartbeatTimers[peerIdx].Stop()
	rf.heartbeatTimers[peerIdx].Reset(HeartBeatTimeout)
}

func (rf *Raft) appendEntriesToPeer(peerIdx int) {
	RPCTimer := time.NewTimer(RPCTimeout)
	defer RPCTimer.Stop()
	for {
		rf.mu.Lock()
		if rf.role != Leader {
			rf.setHeartbeatTimer(peerIdx)
			rf.mu.Unlock()
			return
		}

		// preparation for argus:
		// A follower may be missing entries that are present on the
		// leader, it may have extra entries that are not present on
		// the leader, or both.
		var prevLogIndex, prevLogTerm int
		var logs []LogEntry
		nextIdx := rf.nextIndex[peerIdx]
		lastLogTerm, lastLogIndex := rf.lastLogTermIndex()
		if nextIdx > lastLogIndex { // peer has extra entries
			prevLogIndex = lastLogIndex
			prevLogTerm = lastLogTerm
		} else { // peer may be missing entries, or missing and holding extra entries
			logs = append([]LogEntry{}, rf.log[nextIdx:]...)
			prevLogIndex = nextIdx - 1
			prevLogTerm = rf.log[prevLogIndex].Term
		}

		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevLogIndex,
			PervLogTerm:  prevLogTerm,
			Entries:      logs,
			LeaderCommit: rf.commitIndex,
		}

		rf.setHeartbeatTimer(peerIdx)
		rf.mu.Unlock()

		// multi-thread sending req
		RPCTimer.Stop()
		RPCTimer.Reset(RPCTimeout)
		reply := &AppendEntriesReply{}
		resCh := make(chan bool, 1)
		go func(args *AppendEntriesArgs, reply *AppendEntriesReply) {
			resCh <- rf.peers[peerIdx].Call("Raft.AppendEntries", args, reply)
		}(args, reply)

		select {
		// rpc timeout: retry
		case <-RPCTimer.C:
			continue

		// fail: retry
		case ok := <-resCh:
			if !ok {
				continue
			}
		}

		// rpc succeed
		rf.mu.Lock()
		// if peer has higher term, turn to follower and drop
		if reply.Term > rf.currentTerm {
			rf.changeRole(Follower)
			rf.resetElectionTimer()
			rf.currentTerm = reply.Term
			rf.mu.Unlock()
			return
		}

		// not leader or not that term: drop
		if rf.role != Leader || rf.currentTerm != args.Term {
			rf.mu.Unlock()
			return
		}

		if reply.Success {
			// update peer's next index
			if reply.NextIndex > rf.nextIndex[peerIdx] {
				rf.nextIndex[peerIdx] = reply.NextIndex
				rf.matchIndex[peerIdx] = reply.NextIndex - 1
			}

			// commit entries of current term
			if len(args.Entries) > 0 && args.Entries[len(args.Entries)-1].Term == rf.currentTerm {
				rf.tryToCommit()
			}
			rf.mu.Unlock()
			return
		}

		// fail to append due to mismatch
		// update peer's next index
		if reply.NextIndex > 0 {
			rf.nextIndex[peerIdx] = reply.NextIndex
		}
		rf.mu.Unlock()
	}

}

func (rf *Raft) tryToCommit() {
	// whether any new commits
	toCommit := false
	for i := rf.commitIndex + 1; i <= len(rf.log); i++ {
		count := 0
		for _, m := range rf.matchIndex {
			if m >= i {
				count += 1
				if count > len(rf.peers)/2 {
					rf.commitIndex = i
					toCommit = true
					break
				}
			}
		}
		if rf.commitIndex != i {
			break
		}
	}
	if toCommit {
		rf.notifyApplyCh <- struct{}{}
	}
}
