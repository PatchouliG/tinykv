// Copyright 2015 The etcd Authors
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

package raft

import (
	"errors"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"math/rand"
	"sort"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	voteFailCount int

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int

	randomElectionTimeout int

	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// number of ticks since it reached last electionTimeout
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	nodes []uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	s := c.Storage
	hardStatus, confStatus, err := s.InitialState()

	if err != nil {
		panic(err.Error())
	}

	if c.peers == nil {
		c.peers = confStatus.Nodes
	}

	nodes := c.peers
	// init vote to false
	votes := make(map[uint64]bool)
	for _, nodeId := range nodes {
		votes[nodeId] = false
	}

	return &Raft{
		id:      c.ID,
		Term:    hardStatus.Commit,
		Vote:    hardStatus.Vote,
		RaftLog: newLog(c.Storage),
		Prs:     nil,
		// init as a follower
		State:                 StateFollower,
		votes:                 nil,
		msgs:                  make([]pb.Message, 0),
		Lead:                  None,
		heartbeatTimeout:      c.HeartbeatTick,
		electionTimeout:       c.ElectionTick,
		randomElectionTimeout: randomTimeout(c.ElectionTick),
		heartbeatElapsed:      0,
		electionElapsed:       0,
		leadTransferee:        0,
		PendingConfIndex:      0,
		nodes:                 nodes,
	}
}

func randomTimeout(timeout int) int {
	return timeout + rand.Intn(timeout)
}

func (r *Raft) sendAppendWrap(to uint64) {
	r.sendAppend(to)
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	//	append entry
	lastIndex := r.RaftLog.LastIndex()
	prs := r.Prs[to]
	matched := prs.Match
	//if matched < lastIndex {
	msg := r.buildMsgWithoutData(pb.MessageType_MsgAppend, to, false)
	var position int
	// send empty append,update follower committed index
	if matched == r.RaftLog.LastIndex() {
		position = len(r.RaftLog.entries)
	} else {
		p, found := r.RaftLog.findByIndex(matched + 1)
		if !found {
			panic("not found matched index")
		}
		position = p
	}

	msg.Entries = entryValuesToPoints(r.RaftLog.entries[position:])
	msg.Index = prs.Match
	t, err := r.RaftLog.Term(prs.Match)
	if err != nil {
		panic("error ")
	}
	msg.LogTerm = t
	msg.Commit = r.RaftLog.committed
	r.appendMsg(msg)
	//update prs
	r.Prs[to] = &Progress{
		Match: prs.Match,
		Next:  lastIndex + 1,
	}
	return true
	//}
	// Your Code Here (2A).
	//return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	//r.Prs[to].Next = r.RaftLog.LastIndex() + 1
	msg := r.buildMsgWithoutData(pb.MessageType_MsgHeartbeat, to, false)
	//msg.Entries = entryValuesToPoints(r.RaftLog.entries[len(r.RaftLog.entries)-1:])

	r.appendMsg(msg)
	// Your Code Here (2A).
}

func (r *Raft) addNoopEntryToLog() {
	e := r.buildEmptyEntry()
	r.RaftLog.entries = append(r.RaftLog.entries, e)
	// update self
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
}

func (r *Raft) sendVote(to uint64) {
	voteMsg := r.buildMsgWithoutData(pb.MessageType_MsgRequestVote, to, false)
	t, err := r.RaftLog.Term(r.RaftLog.LastIndex())
	if err != nil {
		panic("fail to get term")
	}

	voteMsg.LogTerm = t
	voteMsg.Index = r.RaftLog.LastIndex()
	r.appendMsg(voteMsg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {

	switch r.State {
	case StateFollower:
		r.electionElapsed += 1
		if r.electionElapsed >= r.randomElectionTimeout {
			r.handleMsgUp()
		}
	case StateCandidate:
		r.electionElapsed += 1
		if r.electionElapsed >= r.randomElectionTimeout {
			r.handleMsgUp()
		}

	case StateLeader:
		// todo append retry is 1 logic clock
		//r.sendMsgToAll(r.sendAppendWrap)
		if r.heartbeatElapsed == 0 {
			r.sendHeartBeatToAll()
		}
		r.heartbeatElapsed = (r.heartbeatElapsed + 1) % r.heartbeatTimeout
	}

	// Your Code Here (2A).
}

func (r *Raft) sendHeartBeatToAll() {
	r.sendMsgToAll(r.sendHeartbeat)
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	r.State = StateFollower
	r.Term = term
	r.Lead = lead
	r.Vote = None
	r.electionElapsed = 0
	r.randomElectionTimeout = randomTimeout(r.electionTimeout)
	// Your Code Here (2A).
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	r.State = StateCandidate
	r.Term += 1
	r.votes = make(map[uint64]bool)
	// vote for self
	r.votes[r.id] = true
	r.Vote = r.id
	r.electionElapsed = 0
	r.randomElectionTimeout = randomTimeout(r.electionTimeout)
	r.voteFailCount = 0

	// Your Code Here (2A).
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {

	r.State = StateLeader
	r.heartbeatElapsed = 0
	r.Vote = None

	// init prs
	prs := make(map[uint64]*Progress)
	for _, nodeId := range r.nodes {
		prs[nodeId] = &Progress{
			Match: r.RaftLog.LastIndex(),
			Next:  r.RaftLog.LastIndex() + 1,
		}
	}
	r.Prs = prs

	// NOTE: Leader should propose a noop entry on its term
	r.addNoopEntryToLog()
	r.sendMsgToAll(r.sendAppendWrap)
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).

	// ignore if term is less than me
	if m.Term < r.Term && (isCandidateMsg(m) || isLeaderMsg(m)) {
		//r.appendMsg(r.buildReject(pb.MessageType_MsgAppend, m.From))
		return nil
	}

	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.handleMsgUp()
		case pb.MessageType_MsgAppend:
			// update state
			if m.Term > r.Term {
				// only append entry as follower
				r.becomeFollower(m.Term, m.From)
			}
			r.handleAppendEntries(m)
		case pb.MessageType_MsgRequestVote:
			if m.Term > r.Term {
				// update term
				r.becomeFollower(m.Term, None)
			}
			r.handleVoter(m)
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
			// ignore other msg
		}
	case StateCandidate:
		switch m.MsgType {
		case pb.MessageType_MsgHup:
			r.handleMsgUp()
		case pb.MessageType_MsgAppend:
			// change state, reset follower info
			if m.Term >= r.Term {
				r.becomeFollower(m.Term, m.From)
				// only append entry as follower
				r.handleAppendEntries(m)
			}
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgRequestVoteResponse:
			r.handleVoteResponse(m)
		case pb.MessageType_MsgRequestVote:
			if m.Term > r.Term {
				r.becomeFollower(m.Term, None)
				r.handleVoter(m)
			} else {
				r.appendMsg(r.buildMsgWithoutData(pb.MessageType_MsgRequestVoteResponse, m.From, true))
			}
			// ignore other msg
		}
	case StateLeader:
		switch m.MsgType {
		case pb.MessageType_MsgAppend:
			// change state, reset follower info
			if m.Term > r.Term {
				r.becomeFollower(m.Term, m.From)
				// only append entry as follower
				r.handleAppendEntries(m)
			}
		case pb.MessageType_MsgHeartbeat:
			r.handleHeartbeat(m)
		case pb.MessageType_MsgPropose:
			r.handlePropose(m)
		case pb.MessageType_MsgAppendResponse:
			r.handleAppendResp(m)
		case pb.MessageType_MsgBeat:
			r.sendHeartBeatToAll()
		case pb.MessageType_MsgRequestVote:
			if m.Term > r.Term {
				r.becomeFollower(m.Term, None)
				r.handleVoter(m)
			} else {
				r.appendMsg(r.buildMsgWithoutData(pb.MessageType_MsgRequestVoteResponse, m.From, true))
			}
			// @think ignore  heart resp,it seems not need to handle it
			// ignore other msg
		}
	}
	return nil
}

func (r *Raft) handleMsgUp() {
	r.becomeCandidate()
	r.sendMsgToAll(r.sendVote)
	// win vote if only one node
	if len(r.nodes) == 1 {
		r.becomeLeader()
		return
	}
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {

	var prevPosition = -1
	if len(r.RaftLog.entries) == 0 || m.Index < r.RaftLog.entries[0].Index {
		term, err := r.RaftLog.storage.Term(m.Index)
		if err != nil || term != m.LogTerm {
			r.appendMsg(r.buildReject(pb.MessageType_MsgAppendResponse, m.From))
			return
		}
	} else {
		//reject if prevPosition entry not findLastMatch
		var found bool
		prevPosition, found = r.RaftLog.findByIndex(m.Index)
		if !found || r.RaftLog.entries[prevPosition].Term != m.LogTerm {
			r.appendMsg(r.buildReject(pb.MessageType_MsgAppendResponse, m.From))
			return
		}
	}

	offset := 0
	for ; offset < len(m.Entries); offset++ {
		if offset+prevPosition+1 >= len(r.RaftLog.entries) {
			r.RaftLog.append(m.Entries[offset:])
			break
		}
		e1 := r.RaftLog.entries[offset+prevPosition+1]
		e2 := m.Entries[offset]
		if e1.Index != e2.Index || e1.Term != e2.Term {
			r.RaftLog.entries = r.RaftLog.entries[:offset+prevPosition+1]
			if len(r.RaftLog.entries) > 0 {
				lastIndexInLog := r.RaftLog.entries[len(r.RaftLog.entries)-1].Index
				if lastIndexInLog < r.RaftLog.stabled {
					r.RaftLog.stabled = lastIndexInLog
				}
			} else {
				r.RaftLog.stabled = 0
			}
			r.RaftLog.append(m.Entries[offset:])
			break
		}
	}

	msg := r.buildMsgWithoutData(pb.MessageType_MsgAppendResponse, m.From, false)
	msg.Index = r.RaftLog.LastIndex()
	r.appendMsg(msg)

	// update committed

	if m.Commit > r.RaftLog.committed && lastIndexInMeg(m) > r.RaftLog.committed {
		r.RaftLog.committed = min(m.Commit, lastIndexInMeg(m))
	}

	// Your Code Here (2A).
}

func lastIndexInMeg(m pb.Message) uint64 {
	if len(m.Entries) == 0 {
		return m.Index
	}
	return m.Entries[len(m.Entries)-1].Index
}

// handleHeartbeat handle Heartbeat RPC request

//|role|term compare|<0|==0|>0|
//|-|-|-|-|-|
//|follower||reject|accept|receive,update leader info|
//|candidate||reject|accept,become follower|same|
//|leader||reject|panic,won't happen|accept,become follower|
func (r *Raft) handleHeartbeat(m pb.Message) {
	// already check
	//for all role, ignore if term is less
	compareRes := r.compareMsgTerm(m)
	//if compareRes > 0 {
	//	return
	//	msg := r.buildReject(pb.MessageType_MsgAppendResponse, m.From)
	//	r.appendMsg(msg)
	//}

	switch r.State {
	case StateFollower:
		// handle timeout
		if compareRes < 0 {
			// reset state
			r.becomeFollower(m.Term, m.From)
		}
		r.heartbeatElapsed = 0
	case StateCandidate:
		r.becomeFollower(m.Term, m.From)
	case StateLeader:
		r.becomeFollower(m.Term, m.From)
	}

	//r.RaftLog.append(m.Entries)
}

func (r *Raft) handlePropose(m pb.Message) {
	lastIndex := r.RaftLog.LastIndex()
	for i, e := range m.Entries {
		e.Index = lastIndex + 1 + uint64(i)
		e.Term = r.Term
	}
	r.RaftLog.append(m.Entries)
	// update self
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1

	// update committed if just one node
	r.updateCommitted()

	r.sendMsgToAll(r.sendAppendWrap)
}
func (r *Raft) handleVoteResponse(m pb.Message) {
	if m.Reject {
		r.voteFailCount += 1
		// vote fail, turn to follower
		if r.voteFailCount >= len(r.nodes)/2+1 {
			r.becomeFollower(r.Term, None)
		}
		return
	}
	r.votes[m.From] = true
	if len(r.votes) >= len(r.nodes)/2+1 {
		r.becomeLeader()
	}
	//r.addNoopEntryToLog()
	//r.sendMsgToAll(r.sendHeartbeat)
}

func (r *Raft) handleVoter(m pb.Message) {
	reject := true
	lastIndex := r.RaftLog.LastIndex()
	lastTerm, err := r.RaftLog.Term(lastIndex)
	if err != nil {
		panic("term not found")
	}

	if lastTerm < m.LogTerm || (lastTerm == m.LogTerm && lastIndex <= m.Index) {
		if r.Vote == None || r.Vote == m.From {
			reject = false
			r.Vote = m.From
		}
	}
	r.appendMsg(r.buildMsgWithoutData(pb.MessageType_MsgRequestVoteResponse, m.From, reject))
}

// update matched index
func (r *Raft) handleAppendResp(m pb.Message) {
	nodeId := m.From
	oldPrs := r.Prs[nodeId]
	// append success
	if !m.Reject {
		r.Prs[nodeId] = &Progress{
			Match: m.Index,
			Next:  m.Index + 1,
		}

		res := r.updateCommitted()
		if res {
			r.sendMsgToAll(r.sendAppendWrap)
		}
	} else {
		var match uint64
		if oldPrs.Match == 0 {
			match = 0
		} else {
			match = oldPrs.Match - 1
		}
		r.Prs[nodeId] = &Progress{
			Match: match,
			Next:  match + 1,
		}
		r.sendAppend(m.From)
	}
	// append fail
}

// return true if update
func (r *Raft) updateCommitted() bool {
	// update committed
	var matches []int
	for _, v := range r.Prs {
		matches = append(matches, int(v.Match))
	}
	sort.Ints(matches)
	m := uint64(matches[((len(matches) - 1) / 2)])

	t, err := r.RaftLog.Term(m)
	if err != nil {
		panic("term not found")
	}

	if t != r.Term {
		return false
	}

	if m > r.RaftLog.committed {
		r.RaftLog.committed = m
		return true
	}
	return false
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}
func (r *Raft) sendMsgToAll(createMsg func(id uint64)) {
	for _, id := range r.nodes {
		if id == r.id {
			continue
		}
		createMsg(id)
	}
}

func (r *Raft) appendMsg(message pb.Message) {
	r.msgs = append(r.msgs, message)
	if r.State == StateLeader && message.MsgType == pb.MessageType_MsgAppend {
		r.Prs[r.id].Match = r.RaftLog.LastIndex()
		r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
	}
}

func (r *Raft) compareMsgTerm(msg pb.Message) int64 {
	return int64(r.Term - msg.Term)
}

func (r *Raft) buildEmptyEntry() pb.Entry {

	return pb.Entry{
		EntryType: pb.EntryType_EntryNormal,
		Term:      r.Term,
		Index:     r.RaftLog.LastIndex() + 1,
	}

}

func (r *Raft) buildReject(messageType pb.MessageType, to uint64) pb.Message {
	return r.buildMsgWithoutData(messageType, to, true)
}

// fill reject
func (r *Raft) buildMsgWithoutData(messageType pb.MessageType, to uint64, reject bool) pb.Message {
	return pb.Message{
		MsgType:  messageType,
		To:       to,
		From:     r.id,
		Term:     r.Term,
		LogTerm:  0,
		Index:    0,
		Entries:  nil,
		Commit:   0,
		Snapshot: nil,
		Reject:   reject,
	}
}

func entryValuesToPoints(entries []pb.Entry) []*pb.Entry {
	res := make([]*pb.Entry, 0)
	for i := range entries {
		res = append(res, &entries[i])
	}
	return res
}

func isLeaderMsg(m pb.Message) bool {
	msgType := m.MsgType

	return msgType == pb.MessageType_MsgAppend ||
		msgType == pb.MessageType_MsgHeartbeat ||
		msgType == pb.MessageType_MsgTransferLeader ||
		msgType == pb.MessageType_MsgTimeoutNow
}
func isCandidateMsg(m pb.Message) bool {
	msgType := m.MsgType
	return msgType == pb.MessageType_MsgRequestVote
}
