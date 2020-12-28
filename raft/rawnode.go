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
)

// ErrStepLocalMsg is returned when try to step a local raft message
var ErrStepLocalMsg = errors.New("raft: cannot step raft local message")

// ErrStepPeerNotFound is returned when try to step a response message
// but there is no peer found in raft.Prs for that node.
var ErrStepPeerNotFound = errors.New("raft: cannot step as peer not found")

// SoftState provides state that is volatile and does not need to be persisted to the WAL.
type SoftState struct {
	Lead      uint64
	RaftState StateType
}

// Ready encapsulates the entries and messages that are ready to read,
// be saved to stable storage, committed or sent to other peers.
// All fields in Ready are read-only.
type Ready struct {
	// The current volatile state of a Node.
	// SoftState will be nil if there is no update.
	// It is not required to consume or store SoftState.
	*SoftState

	// The current state of a Node to be saved to stable storage BEFORE
	// Messages are sent.
	// HardState will be equal to empty state if there is no update.
	pb.HardState

	// Entries specifies entries to be saved to stable storage BEFORE
	// Messages are sent.
	Entries []pb.Entry

	// Snapshot specifies the snapshot to be saved to stable storage.
	Snapshot pb.Snapshot

	// CommittedEntries specifies entries to be committed to a
	// store/state-machine. These have previously been committed to stable
	// store.
	CommittedEntries []pb.Entry

	// Messages specifies outbound messages to be sent AFTER Entries are
	// committed to stable storage.
	// If it contains a MessageType_MsgSnapshot message, the application MUST report back to raft
	// when the snapshot has been received or has failed by calling ReportSnapshot.
	Messages []pb.Message
}

// RawNode is a wrapper of Raft.
type RawNode struct {
	Raft          *Raft
	lastSoftState *SoftState
	lastHardState pb.HardState
	lastReady     *Ready
	firstReady    bool
	// Your Data Here (2A).
}

// NewRawNode returns a new RawNode given configuration and a list of raft peers.
func NewRawNode(config *Config) (*RawNode, error) {
	raft := newRaft(config)

	return &RawNode{
		Raft: raft,
		lastSoftState: &SoftState{
			Lead:      raft.Lead,
			RaftState: raft.State,
		},
		lastHardState: pb.HardState{
			Term: raft.Term,
			Vote: raft.Vote,
			//@continue
			Commit: raft.RaftLog.committed,
		},
		firstReady: true,
	}, nil

	// Your Code Here (2A).
}

// Tick advances the internal logical clock by a single tick.
func (rn *RawNode) Tick() {
	rn.Raft.tick()
}

// Campaign causes this RawNode to transition to candidate state.
func (rn *RawNode) Campaign() error {
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgHup,
	})
}

// Propose proposes data be appended to the raft log.
func (rn *RawNode) Propose(data []byte) error {
	ent := pb.Entry{Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		From:    rn.Raft.id,
		Entries: []*pb.Entry{&ent}})
}

// ProposeConfChange proposes a config change.
func (rn *RawNode) ProposeConfChange(cc pb.ConfChange) error {
	data, err := cc.Marshal()
	if err != nil {
		return err
	}
	ent := pb.Entry{EntryType: pb.EntryType_EntryConfChange, Data: data}
	return rn.Raft.Step(pb.Message{
		MsgType: pb.MessageType_MsgPropose,
		Entries: []*pb.Entry{&ent},
	})
}

// ApplyConfChange applies a config change to the local node.
func (rn *RawNode) ApplyConfChange(cc pb.ConfChange) *pb.ConfState {
	if cc.NodeId == None {
		return &pb.ConfState{Nodes: nodes(rn.Raft)}
	}
	switch cc.ChangeType {
	case pb.ConfChangeType_AddNode:
		rn.Raft.addNode(cc.NodeId)
	case pb.ConfChangeType_RemoveNode:
		rn.Raft.removeNode(cc.NodeId)
	default:
		panic("unexpected conf type")
	}
	return &pb.ConfState{Nodes: nodes(rn.Raft)}
}

// Step advances the state machine using the given message.
func (rn *RawNode) Step(m pb.Message) error {
	// ignore unexpected local messages receiving over network
	if IsLocalMsg(m.MsgType) {
		return ErrStepLocalMsg
	}
	if pr := rn.Raft.Prs[m.From]; pr != nil || !IsResponseMsg(m.MsgType) {
		return rn.Raft.Step(m)
	}
	return ErrStepPeerNotFound
}
func (rn *RawNode) Ready() Ready {
	if rn.lastReady != nil {
		res := *rn.lastReady
		rn.lastReady = nil
		return res
	}
	return rn.ReadyImp()
}

// Ready returns the current point-in-time state of this RawNode.
func (rn *RawNode) ReadyImp() Ready {
	// Your Code Here (2A).

	// get unStabled entries
	unStableEntries := rn.Raft.RaftLog.unstableEntries()

	// get uncommitted entries
	var uncommittedEntries []pb.Entry

	var uncommitted int
	// first ready, need to commit all entry in log
	// not need to commit applied(todo)
	if rn.lastHardState.Commit == 0 || rn.firstReady {
		uncommitted = 0
		rn.firstReady = false
	} else {
		p, found := rn.Raft.RaftLog.findByIndex(rn.lastHardState.Commit + 1)
		if !found {
			uncommitted = len(rn.Raft.RaftLog.entries)
		} else {
			uncommitted = p
		}
	}
	for i := uncommitted; i < len(rn.Raft.RaftLog.entries) &&
		rn.Raft.RaftLog.entries[i].Index <= rn.Raft.RaftLog.committed; i++ {
		uncommittedEntries = append(uncommittedEntries, rn.Raft.RaftLog.entries[i])
	}

	// update stabled
	rn.Raft.RaftLog.stabled = rn.Raft.RaftLog.LastIndex()

	ss := &SoftState{
		Lead:      rn.Raft.Lead,
		RaftState: rn.Raft.State,
	}
	if rn.lastSoftState != nil && *ss == *rn.lastSoftState {
		ss = nil
	} else {
		rn.lastSoftState = ss
	}
	hs := pb.HardState{
		Term:   rn.Raft.Term,
		Vote:   rn.Raft.Vote,
		Commit: rn.Raft.RaftLog.committed,
	}

	if isHardStateEqual(hs, rn.lastHardState) {
		hs = pb.HardState{}
	} else {
		rn.lastHardState = hs
	}

	msg := rn.Raft.msgs
	rn.Raft.msgs = nil

	return Ready{
		SoftState:        ss,
		HardState:        hs,
		Entries:          unStableEntries,
		Snapshot:         pb.Snapshot{},
		CommittedEntries: uncommittedEntries,
		Messages:         msg,
	}
}

// HasReady called when RawNode user need to check if any Ready pending.
func (rn *RawNode) HasReady() bool {
	// Your Code Here (2A).
	if rn.lastReady != nil {
		return true
	}
	r := rn.ReadyImp()
	if r.SoftState == nil && IsEmptyHardState(r.HardState) &&
		len(r.Entries) == 0 && len(r.CommittedEntries) == 0 && len(r.Messages) == 0 &&
		snapshotIsEmpty(r.Snapshot) {
		return false
	} else {
		rn.lastReady = &r
		return true
	}
	//return false
}

func snapshotIsEmpty(snapshot pb.Snapshot) bool {
	return len(snapshot.Data) == 0 && snapshot.Metadata == nil
}

// Advance notifies the RawNode that the application has applied and saved progress in the
// last Ready results.
func (rn *RawNode) Advance(rd Ready) {
	if len(rd.Entries) > 0 {
		lastStabled := rd.Entries[len(rd.Entries)-1].Index
		rn.Raft.RaftLog.stabled = lastStabled
		rn.lastHardState = rd.HardState
	}
}

// GetProgress return the the Progress of this node and its peers, if this
// node is leader.
func (rn *RawNode) GetProgress() map[uint64]Progress {
	prs := make(map[uint64]Progress)
	if rn.Raft.State == StateLeader {
		for id, p := range rn.Raft.Prs {
			prs[id] = *p
		}
	}
	return prs
}

// TransferLeader tries to transfer leadership to the given transferee.
func (rn *RawNode) TransferLeader(transferee uint64) {
	_ = rn.Raft.Step(pb.Message{MsgType: pb.MessageType_MsgTransferLeader, From: transferee})
}
