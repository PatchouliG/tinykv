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

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	return &RaftLog{
		storage:         storage,
		committed:       0,
		applied:         0,
		stabled:         0,
		entries:         make([]pb.Entry, 0),
		pendingSnapshot: nil,
	}
	// Your Code Here (2A).
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	return nil
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	return nil
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	if len(l.entries) > 0 {
		return l.entries[len(l.entries)-1].Index
	}
	i, _ := l.storage.LastIndex()
	return i
	// Your Code Here (2A).
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	lastIndex := l.LastIndex()
	if i > lastIndex {
		return 0, errors.New("more than last index")
	}
	if len(l.entries) > 0 && i >= l.entries[0].Index {
		for _, e := range l.entries {
			if e.Index == i {
				return e.Term, nil
			}
		}
		return 0, errors.New("index not found")
	} else {
		res, err := l.storage.Term(i)
		return res, err
	}
	// Your Code Here (2A).
}

// return entry position
func (l *RaftLog) findByIndex(index uint64) (uint64, error) {
	for i := len(l.entries) - 1; i >= 0; i-- {
		if l.entries[i].Index == index {
			return uint64(i), nil
		}
	}
	return 0, errors.New("entry not found")
}

// return position in log , position in entry
func (l *RaftLog) findLastMatch(beginPosition uint64, entry []*pb.Entry) (uint64, uint64) {
	i := uint64(0)
	for ; beginPosition+i < uint64(len(l.entries)) && i < uint64(len(entry)); i++ {
		localEntry := l.entries[beginPosition+i]
		e := entry[i]
		if localEntry.Index != e.Index || localEntry.Term != e.Term {
			break
		}
	}
	return beginPosition + i - 1, i - 1
}

func (l *RaftLog) append(entries []*pb.Entry) {
	for _, e := range entries {
		l.entries = append(l.entries, *e)
	}

}
