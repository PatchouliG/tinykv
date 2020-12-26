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
	"sort"
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
	// all entry in storage is committed
	//index, err := storage.LastIndex()
	//if err != nil {
	//	panic(err)
	//}
	hs := storage.InitialHardState()
	//if err != nil {
	//	panic(err)
	//}
	committed := hs.Commit

	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}

	// copy all uncommitted entry to raft log
	var entries []pb.Entry
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}

	for index := lastIndex; ; index-- {
		e, err := storage.Entries(index, index+1)
		if err != nil {
			// not found, log is empty
			break
		}
		// committed may be zero
		if e[0].Index == committed || index == firstIndex {
			entries, err = storage.Entries(index, lastIndex+1)
			if err != nil {
				panic(err)
			}
			break
		}
	}

	return &RaftLog{
		storage:   storage,
		committed: committed,
		//todo
		applied:         0,
		stabled:         lastIndex,
		entries:         entries,
		pendingSnapshot: nil,
	}
	// Your Code Here (2A).
}

func (l *RaftLog) updateApplied(appliedIndex uint64) {
	if appliedIndex > l.committed {
		panic("apply is bigger than committed")
	}
	l.applied = appliedIndex
}
func (l *RaftLog) updateCommitted(committed uint64) {
	if l.committed > l.entries[len(l.entries)-1].Index {
		panic("committed is bigger than length")
	}
	l.committed = committed
}
func (l *RaftLog) updateStable(stable uint64) {
	l.stabled = stable
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
// remove all committed entries
func (l *RaftLog) maybeCompact() {
	position, found := l.findByIndex(l.committed)
	if !found {
		return
	}
	l.entries = l.entries[position+1:]
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	if len(l.entries) == 0 {
		return make([]pb.Entry, 0)
	}
	// stabled may be 0(init)
	position, found := l.findByIndex(l.stabled + 1)
	if !found {
		return make([]pb.Entry, 0)
	}

	res := l.entries[position:]
	return res
}
func (l *RaftLog) unCommittedEntries() []pb.Entry {
	if len(l.entries) == 0 {
		return nil
	}
	committedPosition, found := l.findByIndex(l.committed)
	if !found {
		return nil
	}
	stabledPosition, found := l.findByIndex(l.stabled)
	if !found {
		return nil
	}
	return l.entries[committedPosition+1 : stabledPosition+1]

}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	if len(l.entries) == 0 {
		return nil
	}
	position, found := l.findByIndex(l.applied)
	if !found {
		return nil
	}
	for p := position + 1; p < len(l.entries); p++ {
		if l.entries[p].Index == l.committed {
			return l.entries[position+1 : p+1]
		}
	}
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

// return position in log , position in entry
func (l *RaftLog) findLastMatch(beginPosition uint64, entry []*pb.Entry) (uint64, uint64) {
	i := uint64(1)
	for ; beginPosition+i < uint64(len(l.entries)) && i < uint64(len(entry)); i++ {
		localEntry := l.entries[beginPosition+i]
		e := entry[i]
		if localEntry.Index != e.Index || localEntry.Term != e.Term {
			break
		}
	}
	return beginPosition + i - 1, i - 1
}

func (l *RaftLog) findByIndex(index uint64) (int, bool) {
	if len(l.entries) == 0 {
		return 0, false
	}

	res := sort.Search(len(l.entries), func(i int) bool {
		return l.entries[i].Index >= index
	})

	if res == len(l.entries) || l.entries[res].Index != index {
		return 0, false
	}
	return res, true
}

func (l *RaftLog) append(entries []*pb.Entry) {
	for _, e := range entries {
		l.entries = append(l.entries, *e)
	}
}
func (l *RaftLog) appendEntry(entry *pb.Entry) {
	l.entries = append(l.entries, *entry)
}
