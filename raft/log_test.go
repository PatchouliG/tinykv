package raft

import (
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

func newTestEntry(index, term uint64) pb.Entry {
	return pb.Entry{
		Index: index,
		Term:  term,
		Data:  []byte{0},
	}
}
