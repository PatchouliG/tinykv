package raft

import (
	"github.com/magiconair/properties/assert"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
	"testing"
)

func newTestEntry(index, term uint64) pb.Entry {
	return pb.Entry{
		Index: index,
		Term:  term,
		Data:  []byte{0},
	}
}

// unit test
func TestMatch2A(t *testing.T) {
	log := newLog(NewMemoryStorage())

	log.entries = append(log.entries, newTestEntry(0, 1))
	log.entries = append(log.entries, newTestEntry(1, 1))

	pb := make([]*pb.Entry, 0)
	a := newTestEntry(0, 1)
	pb = append(pb, &a)
	b := newTestEntry(1, 1)
	pb = append(pb, &b)
	c := newTestEntry(2, 1)
	pb = append(pb, &c)
	p1, p2 := log.findLastMatch(0, pb)
	assert.Equal(t, 1, p1)
	assert.Equal(t, 1, p2)

}
func TestMatchReturnZero2AA(t *testing.T) {
	log := newLog(NewMemoryStorage())

	log.entries = append(log.entries, newTestEntry(0, 1))
	log.entries = append(log.entries, newTestEntry(0, 2))

	pb := make([]*pb.Entry, 0)
	a := newTestEntry(0, 1)
	pb = append(pb, &a)
	b := newTestEntry(1, 1)
	pb = append(pb, &b)
	c := newTestEntry(2, 1)
	pb = append(pb, &c)
	p1, p2 := log.findLastMatch(0, pb)
	assert.Equal(t, uint64(0), p1)
	assert.Equal(t, uint64(0), p2)
}
func TestMatchLengthIsBigger2AA(t *testing.T) {
	log := newLog(NewMemoryStorage())

	log.entries = append(log.entries, newTestEntry(0, 1))
	log.entries = append(log.entries, newTestEntry(1, 1))
	log.entries = append(log.entries, newTestEntry(2, 1))

	pb := make([]*pb.Entry, 0)
	a := newTestEntry(0, 1)
	pb = append(pb, &a)
	b := newTestEntry(1, 1)
	pb = append(pb, &b)

	p1, p2 := log.findLastMatch(0, pb)
	assert.Equal(t, p1, uint64(1))
	assert.Equal(t, p2, uint64(1))
}
