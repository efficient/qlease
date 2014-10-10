package qleaseproto

import (
	"io"
	"sync"
	"fastrpc"
	"bufio"
	"encoding/binary"
	"state"
)

type byteReader interface {
	io.Reader
	ReadByte() (c byte, err error)
}

func (t *Guard) New() fastrpc.Serializable {
	return new(Guard)
}
func (t *Guard) BinarySize() (nbytes int, sizeKnown bool) {
	return 20, true
}

type GuardCache struct {
	mu	sync.Mutex
	cache	[]*Guard
}

func NewGuardCache() *GuardCache {
	c := &GuardCache{}
	c.cache = make([]*Guard, 0)
	return c
}

func (p *GuardCache) Get() *Guard {
	var t *Guard
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &Guard{}
	}
	return t
}
func (p *GuardCache) Put(t *Guard) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *Guard) Marshal(wire io.Writer) {
	var b [20]byte
	var bs []byte
	bs = b[:20]
	tmp32 := t.ReplicaId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp64 := t.TimestampNs
	bs[4] = byte(tmp64)
	bs[5] = byte(tmp64 >> 8)
	bs[6] = byte(tmp64 >> 16)
	bs[7] = byte(tmp64 >> 24)
	bs[8] = byte(tmp64 >> 32)
	bs[9] = byte(tmp64 >> 40)
	bs[10] = byte(tmp64 >> 48)
	bs[11] = byte(tmp64 >> 56)
	tmp64 = t.GuardDuration
	bs[12] = byte(tmp64)
	bs[13] = byte(tmp64 >> 8)
	bs[14] = byte(tmp64 >> 16)
	bs[15] = byte(tmp64 >> 24)
	bs[16] = byte(tmp64 >> 32)
	bs[17] = byte(tmp64 >> 40)
	bs[18] = byte(tmp64 >> 48)
	bs[19] = byte(tmp64 >> 56)
	wire.Write(bs)
}

func (t *Guard) Unmarshal(wire io.Reader) error {
	var b [20]byte
	var bs []byte
	bs = b[:20]
	if _, err := io.ReadAtLeast(wire, bs, 20); err != nil {
		return err
	}
	t.ReplicaId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.TimestampNs = int64((uint64(bs[4]) | (uint64(bs[5]) << 8) | (uint64(bs[6]) << 16) | (uint64(bs[7]) << 24) | (uint64(bs[8]) << 32) | (uint64(bs[9]) << 40) | (uint64(bs[10]) << 48) | (uint64(bs[11]) << 56)))
	t.GuardDuration = int64((uint64(bs[12]) | (uint64(bs[13]) << 8) | (uint64(bs[14]) << 16) | (uint64(bs[15]) << 24) | (uint64(bs[16]) << 32) | (uint64(bs[17]) << 40) | (uint64(bs[18]) << 48) | (uint64(bs[19]) << 56)))
	return nil
}

func (t *GuardReply) New() fastrpc.Serializable {
	return new(GuardReply)
}
func (t *GuardReply) BinarySize() (nbytes int, sizeKnown bool) {
	return 12, true
}

type GuardReplyCache struct {
	mu	sync.Mutex
	cache	[]*GuardReply
}

func NewGuardReplyCache() *GuardReplyCache {
	c := &GuardReplyCache{}
	c.cache = make([]*GuardReply, 0)
	return c
}

func (p *GuardReplyCache) Get() *GuardReply {
	var t *GuardReply
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &GuardReply{}
	}
	return t
}
func (p *GuardReplyCache) Put(t *GuardReply) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *GuardReply) Marshal(wire io.Writer) {
	var b [12]byte
	var bs []byte
	bs = b[:12]
	tmp32 := t.ReplicaId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp64 := t.TimestampNs
	bs[4] = byte(tmp64)
	bs[5] = byte(tmp64 >> 8)
	bs[6] = byte(tmp64 >> 16)
	bs[7] = byte(tmp64 >> 24)
	bs[8] = byte(tmp64 >> 32)
	bs[9] = byte(tmp64 >> 40)
	bs[10] = byte(tmp64 >> 48)
	bs[11] = byte(tmp64 >> 56)
	wire.Write(bs)
}

func (t *GuardReply) Unmarshal(wire io.Reader) error {
	var b [12]byte
	var bs []byte
	bs = b[:12]
	if _, err := io.ReadAtLeast(wire, bs, 12); err != nil {
		return err
	}
	t.ReplicaId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.TimestampNs = int64((uint64(bs[4]) | (uint64(bs[5]) << 8) | (uint64(bs[6]) << 16) | (uint64(bs[7]) << 24) | (uint64(bs[8]) << 32) | (uint64(bs[9]) << 40) | (uint64(bs[10]) << 48) | (uint64(bs[11]) << 56)))
	return nil
}

func (t *Promise) New() fastrpc.Serializable {
	return new(Promise)
}
func (t *Promise) BinarySize() (nbytes int, sizeKnown bool) {
	return 28, true
}

type PromiseCache struct {
	mu	sync.Mutex
	cache	[]*Promise
}

func NewPromiseCache() *PromiseCache {
	c := &PromiseCache{}
	c.cache = make([]*Promise, 0)
	return c
}

func (p *PromiseCache) Get() *Promise {
	var t *Promise
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &Promise{}
	}
	return t
}
func (p *PromiseCache) Put(t *Promise) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *Promise) Marshal(wire io.Writer) {
	var b [28]byte
	var bs []byte
	bs = b[:28]
	tmp32 := t.ReplicaId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.LeaseInstance
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	tmp64 := t.TimestampNs
	bs[8] = byte(tmp64)
	bs[9] = byte(tmp64 >> 8)
	bs[10] = byte(tmp64 >> 16)
	bs[11] = byte(tmp64 >> 24)
	bs[12] = byte(tmp64 >> 32)
	bs[13] = byte(tmp64 >> 40)
	bs[14] = byte(tmp64 >> 48)
	bs[15] = byte(tmp64 >> 56)
	tmp64 = t.DurationNs
	bs[16] = byte(tmp64)
	bs[17] = byte(tmp64 >> 8)
	bs[18] = byte(tmp64 >> 16)
	bs[19] = byte(tmp64 >> 24)
	bs[20] = byte(tmp64 >> 32)
	bs[21] = byte(tmp64 >> 40)
	bs[22] = byte(tmp64 >> 48)
	bs[23] = byte(tmp64 >> 56)
	tmp32 = t.LatestAcceptedInst
	bs[24] = byte(tmp32)
	bs[25] = byte(tmp32 >> 8)
	bs[26] = byte(tmp32 >> 16)
	bs[27] = byte(tmp32 >> 24)
	wire.Write(bs)
}

func (t *Promise) Unmarshal(wire io.Reader) error {
	var b [28]byte
	var bs []byte
	bs = b[:28]
	if _, err := io.ReadAtLeast(wire, bs, 28); err != nil {
		return err
	}
	t.ReplicaId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.LeaseInstance = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.TimestampNs = int64((uint64(bs[8]) | (uint64(bs[9]) << 8) | (uint64(bs[10]) << 16) | (uint64(bs[11]) << 24) | (uint64(bs[12]) << 32) | (uint64(bs[13]) << 40) | (uint64(bs[14]) << 48) | (uint64(bs[15]) << 56)))
	t.DurationNs = int64((uint64(bs[16]) | (uint64(bs[17]) << 8) | (uint64(bs[18]) << 16) | (uint64(bs[19]) << 24) | (uint64(bs[20]) << 32) | (uint64(bs[21]) << 40) | (uint64(bs[22]) << 48) | (uint64(bs[23]) << 56)))
	t.LatestAcceptedInst = int32((uint32(bs[24]) | (uint32(bs[25]) << 8) | (uint32(bs[26]) << 16) | (uint32(bs[27]) << 24)))
	return nil
}

func (t *PromiseReply) New() fastrpc.Serializable {
	return new(PromiseReply)
}
func (t *PromiseReply) BinarySize() (nbytes int, sizeKnown bool) {
	return 16, true
}

type PromiseReplyCache struct {
	mu	sync.Mutex
	cache	[]*PromiseReply
}

func NewPromiseReplyCache() *PromiseReplyCache {
	c := &PromiseReplyCache{}
	c.cache = make([]*PromiseReply, 0)
	return c
}

func (p *PromiseReplyCache) Get() *PromiseReply {
	var t *PromiseReply
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &PromiseReply{}
	}
	return t
}
func (p *PromiseReplyCache) Put(t *PromiseReply) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *PromiseReply) Marshal(wire io.Writer) {
	var b [16]byte
	var bs []byte
	bs = b[:16]
	tmp32 := t.ReplicaId
	bs[0] = byte(tmp32)
	bs[1] = byte(tmp32 >> 8)
	bs[2] = byte(tmp32 >> 16)
	bs[3] = byte(tmp32 >> 24)
	tmp32 = t.LeaseInstance
	bs[4] = byte(tmp32)
	bs[5] = byte(tmp32 >> 8)
	bs[6] = byte(tmp32 >> 16)
	bs[7] = byte(tmp32 >> 24)
	tmp64 := t.TimestampNs
	bs[8] = byte(tmp64)
	bs[9] = byte(tmp64 >> 8)
	bs[10] = byte(tmp64 >> 16)
	bs[11] = byte(tmp64 >> 24)
	bs[12] = byte(tmp64 >> 32)
	bs[13] = byte(tmp64 >> 40)
	bs[14] = byte(tmp64 >> 48)
	bs[15] = byte(tmp64 >> 56)
	wire.Write(bs)
}

func (t *PromiseReply) Unmarshal(wire io.Reader) error {
	var b [16]byte
	var bs []byte
	bs = b[:16]
	if _, err := io.ReadAtLeast(wire, bs, 16); err != nil {
		return err
	}
	t.ReplicaId = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	t.LeaseInstance = int32((uint32(bs[4]) | (uint32(bs[5]) << 8) | (uint32(bs[6]) << 16) | (uint32(bs[7]) << 24)))
	t.TimestampNs = int64((uint64(bs[8]) | (uint64(bs[9]) << 8) | (uint64(bs[10]) << 16) | (uint64(bs[11]) << 24) | (uint64(bs[12]) << 32) | (uint64(bs[13]) << 40) | (uint64(bs[14]) << 48) | (uint64(bs[15]) << 56)))
	return nil
}

func (t *LeaseMetadata) New() fastrpc.Serializable {
	return new(LeaseMetadata)
}
func (t *LeaseMetadata) BinarySize() (nbytes int, sizeKnown bool) {
	return 0, false
}

type LeaseMetadataCache struct {
	mu	sync.Mutex
	cache	[]*LeaseMetadata
}

func NewLeaseMetadataCache() *LeaseMetadataCache {
	c := &LeaseMetadataCache{}
	c.cache = make([]*LeaseMetadata, 0)
	return c
}

func (p *LeaseMetadataCache) Get() *LeaseMetadata {
	var t *LeaseMetadata
	p.mu.Lock()
	if len(p.cache) > 0 {
		t = p.cache[len(p.cache)-1]
		p.cache = p.cache[0:(len(p.cache) - 1)]
	}
	p.mu.Unlock()
	if t == nil {
		t = &LeaseMetadata{}
	}
	return t
}
func (p *LeaseMetadataCache) Put(t *LeaseMetadata) {
	p.mu.Lock()
	p.cache = append(p.cache, t)
	p.mu.Unlock()
}
func (t *LeaseMetadata) Marshal(wire io.Writer) {
	var b [10]byte
	var bs []byte
	bs = b[:]
	alen1 := int64(len(t.Quorum))
	if wlen := binary.PutVarint(bs, alen1); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen1; i++ {
		bs = b[:4]
		tmp32 := t.Quorum[i]
		bs[0] = byte(tmp32)
		bs[1] = byte(tmp32 >> 8)
		bs[2] = byte(tmp32 >> 16)
		bs[3] = byte(tmp32 >> 24)
		wire.Write(bs)
	}
	bs = b[:]
	alen2 := int64(len(t.ObjectKeys))
	if wlen := binary.PutVarint(bs, alen2); wlen >= 0 {
		wire.Write(b[0:wlen])
	}
	for i := int64(0); i < alen2; i++ {
		t.ObjectKeys[i].Marshal(wire)
	}
	bs = b[:2]
	bs[0] = byte(t.IgnoreReplicas)
	bs[1] = byte(t.ReinstateReplicas)
	wire.Write(bs)
}

func (t *LeaseMetadata) Unmarshal(rr io.Reader) error {
	var wire byteReader
	var ok bool
	if wire, ok = rr.(byteReader); !ok {
		wire = bufio.NewReader(rr)
	}
	var b [10]byte
	var bs []byte
	alen1, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	t.Quorum = make([]int32, alen1)
	for i := int64(0); i < alen1; i++ {
		bs = b[:4]
		if _, err := io.ReadAtLeast(wire, bs, 4); err != nil {
			return err
		}
		t.Quorum[i] = int32((uint32(bs[0]) | (uint32(bs[1]) << 8) | (uint32(bs[2]) << 16) | (uint32(bs[3]) << 24)))
	}
	alen2, err := binary.ReadVarint(wire)
	if err != nil {
		return err
	}
	t.ObjectKeys = make([]state.Key, alen2)
	for i := int64(0); i < alen2; i++ {
		t.ObjectKeys[i].Unmarshal(wire)
	}
	bs = b[:2]
	if _, err := io.ReadAtLeast(wire, bs, 2); err != nil {
		return err
	}
	t.IgnoreReplicas = uint8(bs[0])
	t.ReinstateReplicas = uint8(bs[1])
	return nil
}
