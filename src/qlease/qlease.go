package qlease

import (
    "time"
)

const GUARD_DURATION_NS = 1 * 1e9 // 1 second
const DEFAULT_LEASE_DURATION_NS = 2000 * 1e6 // 2000 ms

type Lease struct {
    PromisedByMeInst int32                  // the current lease instance for which we've sent promises
    PromisedToMeInst int32                  // the lease instance for which we've received promises 
    Duration int64
    LatestTsSent int64
    LatestPromisesReceived []int64
    LatestRepliesReceived []int64
    ReadLocallyUntil int64
    WriteInQuorumUntil int64
    PromiseRejects int
    GuardExpires []int64
}

func NewLease(n int) *Lease {
    return &Lease{
        -1,
        -1,
        DEFAULT_LEASE_DURATION_NS,
        0,
        make([]int64, n),
        make([]int64, n),
        0,
        0,
        0,
        make([]int64, n)}
}


func (ql *Lease) CanRead() bool {
    if ql.PromisedToMeInst < 0 {
        return false
    }
    now := time.Now().UnixNano()
    if now > ql.ReadLocallyUntil {
        return false
    }
    return true
}

func (ql *Lease) CanWriteOutside() bool {
    if ql.PromisedByMeInst < 0 {
        return true
    }
    now := time.Now().UnixNano()
    if now < ql.WriteInQuorumUntil {
        return false
    }
    return true
}

