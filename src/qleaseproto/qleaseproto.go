package qleaseproto

import (
	"state"
)

// The guard message is used before non-renewal lease promises to
// bound the lease expiry time for the grantor.
type Guard struct {
    ReplicaId int32
    TimestampNs int64
    GuardDuration int64
}

type GuardReply struct {
    ReplicaId int32
    TimestampNs int64
}

type Promise struct {
    ReplicaId int32
    LeaseInstance int32
    TimestampNs int64
    DurationNs int64
    LatestAcceptedInst int32
}

type PromiseReply struct {
    ReplicaId int32
    LeaseInstance int32
    TimestampNs int64
}

type LeaseMetadata struct {
    Quorum []int32
    ObjectKeys []state.Key
    IgnoreReplicas uint8     // flag indicating that the replica(s) in Quorum should be excluded from all their current quorums
    ReinstateReplicas uint8  // flag indicating that the replica(s) in Quorum are to be reinstated as replicas of their quorums
}


