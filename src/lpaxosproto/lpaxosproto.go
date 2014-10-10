package lpaxosproto

import (
    "qleaseproto"
)

type ProposeLease struct {
    ReplicaId int32
    Updates []qleaseproto.LeaseMetadata
}

type Prepare struct {
    LeaderId int32
    Instance int32
    Ballot int32
    ToInfinity uint8
}

type PrepareReply struct {
    Instance int32
    OK uint8
    Ballot int32
    LeaseUpdate []qleaseproto.LeaseMetadata
}

type Accept struct {
    LeaderId int32
    Instance int32
    Ballot int32
    LeaseUpdate []qleaseproto.LeaseMetadata
}

type AcceptReply struct {
    Instance int32
    OK uint8
    Ballot int32
}

type Commit struct {
    LeaderId int32
    Instance int32
    Ballot int32
    LeaseUpdate []qleaseproto.LeaseMetadata
}

type CommitShort struct {
    LeaderId int32
    Instance int32
    Count int32
    Ballot int32
}

