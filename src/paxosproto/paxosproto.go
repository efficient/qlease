package paxosproto

import (
    "state"
)

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
    Command []state.Command
}

type Accept struct {
    LeaderId int32
    Instance int32
    Ballot int32
    Command []state.Command
    LeaseInstance int32
    OriginReplica int32
    PropId int32
}

type AcceptReply struct {
    Instance int32
    OK uint8
    Ballot int32
    LeaseInstance int32
    OriginReplica int32
    PropId int32
}

type Commit struct {
    LeaderId int32
    Instance int32
    Ballot int32
    Command []state.Command
}

type CommitShort struct {
    LeaderId int32
    Instance int32
    Count int32
    Ballot int32
}

type Forward struct {
    ReplicaId int32
    PropId int32
    Command state.Command
}

type ForwardReply struct {
    PropId int32
    OK uint8
    Value state.Value
}
