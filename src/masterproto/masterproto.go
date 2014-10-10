package masterproto

type RegisterArgs struct {
    Addr string
    Port int
    LeasePort int
}

type RegisterReply struct {
    ReplicaId int
    NodeList []string
    LeaseNodeList []string
    Ready bool
}

type GetLeaderArgs struct {
}

type GetLeaderReply struct {
    LeaderId int
}

type GetReplicaListArgs struct {
}

type GetReplicaListReply struct {
    ReplicaList []string
    Ready bool
}
