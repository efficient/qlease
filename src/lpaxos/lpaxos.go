package lpaxos

/************************************************************************************************
*************************************************************************************************

 PAXOS FOR LEASES

*************************************************************************************************
*************************************************************************************************/


import (
    "fastrpc"
    "dlog"
    "log"
//    "time"
    "genericsmr"
    "qleaseproto"
    "lpaxosproto"
    "encoding/binary"
)

const CHAN_BUFFER_SIZE = 2000
const TRUE = uint8(1)
const FALSE = uint8(0)

const MAX_BATCH = 5000

type Replica struct {
    *genericsmr.Replica                                 // extends a generic Paxos replica
    ProposeLeaseChan chan fastrpc.Serializable
    prepareChan chan fastrpc.Serializable
    acceptChan chan fastrpc.Serializable
    commitChan chan fastrpc.Serializable
    commitShortChan chan fastrpc.Serializable
    prepareReplyChan chan fastrpc.Serializable
    acceptReplyChan chan fastrpc.Serializable
    proposeLeaseRPC uint8
    prepareRPC uint8
    acceptRPC uint8
    commitRPC uint8
    commitShortRPC uint8
    prepareReplyRPC uint8
    acceptReplyRPC uint8
    IsLeader bool                                       // does this replica think it is the leader
    InstanceSpace []*Instance                           // the space of all instances (used and not yet used)
    crtInstance int32                                   // highest active instance number that this replica knows about
    defaultBallot int32                                 // default ballot for new instances (0 until a Prepare(ballot, instance->infinity) from a leader)
    Shutdown bool
    counter int
    flush bool
    LeaderId int32
    LatestCommitted int32
}

type InstanceStatus int

const (
    PREPARING InstanceStatus = iota
    PREPARED
    ACCEPTED
    COMMITTED
)

type Instance struct {
    Updates []qleaseproto.LeaseMetadata
    ballot int32
    Status InstanceStatus
    lb *LeaderBookkeeping
}

type LeaderBookkeeping struct {
    maxRecvBallot int32
    prepareOKs int
    acceptOKs int
    nacks int
}

func NewReplica(id int, peerAddrList []string, thrifty bool, exec bool, dreply bool, durable bool) *Replica {
    r := &Replica{genericsmr.NewReplica(id, peerAddrList, thrifty, exec, dreply),
                  make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
                  make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
                  make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
                  make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
                  make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
                  make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
                  make(chan fastrpc.Serializable, 3 * genericsmr.CHAN_BUFFER_SIZE),
                  0, 0, 0, 0, 0, 0, 0,
                  false,
                  make([]*Instance, 15 * 1024 * 1024),
                  0,
                  -1,
                  false,
                  0,
                  true,
                  -1,
                  -1}

    r.Durable = durable

    r.proposeLeaseRPC = r.RegisterRPC(new(lpaxosproto.ProposeLease), r.ProposeLeaseChan)
    r.prepareRPC = r.RegisterRPC(new(lpaxosproto.Prepare), r.prepareChan)
    r.acceptRPC = r.RegisterRPC(new(lpaxosproto.Accept), r.acceptChan)
    r.commitRPC = r.RegisterRPC(new(lpaxosproto.Commit), r.commitChan)
    r.commitShortRPC = r.RegisterRPC(new(lpaxosproto.CommitShort), r.commitShortChan)
    r.prepareReplyRPC = r.RegisterRPC(new(lpaxosproto.PrepareReply), r.prepareReplyChan)
    r.acceptReplyRPC = r.RegisterRPC(new(lpaxosproto.AcceptReply), r.acceptReplyChan)

    go r.run()

    return r
}

//append a log entry to stable storage
func (r *Replica) recordInstanceMetadata(inst *Instance) {
    if !r.Durable {
        return
    }

    var b [5]byte
    binary.LittleEndian.PutUint32(b[0:4], uint32(inst.ballot))
    b[4] = byte(inst.Status)
    r.StableStore.Write(b[:])
}

//write a sequence of commands to stable storage
/*
func (r *Replica) recordCommands(cmds []state.Command) {
    if !r.Durable {
        return
    }

    if cmds == nil {
        return
    }
    for i := 0; i < len(cmds); i++ {
        cmds[i].Marshal(io.Writer(r.StableStore))
    }
}

//sync with the stable store
func (r *Replica) sync() {
    if !r.Durable {
        return
    }

    r.StableStore.Sync()
}

*/

/* The interface with the local Paxos/EPaxos/Mencius/etc replica */

func (r *Replica) ProposeLeaseChange(updates []qleaseproto.LeaseMetadata) bool {
    if r.IsLeader {
        r.ProposeLeaseChan <- &lpaxosproto.ProposeLease{r.Id, updates}
        return true
    } else if r.LeaderId >= 0 {
        r.SendMsg(r.LeaderId, r.proposeLeaseRPC, &lpaxosproto.ProposeLease{r.Id, updates})
        return true
    }
    return false
}

func (r *Replica) BeTheLeader() {
    r.IsLeader = true
    r.LeaderId = r.Id
}


/* ============= */


/* Main event processing loop */

func (r *Replica) run() {

    r.ConnectToPeers()

    dlog.Println("Waiting for client connections")

    if r.Id == 0 {
        r.IsLeader = true
    }

	for !r.Shutdown {

        select {

        case proposeS := <-r.ProposeLeaseChan:
            propose := proposeS.(*lpaxosproto.ProposeLease)
            //got a Propose from a client
            dlog.Printf("Lease Update Proposal from replica %d\n", propose.ReplicaId)
            r.handleProposeLease(propose)
            break

        case prepareS := <-r.prepareChan:
            prepare := prepareS.(*lpaxosproto.Prepare)
            //got a Prepare message
            dlog.Printf("Received Prepare from replica %d, for instance %d\n", prepare.LeaderId, prepare.Instance)
            r.handlePrepare(prepare)
            break

        case acceptS := <-r.acceptChan:
            accept := acceptS.(*lpaxosproto.Accept)
            //got an Accept message
            dlog.Printf("Received Accept from replica %d, for instance %d\n", accept.LeaderId, accept.Instance)
            r.handleAccept(accept)
            break

        case commitS := <-r.commitChan:
            commit := commitS.(*lpaxosproto.Commit)
            //got a Commit message
            dlog.Printf("Received Commit from replica %d, for instance %d\n", commit.LeaderId, commit.Instance)
            r.handleCommit(commit)
            break

        case commitS := <-r.commitShortChan:
            commit := commitS.(*lpaxosproto.CommitShort)
            //got a Commit message
            dlog.Printf("Received short Commit from replica %d, for instance %d\n", commit.LeaderId, commit.Instance)
            r.handleCommitShort(commit)
            break

        case prepareReplyS := <-r.prepareReplyChan:
            prepareReply := prepareReplyS.(*lpaxosproto.PrepareReply)
            //got a Prepare reply
            dlog.Printf("Received PrepareReply for instance %d\n", prepareReply.Instance)
            r.handlePrepareReply(prepareReply)
            break

        case acceptReplyS := <-r.acceptReplyChan:
            acceptReply := acceptReplyS.(*lpaxosproto.AcceptReply)
            //got an Accept reply
            dlog.Printf("Received AcceptReply for instance %d\n", acceptReply.Instance)
            r.handleAcceptReply(acceptReply)
            break
        }
    }
}

func (r *Replica) makeUniqueBallot(ballot int32) int32 {
    return (ballot << 4) | r.Id
}

func (r *Replica) replyPrepare(replicaId int32, reply *lpaxosproto.PrepareReply) {
    r.SendMsg(replicaId, r.prepareReplyRPC, reply)
}

func (r *Replica) replyAccept(replicaId int32, reply *lpaxosproto.AcceptReply) {
    r.SendMsg(replicaId, r.acceptReplyRPC, reply)
}

func (r *Replica) bcastPrepare(instance int32, ballot int32, toInfinity bool) {
    defer func() {
        if err := recover(); err != nil {
            log.Println("Prepare bcast failed:", err)
        }
    }()
    ti := FALSE
    if toInfinity {
        ti = TRUE
    }
    args := &lpaxosproto.Prepare{r.Id, instance, ballot, ti}

    n := r.N - 1
    if r.Thrifty {
        n = r.N >> 1
    }
    q := r.Id

    for sent := 0; sent < n; {
        q = (q + 1) % int32(r.N)
        if q == r.Id {
            break
        }
        if !r.Alive[q] {
            continue
        }
        sent++
        r.SendMsg(q, r.prepareRPC, args)
    }
}

var pa lpaxosproto.Accept

func (r *Replica) bcastAccept(instance int32, ballot int32, leaseUpdate []qleaseproto.LeaseMetadata) {
    defer func() {
        if err := recover(); err != nil {
            log.Println("Accept bcast failed:", err)
        }
    }()
    pa.LeaderId = r.Id
    pa.Instance = instance
    pa.Ballot = ballot
    pa.LeaseUpdate = leaseUpdate
    args := &pa

    n := r.N - 1
    if r.Thrifty {
        n = r.N >> 1
    }
    q := r.Id

    for sent := 0; sent < n; {
        q = (q + 1) % int32(r.N)
        if q == r.Id {
            break
        }
        if !r.Alive[q] {
            continue
        }
        sent++
        r.SendMsg(q, r.acceptRPC, args)
    }
}

var pc lpaxosproto.Commit
var pcs lpaxosproto.CommitShort

func (r *Replica) bcastCommit(instance int32, ballot int32, leaseUpdate []qleaseproto.LeaseMetadata) {
    defer func() {
        if err := recover(); err != nil {
            log.Println("Commit bcast failed:", err)
        }
    }()
    pc.LeaderId = r.Id
    pc.Instance = instance
    pc.Ballot = ballot
    pc.LeaseUpdate = leaseUpdate

    args := &pc

    pcs.LeaderId = r.Id
    pcs.Instance = instance
    pcs.Ballot = ballot
    pcs.Count = int32(len(leaseUpdate))
    argsShort := &pcs

    n := r.N - 1
    if r.Thrifty {
        n = r.N >> 1
    }
    q := r.Id
    sent := 0

    for sent < n {
        q = (q + 1) % int32(r.N)
        if q == r.Id {
            break
        }
        if !r.Alive[q] {
            continue
        }
        sent++
        r.SendMsg(q, r.commitShortRPC, argsShort)
    }
    if r.Thrifty && q != r.Id {
        for sent < r.N - 1 {
            q = (q + 1) % int32(r.N)
            if q == r.Id {
                break
            }
            if !r.Alive[q] {
                continue
            }
            sent++
            r.SendMsg(q, r.commitRPC, args)
        }
    }
}

//TODO: Propose lease updates

func (r *Replica) handleProposeLease(propose *lpaxosproto.ProposeLease) {
    if !r.IsLeader {
        //TODO: should notify sender? not necessary, but may speed things up
        return
    }

    for r.InstanceSpace[r.crtInstance] != nil {
        r.crtInstance++
    }

    instNo := r.crtInstance
    r.crtInstance++


    if r.defaultBallot == -1 {
        r.InstanceSpace[instNo] = &Instance{
            propose.Updates,
            r.makeUniqueBallot(0),
            PREPARING,
            &LeaderBookkeeping{0, 0, 0, 0}}
        r.bcastPrepare(instNo, r.makeUniqueBallot(0), true)
        dlog.Printf("Classic round for instance %d\n", instNo)
    } else {
        r.InstanceSpace[instNo] = &Instance{
            propose.Updates,
            r.defaultBallot,
            PREPARED,
            &LeaderBookkeeping{0, 0, 0, 0}}

        /*r.recordInstanceMetadata(r.InstanceSpace[instNo])
        r.recordCommands(cmds)
        r.sync()*/

        r.bcastAccept(instNo, r.defaultBallot, propose.Updates)
        dlog.Printf("Fast round for instance %d\n", instNo)
    }
}

func (r *Replica) handlePrepare(prepare *lpaxosproto.Prepare) {
    inst := r.InstanceSpace[prepare.Instance]
    var preply *lpaxosproto.PrepareReply

    if inst == nil {
        ok := TRUE
        if r.defaultBallot > prepare.Ballot {
            ok = FALSE
        }
        preply = &lpaxosproto.PrepareReply{prepare.Instance, ok, r.defaultBallot, nil}
    } else {
        ok := TRUE
        if prepare.Ballot < inst.ballot {
            ok = FALSE
        }
        preply = &lpaxosproto.PrepareReply{prepare.Instance, ok, inst.ballot, inst.Updates}
    }

    r.replyPrepare(prepare.LeaderId, preply)

    if prepare.ToInfinity == TRUE && prepare.Ballot > r.defaultBallot {
        r.defaultBallot = prepare.Ballot
    }
}

func (r *Replica) handleAccept(accept *lpaxosproto.Accept) {
    inst := r.InstanceSpace[accept.Instance]
    var areply *lpaxosproto.AcceptReply

    if inst == nil {
        if accept.Ballot < r.defaultBallot {
            areply = &lpaxosproto.AcceptReply{accept.Instance, FALSE, r.defaultBallot}
        } else {
            r.InstanceSpace[accept.Instance] = &Instance{
                accept.LeaseUpdate,
                accept.Ballot,
                ACCEPTED,
                nil}
            areply = &lpaxosproto.AcceptReply{accept.Instance, TRUE, r.defaultBallot}
        }
    } else if inst.ballot > accept.Ballot {
        areply = &lpaxosproto.AcceptReply{accept.Instance, FALSE, inst.ballot}
    } else if inst.ballot < accept.Ballot {
        inst.Updates = accept.LeaseUpdate
        inst.ballot = accept.Ballot
        inst.Status = ACCEPTED
        areply = &lpaxosproto.AcceptReply{accept.Instance, TRUE, inst.ballot}
        //try to propose in a different instance or just give up?
    } else {
        // reordered ACCEPT
        r.InstanceSpace[accept.Instance].Updates = accept.LeaseUpdate
        if r.InstanceSpace[accept.Instance].Status != COMMITTED {
            r.InstanceSpace[accept.Instance].Status = ACCEPTED
        } else {
            if r.LatestCommitted < accept.Instance {
                r.LatestCommitted = accept.Instance
            }
        }
        areply = &lpaxosproto.AcceptReply{accept.Instance, TRUE, r.defaultBallot}
    }

    /*if areply.OK == TRUE {
        r.recordInstanceMetadata(r.InstanceSpace[accept.Instance])
        r.recordCommands(accept.Command)
        r.sync()
    }*/

    r.replyAccept(accept.LeaderId, areply)
}

func (r *Replica) handleCommit(commit *lpaxosproto.Commit) {
    inst := r.InstanceSpace[commit.Instance]

    dlog.Printf("Committing instance %d\n", commit.Instance);

    if inst == nil {
        r.InstanceSpace[commit.Instance] = &Instance{
            commit.LeaseUpdate,
            commit.Ballot,
            COMMITTED,
            nil}
    } else {
        r.InstanceSpace[commit.Instance].Updates = commit.LeaseUpdate
        r.InstanceSpace[commit.Instance].Status = COMMITTED
        r.InstanceSpace[commit.Instance].ballot = commit.Ballot
        //try to propose in a different instance or just give up?
    }

    if r.LatestCommitted < commit.Instance {
        r.LatestCommitted = commit.Instance
    }

    /*r.recordInstanceMetadata(r.InstanceSpace[commit.Instance])
    r.recordUpdates(commit.Updates)*/
}

func (r *Replica) handleCommitShort(commit *lpaxosproto.CommitShort) {
    inst := r.InstanceSpace[commit.Instance]

    if inst != nil && inst.Updates != nil {
        log.Printf("Lease update commit at replica %d\n", r.Id)
    }

    dlog.Printf("Committing instance %d\n", commit.Instance);

    if inst == nil {
        r.InstanceSpace[commit.Instance] = &Instance{nil,
            commit.Ballot,
            COMMITTED,
            nil}
    } else {
        r.InstanceSpace[commit.Instance].Status = COMMITTED
        r.InstanceSpace[commit.Instance].ballot = commit.Ballot
        if r.LatestCommitted < commit.Instance {
            r.LatestCommitted = commit.Instance
        }
        //try to propose in a different instance or just give up?
    }
    //r.recordInstanceMetadata(r.InstanceSpace[commit.Instance])
}


func (r *Replica) handlePrepareReply(preply *lpaxosproto.PrepareReply) {
    inst := r.InstanceSpace[preply.Instance]

    if inst.Status != PREPARING {
        // TODO: should replies for non-current ballots be ignored?
        // we've moved on -- these are delayed replies, so just ignore
        return
    }

    if preply.OK == TRUE {
        inst.lb.prepareOKs++

        if preply.Ballot > inst.lb.maxRecvBallot {
            inst.Updates = preply.LeaseUpdate
            inst.lb.maxRecvBallot = preply.Ballot
            //try to propose in a different instance or just give up?
        }

        if inst.lb.prepareOKs + 1 > r.N >> 1 {
            inst.Status = PREPARED
            inst.lb.nacks = 0
            if inst.ballot > r.defaultBallot {
                r.defaultBallot = inst.ballot
            }
            /*r.recordInstanceMetadata(r.InstanceSpace[preply.Instance])
            r.sync()*/
            r.bcastAccept(preply.Instance, inst.ballot, inst.Updates)
        }
    } else {
        // TODO: there is probably another active leader
        inst.lb.nacks++
        if preply.Ballot > inst.lb.maxRecvBallot {
            inst.lb.maxRecvBallot = preply.Ballot
        }
        if inst.lb.nacks >= r.N >> 1 {
            //try to propose in a different instance or just give up?
        }
    }
}

func (r *Replica) handleAcceptReply(areply *lpaxosproto.AcceptReply) {
    inst := r.InstanceSpace[areply.Instance]

    if inst.Status != PREPARED && inst.Status != ACCEPTED {
        // we've move on, these are delayed replies, so just ignore
        return
    }

    if areply.OK == TRUE {
        inst.lb.acceptOKs++
        if inst.lb.acceptOKs + 1 > r.N >> 1 {
            inst = r.InstanceSpace[areply.Instance]
            inst.Status = COMMITTED
            
            /*
            r.recordInstanceMetadata(r.InstanceSpace[areply.Instance])
            r.sync()//is this necessary?*/

            if r.LatestCommitted < areply.Instance {
                r.LatestCommitted = areply.Instance
            }

            r.bcastCommit(areply.Instance, inst.ballot, inst.Updates)
        }
    } else {
        // TODO: there is probably another active leader
        inst.lb.nacks++
        if areply.Ballot > inst.lb.maxRecvBallot {
            inst.lb.maxRecvBallot = areply.Ballot
        }
        if inst.lb.nacks >= r.N >> 1 {
        // TODO
        }
    }
}








