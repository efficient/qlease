package paxos

import (
    "fastrpc"
    "dlog"
    "log"
    "time"
    "state"
    "genericsmr"
    "genericsmrproto"
    "qlease"
    "qleaseproto"
    "paxosproto"
    "lpaxos"
    "lpaxosproto"
    "io"
    "encoding/binary"
    "sync"
)

const CHAN_BUFFER_SIZE = 500000
const READ_CHAN_SIZE = 1100000
const TRUE = uint8(1)
const FALSE = uint8(0)
const HT_INIT_SIZE = 150000

const MAX_BATCH = 1
const GRACE_PERIOD = 5 * 1e9

type Replica struct {
    *genericsmr.Replica                                 // extends a generic Paxos replica
    prepareChan chan fastrpc.Serializable
    acceptChan chan fastrpc.Serializable
    commitChan chan fastrpc.Serializable
    commitShortChan chan fastrpc.Serializable
    prepareReplyChan chan fastrpc.Serializable
    acceptReplyChan chan fastrpc.Serializable
    forwardChan chan fastrpc.Serializable
    forwardReplyChan chan fastrpc.Serializable
    prepareRPC uint8
    acceptRPC uint8
    commitRPC uint8
    commitShortRPC uint8
    prepareReplyRPC uint8
    acceptReplyRPC uint8
    forwardRPC uint8
    forwardReplyRPC uint8
    IsLeader bool                                       // does this replica think it is the leader
    leaderId int32
    instanceSpace []*Instance                           // the space of all instances (used and not yet used)
    crtInstance int32                                   // highest active instance number that this replica knows about
    defaultBallot int32                                 // default ballot for new instances (0 until a Prepare(ballot, instance->infinity) from a leader)
    Shutdown bool
    counter int
    flush bool
    leaseSMR *lpaxos.Replica                            // we use Lease-Paxos to achieve consensus on lease state
    keyToQuorum map[state.Key][]int32                   // the write quorum for each key
    keyGranted map[state.Key]bool                       // map with keys granted to the current replica
    latestAcceptedInst int32                            // latest instance accepted or committed
    delayedInstances chan int32                         // used to keep track of instances delayed because of lease mismatches
    updating map[state.Key]int
    updatingLock *sync.Mutex
    newestInstanceIDontKnow int32
    committedUpTo int32
    readsChannel chan *genericsmr.Propose
    fwdReadsChannel chan *paxosproto.Forward
    fwdPropMap map[int32]*genericsmr.Propose
    fwdId int32
    readStats *ReadStats
    maintainReadStats bool
    directAcks bool
    disasbledReplica []bool
    noReplicasDisabled bool
}

type InstanceStatus int8

const (
    NONE InstanceStatus = iota
    PREPARING
    PREPARED
    ACCEPTED
    COMMITTED
)

type Instance struct {
    cmds []state.Command
    ballot int32
    status InstanceStatus
    lb *LeaderBookkeeping
    directAcks int8
    sentReply bool
}

type LeaderBookkeeping struct {
    clientProposals []*genericsmr.Propose
    maxRecvBallot int32
    prepareOKs int
    acceptOKs int
    nacks int
    acceptOKsToWait int
}

func NewReplica(id int, peerAddrList []string, thrifty bool, exec bool, dreply bool, durable bool, beacon bool, leaseRep *lpaxos.Replica, directAcks bool) *Replica {
    r := &Replica{genericsmr.NewReplica(id, peerAddrList, thrifty, exec, dreply),
                  make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
                  make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
                  make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
                  make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
                  make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
                  make(chan fastrpc.Serializable, 3 * genericsmr.CHAN_BUFFER_SIZE),
                  make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
                  make(chan fastrpc.Serializable, genericsmr.CHAN_BUFFER_SIZE),
                  0, 0, 0, 0, 0, 0, 0, 0,
                  false,
                  0,
                  make([]*Instance, 15 * 1024 * 1024),
                  0,
                  -1,
                  false,
                  0,
                  true,
                  leaseRep,
                  make(map[state.Key][]int32, HT_INIT_SIZE),
                  make(map[state.Key]bool, HT_INIT_SIZE),
                  -1,
                  make(chan int32, genericsmr.CHAN_BUFFER_SIZE),
                  make(map[state.Key]int, 15000),
                  new(sync.Mutex),
                  -1,
                  -1,
                  make(chan *genericsmr.Propose, READ_CHAN_SIZE),
                  make(chan *paxosproto.Forward, READ_CHAN_SIZE),
                  make(map[int32]*genericsmr.Propose, 200000),
                  0,
                  nil,
                  true,
                  directAcks,
                  make([]bool, len(peerAddrList)),
                  true}

    r.Durable = durable
    r.Beacon = beacon

    r.prepareRPC = r.RegisterRPC(new(paxosproto.Prepare), r.prepareChan)
    r.acceptRPC = r.RegisterRPC(new(paxosproto.Accept), r.acceptChan)
    r.commitRPC = r.RegisterRPC(new(paxosproto.Commit), r.commitChan)
    r.commitShortRPC = r.RegisterRPC(new(paxosproto.CommitShort), r.commitShortChan)
    r.prepareReplyRPC = r.RegisterRPC(new(paxosproto.PrepareReply), r.prepareReplyChan)
    r.acceptReplyRPC = r.RegisterRPC(new(paxosproto.AcceptReply), r.acceptReplyChan)
    r.forwardRPC = r.RegisterRPC(new(paxosproto.Forward), r.forwardChan)
    r.forwardReplyRPC = r.RegisterRPC(new(paxosproto.ForwardReply), r.forwardReplyChan)

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
    b[4] = byte(inst.status)
    r.StableStore.Write(b[:])
}

//write a sequence of commands to stable storage
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



/* RPC to be called by master */

func (r *Replica) BeTheLeader(args *genericsmrproto.BeTheLeaderArgs, reply *genericsmrproto.BeTheLeaderReply) error {
    r.IsLeader = true
    r.leaseSMR.BeTheLeader()
    return nil
}


/* ============= */

var leaseClockChan chan bool
var leaseClockRestart chan bool

var clockChan chan bool
func (r *Replica) clock() {
    for !r.Shutdown {
        time.Sleep(100 * 1e6) // 100 ms
        clockChan <- true
    }
}

//var hackflag int = 2
const TICKS_TO_RECONF_LEASE = 20
var ticks = 10

func (r *Replica) leaseClock() {
    for !r.Shutdown {
        time.Sleep(500 * 1e6) // 500 ms
        leaseClockChan <- true
        <-leaseClockRestart
    }
}
var newPromiseCount int = 0

/* Main event processing loop */

func (r *Replica) run() {

    r.ConnectToPeers()

    dlog.Println("Waiting for client connections")

    go r.WaitForClientConnections()

    if r.Exec {
        go r.executeCommands()
        go r.reader()
    }


    if r.Id == 0 {
        r.IsLeader = true
        r.readStats = NewReadStats(r.N, r.Id)
    }

    clockChan = make(chan bool, 1)
    go r.clock()

    r.QLease = qlease.NewLease(r.N)

    leaseClockChan = make(chan bool, 1)
    leaseClockRestart = make(chan bool)
    go r.leaseClock()

//	clockRang := false

    var tickCounter uint64 = 0
    latestBeaconFromReplica := make([]uint64, r.N)
    proposedDead := make([]bool, r.N)
    for i := 0; i < r.N; i++ {
        latestBeaconFromReplica[i] = 0
        proposedDead[i] = false
    }

    stopRenewing := false
    
    for !r.Shutdown {

        select {

        case <-clockChan:
            //clockRang = true
            tickCounter++
            if tickCounter % 20  == 0 {
                if r.Beacon {
                    for q := int32(0); q < int32(r.N); q++ {
                        if q == r.Id {
                            continue
                        }
                        r.SendBeacon(q)
                    }
                }
            
                if r.IsLeader && r.Beacon {
                    for rid := int32(0); rid < int32(r.N); rid++ {
                        if rid == r.Id {
                            continue
                        }
                        if !proposedDead[rid] && tickCounter - latestBeaconFromReplica[rid] >= 200 {
                            log.Println("Proposing replica dead: ", rid)
                            //replica might be dead
                            //propose a lease configuration change
                            dr := make([]int32, 1)
                            dr[0] = rid
                            r.proposeReplicasDead(dr)
                            proposedDead[rid] = true
                        }
                    }
                }
                
            	rightNow := time.Now().UnixNano()
                for rid := int32(0); rid < int32(r.N); rid++ {
                    if rid == r.Id {
                        continue
                    }
                    if !proposedDead[rid] && r.LastReplyReceivedTimestamp[rid] > 0 && rightNow - r.LastReplyReceivedTimestamp[rid] >= GRACE_PERIOD {
                        if r.IsLeader {
	                        log.Println("Proposing replica dead: ", rid)
	                        //replica might be dead
	                        //propose a lease configuration change
	                        dr := make([]int32, 1)
	                        dr[0] = rid
	                        r.proposeReplicasDead(dr)
                    	}
                        proposedDead[rid] = true
                        stopRenewing = true
                    }
                }

                if r.IsLeader {
                    for i := r.committedUpTo; i >= 0 && i < r.crtInstance; i++ {
                    	r.delayedInstances <- i
                    }
                }
            }
            break

        case propose := <-r.ProposeChan:
            //got a Propose from a client
            dlog.Printf("Proposal with op %d\n", propose.Command.Op)
            r.handlePropose(propose)
            //clockRang = false
            break

        case forwardS := <-r.forwardChan:
            forward := forwardS.(*paxosproto.Forward)
            dlog.Printf("Forward proposal from replica %d\n", forward.ReplicaId)
            r.handleForward(forward)
            break

        case frS := <-r.forwardReplyChan:
            fr := frS.(*paxosproto.ForwardReply)
            r.handleForwardReply(fr)
            break

        case prepareS := <-r.prepareChan:
            prepare := prepareS.(*paxosproto.Prepare)
            //got a Prepare message
            dlog.Printf("Received Prepare from replica %d, for instance %d\n", prepare.LeaderId, prepare.Instance)
            r.handlePrepare(prepare)
            break

        case acceptS := <-r.acceptChan:
            accept := acceptS.(*paxosproto.Accept)
            //got an Accept message
            dlog.Printf("Received Accept from replica %d, for instance %d\n", accept.LeaderId, accept.Instance)
            r.handleAccept(accept)
            break

        case commitS := <-r.commitChan:
            commit := commitS.(*paxosproto.Commit)
            //got a Commit message
            dlog.Printf("Received Commit from replica %d, for instance %d\n", commit.LeaderId, commit.Instance)
            r.handleCommit(commit)
            break

        case commitS := <-r.commitShortChan:
            commit := commitS.(*paxosproto.CommitShort)
            //got a Commit message
            dlog.Printf("Received Commit from replica %d, for instance %d\n", commit.LeaderId, commit.Instance)
            r.handleCommitShort(commit)
            break

        case prepareReplyS := <-r.prepareReplyChan:
            prepareReply := prepareReplyS.(*paxosproto.PrepareReply)
            //got a Prepare reply
            dlog.Printf("Received PrepareReply for instance %d\n", prepareReply.Instance)
            r.handlePrepareReply(prepareReply)
            break

        case acceptReplyS := <-r.acceptReplyChan:
            acceptReply := acceptReplyS.(*paxosproto.AcceptReply)
            //got an Accept reply
            dlog.Printf("Received AcceptReply for instance %d\n", acceptReply.Instance)
            r.handleAcceptReply(acceptReply)
            break

        case instNo := <-r.delayedInstances:
        	log.Println("Trying to re-start instance")
            inst := r.instanceSpace[instNo]
            if inst.status < COMMITTED {
                inst.status = PREPARED
                inst.lb.acceptOKs = 0
                inst.ballot = r.makeBallotLargerThan(inst.ballot)
                inst.lb.acceptOKsToWait, _ = r.bcastAccept(instNo, inst.ballot, inst.cmds, inst.lb.clientProposals[0].FwdReplica, inst.lb.clientProposals[0].FwdId)
                if genericsmr.SendError {
            		r.delayedInstances <- instNo
            	}
            }

        case beacon := <-r.BeaconChan:
            dlog.Printf("Received Beacon from replica %d with timestamp %d\n", beacon.Rid, beacon.Timestamp)
            //r.ReplyBeacon(beacon)
            latestBeaconFromReplica[beacon.Rid] = tickCounter
            break

        case guardS := <-r.QLGuardChan:
            guard := guardS.(*qleaseproto.Guard)
            r.HandleQLeaseGuard(r.QLease, guard)
            break

        case guardReplyS := <-r.QLGuardReplyChan:
            guardReply := guardReplyS.(*qleaseproto.GuardReply)
            r.HandleQLeaseGuardReply(r.QLease, guardReply, r.latestAcceptedInst)
            break

        case promiseS := <-r.QLPromiseChan:
            promise := promiseS.(*qleaseproto.Promise)
            prev := r.QLease.PromisedToMeInst
            if !r.HandleQLeasePromise(r.QLease, promise) {
                continue
            }
            if prev != r.QLease.PromisedToMeInst {
                r.updateGrantedKeys(prev)
                newPromiseCount = 0
            }
            newPromiseCount++
            if newPromiseCount <= r.N / 2 {
                if r.newestInstanceIDontKnow < promise.LatestAcceptedInst {
                   r.newestInstanceIDontKnow = promise.LatestAcceptedInst
                }
            }
            break

        case preplyS := <-r.QLPromiseReplyChan:
            preply := preplyS.(*qleaseproto.PromiseReply)
            r.HandleQLeaseReply(r.QLease, preply)
            break

        case <-leaseClockChan:
            if r.QLease.PromisedByMeInst < r.leaseSMR.LatestCommitted {
                // wait for previous lease to expire before switching to new config
                if r.QLease.CanWriteOutside() {
                    r.updateKeyQuorumInfo(r.leaseSMR.LatestCommitted)
                    r.QLease.PromisedByMeInst = r.leaseSMR.LatestCommitted
                    log.Printf("Replica %d - New lease for instance %d\n", r.Id, r.QLease.PromisedByMeInst)
                    r.EstablishQLease(r.QLease)
                    stopRenewing = false
                }
            } else if r.QLease.PromisedByMeInst >= 0 {
                if r.QLease.CanWriteOutside() {
                    r.EstablishQLease(r.QLease)
                    stopRenewing = false
                } else if !stopRenewing {
                    r.RenewQLease(r.QLease, r.latestAcceptedInst)
                }
            }

            if r.maintainReadStats {
                ticks--
                if ticks == 0 {
                    r.maintainReadStats = false
                    if r.IsLeader && r.readStats != nil {
                        go r.proposeLeaseReconf()
                    }

                    ticks = TICKS_TO_RECONF_LEASE
                    //ticks = 60
                }
            }
            // restart the clock
            leaseClockRestart <- true

        case <-r.OnClientConnect:
            log.Printf("reads: %d, local: %d\n", reads, local)
        }
    }
}

func (r *Replica) proposeLeaseReconf() {
    lms := r.readStats.GetQuorums()
    if lms != nil && len(lms) > 0 {
        r.leaseSMR.ProposeLeaseChan <- &lpaxosproto.ProposeLease{r.Id, lms}
        //r.readStats = NewReadStats(r.N, r.Id)
    }
    /*hackflag--
    if hackflag > 0 {
        r.maintainReadStats = true
    }*/
    r.maintainReadStats = true
}

func (r *Replica) proposeReplicasDead(rids []int32) {
    lms := make([]qleaseproto.LeaseMetadata, 1)
    lms[0] = qleaseproto.LeaseMetadata{rids, nil, TRUE, FALSE}
    r.leaseSMR.ProposeLeaseChan <- &lpaxosproto.ProposeLease{r.Id, lms}
}

func (r *Replica) makeUniqueBallot(ballot int32) int32 {
    return (ballot << 4) | r.Id
}

func (r *Replica) makeBallotLargerThan(ballot int32) int32 {
    return r.makeUniqueBallot((ballot >> 4) + 1)
}

func (r *Replica) replyPrepare(replicaId int32, reply *paxosproto.PrepareReply) {
    r.SendMsg(replicaId, r.prepareReplyRPC, reply)
}

func (r *Replica) replyAccept(replicaId int32, reply *paxosproto.AcceptReply) {
    r.SendMsg(replicaId, r.acceptReplyRPC, reply)
}

func (r *Replica) updateKeyQuorumInfo(li int32) {
    if li < 0 || r.leaseSMR.InstanceSpace[li] == nil || r.leaseSMR.InstanceSpace[li].Status != lpaxos.COMMITTED {
        return
    }
    for i := r.QLease.PromisedByMeInst + 1; i <= li; i++ {
        for _, upd := range r.leaseSMR.InstanceSpace[i].Updates {
            if upd.IgnoreReplicas == TRUE {
                for _, id := range upd.Quorum {
                    r.disasbledReplica[id] = true
                    log.Println("Excluded replica ", id)
                    r.Alive[id] = false
                }
                r.noReplicasDisabled = false
            } else if upd.ReinstateReplicas == TRUE {
                for _, id := range upd.Quorum {
                    r.disasbledReplica[id] = false
                    r.Alive[id] = true
                }
                r.noReplicasDisabled = true
                for i := int32(0); i < int32(r.N); i++ {
                    if r.disasbledReplica[i] {
                        r.noReplicasDisabled = false
                        break
                    }
                }
            } else {
                for _, k := range upd.ObjectKeys {
                    r.keyToQuorum[k] = upd.Quorum
                }
            }
        }
    }
}

func (r *Replica) updateGrantedKeys(li int32) {
    for i := li + 1; i <= r.QLease.PromisedToMeInst; i++ {
        for _, upd := range r.leaseSMR.InstanceSpace[i].Updates {
            found := false
            for _, rid := range upd.Quorum {
                if rid == r.Id {
                    found = true
                    break
                }
            }
            for _, k := range upd.ObjectKeys {
                if found {
                    r.keyGranted[k] = true
                } else {
                    delete(r.keyGranted, k)
                    //TODO: is marking it false faster?
                }
            }
        }
    }
}

func (r *Replica) addUpdatingKeys(cmds []state.Command) {
    r.updatingLock.Lock()
    defer r.updatingLock.Unlock()
    for i := 0; i < len(cmds); i++ {
        if u, present := r.updating[cmds[i].K]; present {
            r.updating[cmds[i].K] = u + 1
        } else {
            r.updating[cmds[i].K] = 1
        }
    }
}

func (r *Replica) removeUpdatingKeys(cmds []state.Command) {
    r.updatingLock.Lock()
    defer r.updatingLock.Unlock()
    for i := 0; i < len(cmds); i++ {
        if u, present := r.updating[cmds[i].K]; present {
            r.updating[cmds[i].K] = u - 1
        }
    }
}

func (r *Replica) isKeyUpdating(k state.Key) bool {
    if u, present := r.updating[k]; present && u > 0 {
        return true
    }
    return false
}

func (r *Replica) updateCommittedUpTo() {
    for r.instanceSpace[r.committedUpTo + 1] != nil &&
        r.instanceSpace[r.committedUpTo + 1].status == COMMITTED {
        r.committedUpTo++
    }
}

func (r *Replica) isKeyGranted(key state.Key) bool {
    if _, present := r.keyGranted[key]; !present {
        //return false
        if !r.IsLeader {
            return false
        }
        if _, pres := r.keyToQuorum[key]; pres {
            return false
        }
    }
    return true
}

func (r *Replica) isMyLeaseActive() bool {
    if r.QLease == nil || !r.QLease.CanRead() {
        return false
    }
    return true
}

func (r *Replica) getLeaseQuorumForKey(key state.Key, originReplica int32) []int32 {
    var q []int32
    present := false
    if q, present = r.keyToQuorum[key]; !present {
        q = make([]int32, r.N / 2)
        copy(q, r.PreferredPeerOrder[0 : r.N / 2])
    }
    if r.directAcks {
        found := false
        for _, rep := range q {
            if rep == originReplica {
                found = true
                break
            }
        }
        if !found && originReplica != r.Id && originReplica >= 0 {
            if present {
                q = append(q, originReplica)
            } else {
                q[len(q) - 1] = originReplica
            }
        }
    }

    if r.noReplicasDisabled {
        return q
    }

    taken := make([]bool, r.N)
    newQuorum := make([]int32, 0, r.N / 2)
    for _, id := range q {
        if !r.disasbledReplica[id] && id != r.Id {
            taken[id] = true
            newQuorum = append(newQuorum, id)
        }
    }
    for id := int32(0); id < int32(r.N); id++ {
        if len(newQuorum) >= r.N / 2 {
            break
        }
        if taken[id] || r.disasbledReplica[id] || id == r.Id {
            continue
        }
        newQuorum = append(newQuorum, id)
        taken[id] = true
    }
    if len(newQuorum) < r.N / 2 {
        //not enough valid replicas to form a quorum
        return nil
    }
    return newQuorum
}

func quorumToInt64(q []int32) int64 {
    r := int64(0)
    for _, i := range q {
        r = (r << 8) | int64(i)
    }
    return r
}

func (r *Replica) bcastPrepare(instance int32, ballot int32, toInfinity bool) error {
    ti := FALSE
    if toInfinity {
        ti = TRUE
    }
    args := &paxosproto.Prepare{r.Id, instance, ballot, ti}

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
    return nil
}

var pa paxosproto.Accept

func (r *Replica) bcastAccept(instance int32, ballot int32, command []state.Command, originReplica int32, fwdId int32) (int, error) {
    pa.LeaderId = r.Id
    pa.Instance = instance
    pa.Ballot = ballot
    pa.Command = command
    pa.LeaseInstance = r.QLease.PromisedByMeInst
    pa.OriginReplica = originReplica
    pa.PropId = fwdId
    args := &pa

    q := r.getLeaseQuorumForKey(command[0].K, originReplica)
    sent := 0

    for _, i := range q {
        if i == r.Id {
            continue
        }
        if err := r.SendMsg(i, r.acceptRPC, args); err != nil {
        	log.Println("Accept bcast failed:", err)
        	return 0, err
        }
        sent++
    }
    return sent, nil
}

var pc paxosproto.Commit
var pcs paxosproto.CommitShort

func (r *Replica) bcastCommit(instance int32, ballot int32, command []state.Command) error {
    pc.LeaderId = r.Id
    pc.Instance = instance
    pc.Ballot = ballot
    pc.Command = command

    args := &pc

    pcs.LeaderId = r.Id
    pcs.Instance = instance
    pcs.Ballot = ballot
    pcs.Count = int32(len(command))
    //argsShort := &pcs

    //n := r.N - 1

    q := r.Id
    sent := 0

    //TODO: send CommitShort to replicas that have already accepted

/*
    if r.Thrifty {
        n = r.N >> 1
    }

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
    if r.Thrifty && q != r.Id {*/
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
    //}
    return nil
}

var reads int = 0
var local int = 0

func (r *Replica) handlePropose(propose *genericsmr.Propose) {

    batches := make(map[int64][]state.Command, 10)
    proposals := make(map[int64][]*genericsmr.Propose, 10)

    haveWrites := false

    totalLen := len(r.ProposeChan) + 1

    if totalLen > MAX_BATCH {
        totalLen = MAX_BATCH
    }

    for i := 0; i < totalLen; i++ {
        if state.IsRead(&propose.Command) && (r.IsLeader || r.isKeyGranted(propose.Command.K)) {
            reads++
            //make sure that the channel is not going to be full,
            //because this may cause the consumer to block when trying to
            //put request back
            for len(r.readsChannel) >= READ_CHAN_SIZE - 10000 {
                time.Sleep(1000)
            }
            r.readsChannel <- propose
        } else if !r.IsLeader {
            if state.IsRead(&propose.Command) {
                reads++
            }
            r.fwdId++
            r.fwdPropMap[r.fwdId] = propose
            r.SendMsg(r.leaderId, r.forwardRPC, &paxosproto.Forward{r.Id, r.fwdId, propose.Command})    
        } else {
            q := quorumToInt64(r.getLeaseQuorumForKey(propose.Command.K, r.Id))
            var cmds []state.Command
            var present bool
            var props []*genericsmr.Propose
            props = proposals[q]
            if cmds, present = batches[q]; !present {
                cmds = make([]state.Command, 0, totalLen / 2 + 1)
                props = make([]*genericsmr.Propose, 0, totalLen / 2 + 1)
            }
            cmds = append(cmds, propose.Command)
            batches[q] = cmds
            props = append(props, propose)
            proposals[q] = props
            haveWrites = true
        }
        if i < totalLen - 1 {
            propose = <-r.ProposeChan
        }
    }

    if !haveWrites {
        return
    }

/*    if !r.IsLeader {
        preply := &genericsmrproto.ProposeReplyTS{FALSE, -1, state.NIL, 0}
        r.ReplyProposeTS(preply, propose)
        return
    }*/

    for r.instanceSpace[r.crtInstance] != nil {
        r.crtInstance++
    }

    //dlog.Printf("Batched %d\n", batchSize)

    status := PREPARING
    ballot := r.makeUniqueBallot(0)

    if r.defaultBallot > -1 {
        status = PREPARED
        ballot = r.defaultBallot
    }

    for q, cmds := range batches {
        if len(cmds) == 0 {
            continue
        }
        //log.Printf("Batching %d\n", len(cmds))
        props := proposals[q]
        r.instanceSpace[r.crtInstance] = &Instance {
            cmds,
            ballot,
            status,
            &LeaderBookkeeping{props, 0, 0, 0, 0, 0},
            0, false}
        if status == PREPARING {
            r.bcastPrepare(r.crtInstance, ballot, true)
            dlog.Printf("Classic round for instance %d\n", r.crtInstance)
        } else {
            r.recordInstanceMetadata(r.instanceSpace[r.crtInstance])
            r.recordCommands(cmds)
            r.sync()

            if r.crtInstance > r.latestAcceptedInst {
                r.latestAcceptedInst = r.crtInstance
            }

            if props[0].FwdReplica >= 0 && props[0].FwdReplica != r.Id {
                r.addUpdatingKeys(cmds)
            }

            //TODO: make sure it supports Forwards
            r.instanceSpace[r.crtInstance].lb.acceptOKsToWait, _ = r.bcastAccept(r.crtInstance, ballot, cmds, props[0].FwdReplica, props[0].FwdId)
            dlog.Printf("Fast round for instance %d\n", r.crtInstance)
            if genericsmr.SendError {
            	log.Println("BCAST ERROR")
            	r.delayedInstances <- r.crtInstance
            }
        }
        r.crtInstance++
    }
}



func (r *Replica) handleForward(fwd *paxosproto.Forward) {
    if state.IsRead(&fwd.Command) {
        if r.maintainReadStats {
            r.readStats.AddRead(fwd.Command.K, fwd.ReplicaId)
        }
        //make sure that the channel is not going to be full,
        //because this may cause the consumer to block when trying to
        //put request back
        for len(r.fwdReadsChannel) >= READ_CHAN_SIZE - 10000 {
            time.Sleep(1000)
        }
        r.fwdReadsChannel <- fwd
    } else {
        // the forward is a write
        r.handlePropose(&genericsmr.Propose{&genericsmrproto.Propose{0, fwd.Command, 0}, fwd.ReplicaId, fwd.PropId, nil, nil})
    }
}

func (r *Replica) handleForwardReply(fr *paxosproto.ForwardReply) {
    prop := r.fwdPropMap[fr.PropId]
    r.ReplyProposeTS(&genericsmrproto.ProposeReplyTS{
                        fr.OK,
                        prop.CommandId,
                        fr.Value,
                        prop.Timestamp},
                    prop)
    delete(r.fwdPropMap, fr.PropId)
}

func (r *Replica) handlePrepare(prepare *paxosproto.Prepare) {
    inst := r.instanceSpace[prepare.Instance]
    var preply *paxosproto.PrepareReply

    if inst == nil {
        ok := TRUE
        if r.defaultBallot > prepare.Ballot {
            ok = FALSE
        }
        preply = &paxosproto.PrepareReply{prepare.Instance, ok, r.defaultBallot, nil}
    } else {
        ok := TRUE
        if prepare.Ballot < inst.ballot {
            ok = FALSE
        }
        preply = &paxosproto.PrepareReply{prepare.Instance, ok, inst.ballot, inst.cmds}
    }

    r.replyPrepare(prepare.LeaderId, preply)

    if prepare.ToInfinity == TRUE && prepare.Ballot > r.defaultBallot {
        r.defaultBallot = prepare.Ballot
        //update leader id for the lease-maintaining Paxos replica
        r.leaseSMR.LeaderId = prepare.LeaderId
    }
}

func (r *Replica) handleAccept(accept *paxosproto.Accept) {
    inst := r.instanceSpace[accept.Instance]
    var areply *paxosproto.AcceptReply

    if inst == nil {
        if accept.Ballot < r.defaultBallot {
            areply = &paxosproto.AcceptReply{accept.Instance, FALSE, r.defaultBallot, -1, accept.OriginReplica, accept.PropId}
        } else {
            r.instanceSpace[accept.Instance] = &Instance{
                accept.Command,
                accept.Ballot,
                ACCEPTED,
                nil,
                1, false}
            areply = &paxosproto.AcceptReply{accept.Instance, TRUE, accept.Ballot, -1, accept.OriginReplica, accept.PropId}
            r.addUpdatingKeys(accept.Command)
        }
    } else if inst.ballot > accept.Ballot {
        areply = &paxosproto.AcceptReply{accept.Instance, FALSE, inst.ballot, -1, accept.OriginReplica, accept.PropId}
    } else if inst.ballot < accept.Ballot && inst.status != COMMITTED {
        if inst.cmds != nil {
            r.removeUpdatingKeys(inst.cmds)
        }
        r.addUpdatingKeys(accept.Command)
        inst.cmds = accept.Command
        inst.ballot = accept.Ballot
        inst.status = ACCEPTED
        inst.directAcks = 1
        areply = &paxosproto.AcceptReply{accept.Instance, TRUE, inst.ballot, -1, accept.OriginReplica, accept.PropId}
        if inst.lb != nil && inst.lb.clientProposals != nil {
            //TODO: is this correct?
            // try the proposal in a different instance
            for i := 0; i < len(inst.lb.clientProposals); i++ {
                r.ProposeChan <- inst.lb.clientProposals[i]
            }
            inst.lb.clientProposals = nil
        }
    } else {
        // reordered ACCEPT
        r.instanceSpace[accept.Instance].cmds = accept.Command
        if r.instanceSpace[accept.Instance].status < COMMITTED {
            r.instanceSpace[accept.Instance].status = ACCEPTED
        }
            r.instanceSpace[accept.Instance].ballot = accept.Ballot
            areply = &paxosproto.AcceptReply{accept.Instance, TRUE, accept.Ballot, -1, accept.OriginReplica, accept.PropId}
        /*} else {
            return
        }*/
    }

    if accept.LeaseInstance != r.QLease.PromisedByMeInst && !r.QLease.CanWriteOutside() {
        // cannot accept because lease eras do not match
        areply.OK = FALSE
        areply.LeaseInstance = r.QLease.PromisedByMeInst
        log.Println(accept.LeaseInstance, r.leaseSMR.LatestCommitted)
    }

    if areply.OK == TRUE {
        if accept.Instance > r.latestAcceptedInst {
            r.latestAcceptedInst = accept.Instance
        }

        if r.directAcks && r.Id == accept.OriginReplica {
            inst = r.instanceSpace[accept.Instance]
            inst.directAcks++
            if inst.directAcks == int8(r.N / 2 + 1) {
                //safe to commit
                prop := r.fwdPropMap[accept.PropId]
                inst.status = COMMITTED
                // give client the all clear
                if !r.Dreply && !inst.sentReply {
                    propreply := &genericsmrproto.ProposeReplyTS{
                            TRUE,
                            prop.CommandId,
                            state.NIL,
                            prop.Timestamp}
                    r.ReplyProposeTS(propreply, prop)
                    inst.sentReply = true
                } else if inst.sentReply {
                    dlog.Println("Trying to send reply twice!")
                }
                r.updateCommittedUpTo()
            }
        }

        r.recordInstanceMetadata(r.instanceSpace[accept.Instance])
        r.recordCommands(accept.Command)
        r.sync()
    }

    r.replyAccept(accept.LeaderId, areply)

    if r.directAcks &&
            accept.OriginReplica >= 0 &&
            accept.OriginReplica != accept.LeaderId &&
            accept.OriginReplica != r.Id &&
            areply.OK == TRUE {
        r.replyAccept(accept.OriginReplica, areply)
    }
}

func (r *Replica) handleCommit(commit *paxosproto.Commit) {
    inst := r.instanceSpace[commit.Instance]

    dlog.Printf("Committing instance %d\n", commit.Instance);

    if inst == nil {
        r.instanceSpace[commit.Instance] = &Instance{
            commit.Command,
            commit.Ballot,
            COMMITTED,
            nil,
            0, false}
        r.addUpdatingKeys(commit.Command)
    } else {
        r.instanceSpace[commit.Instance].cmds = commit.Command
        r.instanceSpace[commit.Instance].status = COMMITTED
        r.instanceSpace[commit.Instance].ballot = commit.Ballot
        if inst.lb != nil && inst.lb.clientProposals != nil {
            for i := 0; i < len(inst.lb.clientProposals); i++ {
                r.ProposeChan <- inst.lb.clientProposals[i]
            }
            inst.lb.clientProposals = nil
        }
    }

    if commit.Instance > r.latestAcceptedInst {
        r.latestAcceptedInst = commit.Instance
    }

    r.updateCommittedUpTo()

    r.recordInstanceMetadata(r.instanceSpace[commit.Instance])
    r.recordCommands(commit.Command)
}

func (r *Replica) handleCommitShort(commit *paxosproto.CommitShort) {
    inst := r.instanceSpace[commit.Instance]

    dlog.Printf("Committing instance %d\n", commit.Instance);

    if inst == nil {
        r.instanceSpace[commit.Instance] = &Instance{nil,
            commit.Ballot,
            COMMITTED,
            nil,
            0, false}
        r.addUpdatingKeys(r.instanceSpace[commit.Instance].cmds)
    } else {
        r.instanceSpace[commit.Instance].status = COMMITTED
        r.instanceSpace[commit.Instance].ballot = commit.Ballot
        if inst.lb != nil && inst.lb.clientProposals != nil {
            for i := 0; i < len(inst.lb.clientProposals); i++ {
                r.ProposeChan <- inst.lb.clientProposals[i]
            }
            inst.lb.clientProposals = nil
        }
    }
    if commit.Instance > r.latestAcceptedInst {
        r.latestAcceptedInst = commit.Instance
    }

    r.updateCommittedUpTo()

    r.recordInstanceMetadata(r.instanceSpace[commit.Instance])
}


func (r *Replica) handlePrepareReply(preply *paxosproto.PrepareReply) {
    inst := r.instanceSpace[preply.Instance]

    if inst.status != PREPARING {
        // TODO: should replies for non-current ballots be ignored?
        // we've moved on -- these are delayed replies, so just ignore
        return
    }

    if preply.OK == TRUE {
        inst.lb.prepareOKs++

        if preply.Ballot > inst.lb.maxRecvBallot {
            inst.cmds = preply.Command
            inst.lb.maxRecvBallot = preply.Ballot
            if inst.lb.clientProposals != nil {
                // there is already a competing command for this instance,
                // so we put the client proposal back in the queue so that
                // we know to try it in another instance
                for i := 0; i < len(inst.lb.clientProposals); i++ {
                    r.ProposeChan <- inst.lb.clientProposals[i]
                }
                inst.lb.clientProposals = nil
            }
        }

        if inst.lb.prepareOKs + 1 > r.N >> 1 {
            inst.status = PREPARED
            inst.lb.nacks = 0
            if inst.ballot > r.defaultBallot {
                r.defaultBallot = inst.ballot
            }
            r.recordInstanceMetadata(r.instanceSpace[preply.Instance])
            r.sync()
            if inst.lb.clientProposals[0].FwdReplica >= 0 && inst.lb.clientProposals[0].FwdReplica != r.Id {
                r.addUpdatingKeys(inst.cmds)
            }
            inst.lb.acceptOKsToWait, _ = r.bcastAccept(preply.Instance, inst.ballot, inst.cmds, inst.lb.clientProposals[0].FwdReplica, inst.lb.clientProposals[0].FwdId)
            if genericsmr.SendError {
            	r.delayedInstances <- preply.Instance
            }
        }
    } else {
        // TODO: there is probably another active leader
        inst.lb.nacks++
        if preply.Ballot > inst.lb.maxRecvBallot {
            inst.lb.maxRecvBallot = preply.Ballot
        }
        if inst.lb.nacks >= r.N >> 1 {
            if inst.lb.clientProposals != nil {
                // try the proposals in another instance
                for i := 0; i < len(inst.lb.clientProposals); i++ {
                    r.ProposeChan <- inst.lb.clientProposals[i]
                }
                inst.lb.clientProposals = nil
            }
        }
    }
}

func (r *Replica) handleAcceptReply(areply *paxosproto.AcceptReply) {
    inst := r.instanceSpace[areply.Instance]

    if !r.IsLeader && areply.OK == TRUE && areply.OriginReplica == r.Id {
        //direct ACK optimization
        prop := r.fwdPropMap[areply.PropId]
        if inst == nil {
            cmds := make([]state.Command, 1)
            cmds[0] = prop.Command
            r.instanceSpace[areply.Instance] =  &Instance{
                cmds,
                areply.Ballot,
                NONE,
                nil,
                1, false}
            inst = r.instanceSpace[areply.Instance]
            r.addUpdatingKeys(cmds)
        }
        if inst.ballot > areply.Ballot {
            return
        }
        if areply.Ballot > inst.ballot {
            inst.ballot = areply.Ballot
            inst.directAcks = 2
        } else {
            inst.directAcks++
        }
        if (inst.status == COMMITTED && inst.directAcks <= int8(r.N / 2 + 1)) ||
           (inst.status != COMMITTED && inst.directAcks == int8(r.N / 2 + 1)) {
            //safe to commit
            inst.status = COMMITTED
            // give the client the all clear
            inst.directAcks = int8(r.N / 2 + 2)
            if !r.Dreply && !inst.sentReply {
                propreply := &genericsmrproto.ProposeReplyTS{
                        TRUE,
                        prop.CommandId,
                        state.NIL,
                        prop.Timestamp}
                r.ReplyProposeTS(propreply, prop)
                inst.sentReply = true
            } else if inst.sentReply {
                dlog.Println("Trying to send reply twice!")
            }
            r.updateCommittedUpTo()
        }
        return
    }

    if inst.status >= COMMITTED {
        // we've moved on, these are delayed replies, so just ignore
        return
    }

    if inst.ballot != areply.Ballot {
        return
    }

    if areply.OK == TRUE {
        inst.lb.acceptOKs++
        //if inst.lb.acceptOKs + 1 > r.N >> 1 {
        if inst.lb.acceptOKs >= inst.lb.acceptOKsToWait {
            inst = r.instanceSpace[areply.Instance]
            inst.status = COMMITTED
            if inst.lb.clientProposals != nil && !r.Dreply {
                // give client the all clear
                for i := 0; i < len(inst.cmds); i++ {
                    propreply := &genericsmrproto.ProposeReplyTS{
                        TRUE,
                        inst.lb.clientProposals[i].CommandId,
                        state.NIL,
                        inst.lb.clientProposals[i].Timestamp}
                    r.ReplyProposeTS(propreply, inst.lb.clientProposals[i])
                }
            }

            r.recordInstanceMetadata(r.instanceSpace[areply.Instance])
            r.sync()//is this necessary?

            if areply.OriginReplica < 0 || areply.OriginReplica == r.Id {
                r.addUpdatingKeys(inst.cmds)
            }
            r.updateCommittedUpTo()

            r.bcastCommit(areply.Instance, inst.ballot, inst.cmds)
        }
    } else {
        if areply.LeaseInstance >= 0 {
            // acceptor is in different lease era
            // re-try later
            r.delayedInstances <- areply.Instance
        }
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

func (r *Replica) executeCommands() {
    i := int32(0)
    for !r.Shutdown {
        executed := false

        for i <= r.committedUpTo {
            if r.instanceSpace[i].cmds != nil {
                inst := r.instanceSpace[i]
                for j := 0; j < len(inst.cmds); j++ {
                    val := inst.cmds[j].Execute(r.State)
                    if r.Dreply && inst.lb != nil && inst.lb.clientProposals != nil {
                        propreply := &genericsmrproto.ProposeReplyTS{
                            TRUE,
                            inst.lb.clientProposals[j].CommandId,
                            val,
                            inst.lb.clientProposals[j].Timestamp}
                        r.ReplyProposeTS(propreply, inst.lb.clientProposals[j])
                    }
                }

                r.removeUpdatingKeys(inst.cmds)

                i++
                executed = true
            } else {
                break
            }
        }

        if !executed {
            time.Sleep(1000 * 1000)
        }
    }
}

func (r *Replica) reader() {
    for !r.Shutdown {
        select {
        case prop := <-r.readsChannel:
            for r.committedUpTo < r.newestInstanceIDontKnow {
                time.Sleep(1000 * 1000)
            }
            for !r.isMyLeaseActive() {
                time.Sleep(1000 * 1000)
            }
            r.updatingLock.Lock()
            if r.isKeyUpdating(prop.Command.K) /*|| !r.isMyLeaseActive()*/ {
                r.updatingLock.Unlock()
                r.readsChannel <- prop
                if len(r.readsChannel) <= 1 {
                    time.Sleep(1000 * 1000)
                }
                /*r.ReplyProposeTS(
                    &genericsmrproto.ProposeReplyTS{
                        FALSE,
                        prop.CommandId,
                        0,
                        prop.Timestamp},
                    prop)*/
                break
            }
            val := prop.Command.Execute(r.State)
            r.updatingLock.Unlock()
            if r.isKeyGranted(prop.Command.K) && r.isMyLeaseActive() {
                local++
                r.ReplyProposeTS(
                    &genericsmrproto.ProposeReplyTS{
                        TRUE,
                        prop.CommandId,
                        1000772,
                        prop.Timestamp},
                    prop)
            } else {
                //r.ProposeChan <- prop
                r.ReplyProposeTS(
                    &genericsmrproto.ProposeReplyTS{
                        FALSE,
                        prop.CommandId,
                        val,
                        prop.Timestamp},
                    prop)
            }
            break
        case fwd := <-r.fwdReadsChannel:
            for r.committedUpTo < r.newestInstanceIDontKnow {
                time.Sleep(1000 * 1000)
            }
            for !r.isMyLeaseActive() {
                time.Sleep(1000 * 1000)
            }
            r.updatingLock.Lock()
            if r.isKeyUpdating(fwd.Command.K) /*|| !r.isMyLeaseActive()*/ {
                r.updatingLock.Unlock()
                r.fwdReadsChannel <- fwd
                if len(r.fwdReadsChannel) <= 1 {
                    time.Sleep(1000 * 1000)
                }
                //r.SendMsg(fwd.ReplicaId, r.forwardReplyRPC, &paxosproto.ForwardReply{fwd.PropId, FALSE, 0})
                break
            }
            val := fwd.Command.Execute(r.State)
            r.updatingLock.Unlock()
            if r.isKeyGranted(fwd.Command.K) && r.isMyLeaseActive() {
                local++
                r.SendMsg(fwd.ReplicaId, r.forwardReplyRPC, &paxosproto.ForwardReply{fwd.PropId, TRUE, val})
            } else {
                r.SendMsg(fwd.ReplicaId, r.forwardReplyRPC, &paxosproto.ForwardReply{fwd.PropId, FALSE, val})
            }
        }
    }
}






