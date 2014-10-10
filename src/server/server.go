package main

import (
	"log"
	"net/rpc"
	"net/http"
	"net"
	"flag"
	"fmt"
    "paxos"
    "lpaxos"
    "time"
    "masterproto"
    "runtime"
    "os"
    "os/signal"
    "runtime/pprof"
)

var portnum *int = flag.Int("port", 7070, "Port # to listen on. Defaults to 7070")
var leaseport *int = flag.Int("lport", 7060, "Lease port # to listen on. Defaults to 7030") 
var masterAddr *string = flag.String("maddr", "", "Master address. Defaults to localhost.")
var masterPort *int = flag.Int("mport", 7077, "Master port.  Defaults to 7087.")
var myAddr *string = flag.String("addr", "", "Server address (this machine). Defaults to localhost.")
var procs *int = flag.Int("p", 2, "GOMAXPROCS. Defaults to 2")
var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var thrifty = flag.Bool("thrifty", false, "Use only as many messages as strictly required for inter-replica communication.")
var exec = flag.Bool("exec", false, "Execute commands.")
var dreply = flag.Bool("dreply", false, "Reply to client only after command has been executed.")
var beacon = flag.Bool("beacon", false, "Send beacons to other replicas to compare their relative speeds.")
var durable = flag.Bool("durable", false, "Log to a stable store (i.e., a file in the current dir).")
var directAcks = flag.Bool("directAcks", false, "Send Accept Replies directly to the originating replica, not only the leader.")


func main() {
	flag.Parse()

    runtime.GOMAXPROCS(*procs)

    if *cpuprofile != "" {
        f, err := os.Create(*cpuprofile)
        if err != nil {
            log.Fatal(err)
        }
        pprof.StartCPUProfile(f)

        interrupt := make(chan os.Signal, 1)
        signal.Notify(interrupt)
        go catchKill(interrupt)
    }

	log.Printf("Server starting on port %d\n", *portnum)

    replicaId, nodeList, leaseNodeList := registerWithMaster(fmt.Sprintf("%s:%d", *masterAddr, *masterPort))
    log.Println("Lease nodes:")
    log.Println(leaseNodeList)
    log.Println(nodeList)
    log.Println(replicaId)


    // we first start a Lease-Paxos replica -- we use Lease-Paxos to maintain consensus on lease info
    log.Println("Starting Lease-Paxos replica...")
    leaseRep := lpaxos.NewReplica(replicaId, leaseNodeList, *thrifty, *exec, *dreply, *durable)

    log.Println("Starting classic Paxos replica...")
    rep := paxos.NewReplica(replicaId, nodeList, *thrifty, *exec, *dreply, *durable, *beacon, leaseRep, *directAcks)
    rpc.Register(rep)

    rpc.HandleHTTP()
    //listen for RPC on a different port (8070 by default)
    l, err := net.Listen("tcp", fmt.Sprintf(":%d", *portnum + 1000))
    if err != nil {
        log.Fatal("listen error:", err)
    }

    http.Serve(l, nil)
}

func registerWithMaster(masterAddr string) (int, []string, []string) {
    args := &masterproto.RegisterArgs{*myAddr, *portnum, *leaseport}
    var reply masterproto.RegisterReply

    for done := false; !done; {
        mcli, err := rpc.DialHTTP("tcp", masterAddr)
        if err == nil {
            err = mcli.Call("Master.Register", args, &reply)
            if err == nil && reply.Ready == true {
                done = true
                break
            }
        }
        time.Sleep(1e9)
    }

    return reply.ReplicaId, reply.NodeList, reply.LeaseNodeList
}

func catchKill(interrupt chan os.Signal) {
    <-interrupt
    if *cpuprofile != "" {
        pprof.StopCPUProfile()
    }
    fmt.Println("Caught signal")
    os.Exit(0)
}

