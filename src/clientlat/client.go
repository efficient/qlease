package main

import (
    "log"
//	"dlog"
    "net"
	"net/rpc"
	"flag"
	"fmt"
    "genericsmrproto"
    "state"
    "runtime"
    "masterproto"
    "math/rand"
    "time"
    "bufio"
    "ycsbzipf"
    "randperm"
)

var masterAddr *string = flag.String("maddr", "", "Master address. Defaults to localhost")
var masterPort *int = flag.Int("mport", 7077, "Master port.  Defaults to 7077.")
var writes *int = flag.Int("w", 100, "Percentage of updates (writes). Defaults to 100%.")   
var reqsNb *int = flag.Int("q", 5000, "Total number of requests. Defaults to 5000.")
var noLeader *bool = flag.Bool("e", false, "Egalitarian (no leader). Defaults to false.")
var procs *int = flag.Int("p", 2, "GOMAXPROCS. Defaults to 2")
var check = flag.Bool("check", false, "Check that every expected reply was received exactly once.")
var conflicts *int = flag.Int("c", -1, "Percentage of conflicts. Defaults to 0%")
var s = flag.Float64("s", 2, "Zipfian s parameter")
var v = flag.Float64("v", 1, "Zipfian v parameter")
var barOne = flag.Bool("barOne", false, "Sent commands to all replicas except the last one.")
var forceLeader = flag.Int("l", -1, "Force client to talk to a certain replica.")
var readFrom = flag.Int("readFrom", -1, "Send reads to a specified replica instead of the leader.")
var startRange = flag.Int("sr", 0, "Key range start")
var sleep = flag.Int("sleep", 0, "Sleep")
var T = flag.Int("T", 1, "Number of threads (simulated clients).")
var D = flag.Int("D", -1, "Key range - defaults to the number of operations.")

var rarray []int
var rsp []bool

var karray []int64

func main() {
    flag.Parse()

    runtime.GOMAXPROCS(*procs)

    if *conflicts > 100 {
        log.Fatalf("Conflicts percentage must be between 0 and 100.\n")
    }

    if *D < *reqsNb {
        *D = *reqsNb
    }

    master, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", *masterAddr, *masterPort))
    if err != nil {
        log.Fatalf("Error connecting to master\n")
    }

    rlReply := new(masterproto.GetReplicaListReply)
    err = master.Call("Master.GetReplicaList", new(masterproto.GetReplicaListArgs), rlReply)
    if err != nil {
        log.Fatalf("Error making the GetReplicaList RPC")
    }

    leader := 0
    if *noLeader == false && *forceLeader < 0 {
        reply := new(masterproto.GetLeaderReply)
        if err = master.Call("Master.GetLeader", new(masterproto.GetLeaderArgs), reply); err != nil {
            log.Fatalf("Error making the GetLeader RPC\n")
        }
        leader = reply.LeaderId
        //log.Printf("The leader is replica %d\n", leader)
    } else if *forceLeader > 0 {
        leader = *forceLeader
    }

    done := make(chan bool, *T)

    readsChan := make(chan float64, 2 * *reqsNb)
    writesChan := make(chan float64, 2 * *reqsNb)


    //prepare keys
    karray = make([]int64, *D)
    for i := 0; i < len(karray); i++ {
        karray[i] = int64(*startRange + i)
    }

    randObj := rand.New(rand.NewSource(int64(42 + *forceLeader + *readFrom)))
    randperm.Permute(karray, randObj)

    go printer(readsChan, writesChan, done)

    for i := 0; i < *T; i++ {
        go simulatedClient(rlReply, leader, readsChan, writesChan, done, i)
    }

    log.Println("Waiting")
    for i := 0; i < *T + 1; i++ {
        <-done
    }
    master.Close()
}

func simulatedClient(rlReply *masterproto.GetReplicaListReply, leaderId int, readsChan chan float64, writesChan chan float64, done chan bool, idx int) {
    N := len(rlReply.ReplicaList)
    servers := make([]net.Conn, N)
    readers := make([]*bufio.Reader, N)
    writers := make([]*bufio.Writer, N)

    rarray := make([]int, *reqsNb)
    iarray := make([]int, *reqsNb)
    put := make([]bool, *reqsNb)

    perReplicaCount := make([]int, N)
    M := N
    if *barOne {
        M = N - 1
    }
    randObj := rand.New(rand.NewSource(int64(42 + idx)))
    zipf := ycsbzipf.NewZipf(int(*D), randObj)
    for i := 0; i < len(rarray); i++ {
        r := rand.Intn(M)

        rarray[i] = r
        perReplicaCount[r]++

        if *conflicts >= 0 {
            r = rand.Intn(100)
            if r < *conflicts {
                iarray[i] = 0
            } else {
                iarray[i] = i
            }
        } else {
            iarray[i] = int(zipf.NextInt64())
        }
        //r = rand.Intn(100)
        r = randObj.Intn(100)
        if r < *writes {
            put[i] = true
        } else {
            put[i] = false
        }
    }

    for i := 0; i < N; i++ {
        var err error
        servers[i], err = net.Dial("tcp", rlReply.ReplicaList[i])
        if err != nil {
            log.Printf("Error connecting to replica %d\n", i)
        }
        readers[i] = bufio.NewReader(servers[i])
        writers[i] = bufio.NewWriter(servers[i])
    }

    var id int32 = 0
    args := genericsmrproto.Propose{id, state.Command{state.PUT, 0, 0}, 0}
    var reply genericsmrproto.ProposeReplyTS

    n := *reqsNb

    successful := 0
    for i := 0; i < n; i++ {
        leader := leaderId
        if *noLeader {
            leader = rarray[i]
        }
        args.CommandId = id
        if put[i] {
            args.Command.Op = state.PUT
        } else {
            args.Command.Op = state.GET
            if *readFrom > 0 {
                leader = *readFrom
            }
        }
        args.Command.K = state.Key(karray[iarray[i]])
        writers[leader].WriteByte(genericsmrproto.PROPOSE)

        before := time.Now()
        //log.Println(i, karray[i], iarray[i], karray[iarray[i]])

        args.Marshal(writers[leader])
        writers[leader].Flush()
        if err := reply.Unmarshal(readers[leader]); err != nil {
            fmt.Println("Error when reading:", err)
            continue
        }

        if reply.OK != 0 {
            successful++
        }

        after := time.Now()

        id++

        if put[i] {
            writesChan <- (after.Sub(before)).Seconds() * 1000
        } else {
            readsChan <- (after.Sub(before)).Seconds() * 1000
        }

        if *sleep > 0 {
            time.Sleep(100 * 1000 * 1000)
        }
    }

    for _, client := range servers {
        if client != nil {
            client.Close()
        }
    }
    log.Println("Successful:", successful)
    done <- true
}

func printer(reads chan float64, writes chan float64, done chan bool) {
    n := *T * *reqsNb
    for i := 0; i < n; i++ {
        select {
        case lat := <-reads:
            fmt.Printf("r %v\n", lat)
        case lat := <-writes:
            fmt.Printf("w %v\n", lat)
        }
    }
    done <- true
}




