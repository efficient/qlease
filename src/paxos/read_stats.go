package paxos

import (
	"state"
	"qleaseproto"
)

type ReadStats struct {
	N int
	leaderId int32
	freqMap map[state.Key][]int
	prevMap map[state.Key][]int
}

func NewReadStats(N int, leaderId int32) *ReadStats {
	return &ReadStats{
		N,
		leaderId,
		make(map[state.Key][]int, 1000),
		make(map[state.Key][]int, 1000)}
}

func (rs *ReadStats) AddRead(key state.Key, replicaId int32) {
	s, present := rs.freqMap[key]
	if !present {
		s = make([]int, rs.N)
		rs.freqMap[key] = s
	}
	s[replicaId]++
}

func (rs *ReadStats) findMax2Indices(slice []int) (int32, int32) {
	max1 := int32(rs.leaderId)
	max2 := int32(rs.leaderId)
	for i, v := range slice {
		if v > slice[max1] {
			max2 = max1
			max1 = int32(i)
		} else if v > slice[max2] {
			max2 = int32(i)
		}
	}
	return max1, max2
}

func (rs *ReadStats) GetQuorums() []qleaseproto.LeaseMetadata {
	lm := make(map[int64][]state.Key)
	for k, v := range rs.freqMap {
		r1, r2 := rs.findMax2Indices(v)
		if r1 > r2 {
			aux := r1
			r1 = r2
			r2 = aux
		}
		for r1 == rs.leaderId || r1 == r2 {
			r1++
			if r1 == int32(rs.N) {
				r1 = 0
			}
		}

		found := false
		if pq, present := rs.prevMap[k]; present {
		    for pf := range pq {
		    	if pf > v[r1] && pf > v[r2] {
		    		found = true
		    		break
		    	}
		    }
		}
	    if found {
	    	continue
	    }
		rs.prevMap[k] = v

		key64 := (int64(r1) << 32) | int64(r2)
		s, present := lm[key64]
		if !present {
			s = make([]state.Key, 0, 1000)
		}
		if len(s) == cap(s) {
			ns := make([]state.Key, len(s), 2 * (cap(s) + 1 ))
			copy(ns, s)
			s = ns
		}
		s = s[0 : len(s) + 1]
		s[len(s) - 1] = k
		lm[key64] = s
	}
	ret := make([]qleaseproto.LeaseMetadata, len(lm))
	i := 0
	for q64, keys := range lm {
		ret[i].ObjectKeys = keys
		quorum := make([]int32, rs.N / 2 + 1)
		quorum[0] = rs.leaderId
		quorum[1] = int32(q64 >> 32)
		if rs.N > 3 {
			quorum[2] = int32(q64 & 0xFFFFFFFF)
		}
		ret[i].Quorum = quorum
		ret[i].IgnoreReplicas, ret[i].ReinstateReplicas = 0, 0
		i++
	}

	rs.freqMap = make(map[state.Key][]int, 1000)

	return ret
}
