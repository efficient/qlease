package cheaptime

import (
    "time"
    "rdtsc"
)

var baseNano int64 = 0
var baseTsc uint64
var scale float64


func Now() int64 {
    if baseNano == 0 {
        baseNano = time.Now().UnixNano()
        baseTsc = rdtsc.Cputicks()
        time.Sleep(1e7)
        rn := time.Now().UnixNano()
        ct := rdtsc.Cputicks()
        scale = float64(rn - baseNano) /  float64(ct - baseTsc)
    }
    return baseNano + int64(float64(rdtsc.Cputicks() - baseTsc) * scale)
}

