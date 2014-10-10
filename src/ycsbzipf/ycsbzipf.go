package ycsbzipf

import (
    "math"
    "math/rand"
    )

const THETA float64 = 0.99

type Zipf struct {
    r *rand.Rand
    count int
    items float64
    alpha float64
    zeta2 float64
    zetan float64
    eta float64
}

func NewZipf(count int, r *rand.Rand) *Zipf {
    z := new(Zipf)

    z.r = r
    z.count = count
    z.items = float64(count)
    z.zeta2 = computeZeta(2)
    z.alpha = 1.0 / (1.0 - THETA)
    z.zetan = computeZeta(count)
    z.eta = (1 - math.Pow(2.0 / z.items, 1 - THETA)) / (1 - z.zeta2 / z.zetan)

    return z
}

func computeZeta(count int) float64 {
    var sum float64 = 0
    for i := 0; i < count; i++ {
        sum += 1 / math.Pow(float64(i + 1), THETA)
    }
    return sum
}

func (z *Zipf) NextInt64() int64 {
    u := z.r.Float64()
    uz := u * z.zetan

    if uz < 1.0 {
        return 0
    }

    if uz < 1.0 + math.Pow(0.5, THETA) {
        return 1
    }

    return int64(z.items * math.Pow(z.eta * u - z.eta + 1, z.alpha))
}



