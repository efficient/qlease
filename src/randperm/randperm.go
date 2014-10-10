package randperm

import (
	"math/rand"
)

func Permute(v []int64, r *rand.Rand) {
	n := int32(len(v))

	for n > 0 {
		i := r.Int31n(n)
		aux := v[n - 1]
		v[n - 1] = v[i]
		v[i] = aux
		n--
	}
}