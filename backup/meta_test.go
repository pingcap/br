package backup

import (
	"math/rand"
	"testing"
	"time"
)

func TestTimestampEncodeDecode(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < 10; i++ {
		ts := rand.Uint64()
		tp := decodeTs(ts)
		ts1 := encodeTs(tp)
		if ts != ts1 {
			t.Fatalf("%d != %d", ts, ts1)
		}
	}
}
