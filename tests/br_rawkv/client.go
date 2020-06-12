package main

import (
	"bytes"
	"encoding/hex"
	"flag"
	"fmt"
	"hash/crc64"
	"math/rand"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/prometheus/common/log"
)

var (
	pdAddr      = flag.String("pd", "127.0.0.1:2379", "Address of PD")
	runMode     = flag.String("mode", "", "Mode. One of 'rand-gen', 'checksum', 'scan', 'diff', 'delete' and 'put'")
	startKeyStr = flag.String("start-key", "", "Start key in hex")
	endKeyStr   = flag.String("end-key", "", "End key in hex")
	keyMaxLen   = flag.Int("key-max-len", 32, "Max length of keys for rand-gen mode")
	concurrency = flag.Int("concurrency", 32, "Concurrency to run rand-gen")
	duration    = flag.Int("duration", 10, "duration(second) of rand-gen")
	putDataStr  = flag.String("put-data", "", "Kv pairs to put to the cluster in hex. "+
		"kv pairs are separated by commas, key and value in a pair are separated by a colon")
)

func createClient(addr string) (*tikv.RawKVClient, error) {
	cli, err := tikv.NewRawKVClient([]string{addr}, config.Security{})
	return cli, err
}

func main() {
	flag.Parse()

	startKey, err := hex.DecodeString(*startKeyStr)
	if err != nil {
		log.Fatalf("Invalid startKey: %v, err: %+v", startKeyStr, err)
	}
	endKey, err := hex.DecodeString(*endKeyStr)
	if err != nil {
		log.Fatalf("Invalid endKey: %v, err: %+v", endKeyStr, err)
	}
	// For "put" mode, the key range is not used. So no need to throw error here.
	if len(endKey) == 0 && *runMode != "put" {
		log.Fatal("Empty endKey is not supported yet")
	}

	if *runMode == "test-rand-key" {
		testRandKey(startKey, endKey, *keyMaxLen)
		return
	}

	client, err := createClient(*pdAddr)
	if err != nil {
		log.Fatalf("Failed to create client to %v, err: %+v", *pdAddr, err)
	}

	switch *runMode {
	case "rand-gen":
		err = randGenWithDuration(client, startKey, endKey, *keyMaxLen, *concurrency, *duration)
	case "checksum":
		err = checksum(client, startKey, endKey)
	case "scan":
		err = scan(client, startKey, endKey)
	case "delete":
		err = deleteRange(client, startKey, endKey)
	case "put":
		err = put(client, *putDataStr)
	}

	if err != nil {
		log.Fatalf("Error: %+v", err)
	}
}

func randGenWithDuration(client *tikv.RawKVClient, startKey, endKey []byte,
	maxLen int, concurrency int, duration int) error {
	var err error
	ok := make(chan struct{})
	go func() {
		err = randGen(client, startKey, endKey, maxLen, concurrency)
		ok <- struct{}{}
	}()
	select {
	case <-time.After(time.Second * time.Duration(duration)):
	case <-ok:
	}
	return err
}

func randGen(client *tikv.RawKVClient, startKey, endKey []byte, maxLen int, concurrency int) error {
	log.Infof("Start rand-gen from %v to %v, maxLen %v", hex.EncodeToString(startKey), hex.EncodeToString(endKey), maxLen)
	log.Infof("Rand-gen will keep running. Please Ctrl+C to stop manually.")

	// Cannot generate shorter key than commonPrefix
	commonPrefixLen := 0
	for ; commonPrefixLen < len(startKey) && commonPrefixLen < len(endKey) &&
		startKey[commonPrefixLen] == endKey[commonPrefixLen]; commonPrefixLen++ {
		continue
	}

	if maxLen < commonPrefixLen {
		return errors.Errorf("maxLen (%v) < commonPrefixLen (%v)", maxLen, commonPrefixLen)
	}

	const batchSize = 32

	errCh := make(chan error, concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			for {
				keys := make([][]byte, 0, batchSize)
				values := make([][]byte, 0, batchSize)

				for i := 0; i < batchSize; i++ {
					key := randKey(startKey, endKey, maxLen)
					keys = append(keys, key)
					value := randValue()
					values = append(values, value)
				}

				err := client.BatchPut(keys, values)
				if err != nil {
					errCh <- errors.Trace(err)
				}
			}
		}()
	}

	err := <-errCh
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func testRandKey(startKey, endKey []byte, maxLen int) {
	for {
		k := randKey(startKey, endKey, maxLen)
		if bytes.Compare(k, startKey) < 0 || bytes.Compare(k, endKey) >= 0 {
			panic(hex.EncodeToString(k))
		}
	}
}

func randKey(startKey, endKey []byte, maxLen int) []byte {
Retry:
	for { // Regenerate on fail
		result := make([]byte, 0, maxLen)

		upperUnbounded := false
		lowerUnbounded := false

		for i := 0; i < maxLen; i++ {
			upperBound := 256
			if !upperUnbounded {
				if i >= len(endKey) {
					// The generated key is the same as endKey which is invalid. Regenerate it.
					continue Retry
				}
				upperBound = int(endKey[i]) + 1
			}

			lowerBound := 0
			if !lowerUnbounded {
				if i >= len(startKey) {
					lowerUnbounded = true
				} else {
					lowerBound = int(startKey[i])
				}
			}

			if lowerUnbounded {
				if rand.Intn(257) == 0 {
					return result
				}
			}

			value := rand.Intn(upperBound - lowerBound)
			value += lowerBound

			if value < upperBound-1 {
				upperUnbounded = true
			}
			if value > lowerBound {
				lowerUnbounded = true
			}

			result = append(result, uint8(value))
		}

		return result
	}
}

func randValue() []byte {
	result := make([]byte, 0, 512)
	for i := 0; i < 512; i++ {
		value := rand.Intn(257)
		if value == 256 {
			if i > 0 {
				return result
			}
			value--
		}
		result = append(result, uint8(value))
	}
	return result
}

func checksum(client *tikv.RawKVClient, startKey, endKey []byte) error {
	log.Infof("Start checkcum on range %v to %v", hex.EncodeToString(startKey), hex.EncodeToString(endKey))

	scanner := newRawKVScanner(client, startKey, endKey)
	digest := crc64.New(crc64.MakeTable(crc64.ECMA))

	var res uint64

	for {
		k, v, err := scanner.Next()
		if err != nil {
			return errors.Trace(err)
		}
		if len(k) == 0 {
			break
		}
		_, _ = digest.Write(k)
		_, _ = digest.Write(v)
		res ^= digest.Sum64()
	}

	log.Infof("Checksum result: %016x", res)
	fmt.Printf("Checksum result: %016x\n", res)
	return nil
}

func deleteRange(client *tikv.RawKVClient, startKey, endKey []byte) error {
	log.Infof("Start delete data in range %v to %v", hex.EncodeToString(startKey), hex.EncodeToString(endKey))
	return client.DeleteRange(startKey, endKey)
}

func scan(client *tikv.RawKVClient, startKey, endKey []byte) error {
	log.Infof("Start scanning data in range %v to %v", hex.EncodeToString(startKey), hex.EncodeToString(endKey))

	scanner := newRawKVScanner(client, startKey, endKey)

	var key []byte
	for {
		k, v, err := scanner.Next()
		if err != nil {
			return errors.Trace(err)
		}
		if len(k) == 0 {
			break
		}
		fmt.Printf("key: %v, value: %v\n", hex.EncodeToString(k), hex.EncodeToString(v))
		if bytes.Compare(key, k) >= 0 {
			log.Errorf("Scan result is not in order. "+
				"Previous key: %v, Current key: %v",
				hex.EncodeToString(key), hex.EncodeToString(k))
		}
	}

	log.Infof("Finished Scanning.")
	return nil
}

func put(client *tikv.RawKVClient, dataStr string) error {
	keys := make([][]byte, 0)
	values := make([][]byte, 0)

	for _, pairStr := range strings.Split(dataStr, ",") {
		pair := strings.Split(pairStr, ":")
		if len(pair) != 2 {
			return errors.Errorf("invalid kv pair string %q", pairStr)
		}

		key, err := hex.DecodeString(strings.Trim(pair[0], " "))
		if err != nil {
			return errors.Annotatef(err, "invalid kv pair string %q", pairStr)
		}
		value, err := hex.DecodeString(strings.Trim(pair[1], " "))
		if err != nil {
			return errors.Annotatef(err, "invalid kv pair string %q", pairStr)
		}

		keys = append(keys, key)
		values = append(values, value)
	}

	log.Infof("Put rawkv data, keys: %q, values: %q", keys, values)

	err := client.BatchPut(keys, values)
	return errors.Trace(err)
}

const defaultScanBatchSize = 128

type rawKVScanner struct {
	client    *tikv.RawKVClient
	batchSize int

	currentKey []byte
	endKey     []byte

	bufferKeys   [][]byte
	bufferValues [][]byte
	bufferCursor int
	noMore       bool
}

func newRawKVScanner(client *tikv.RawKVClient, startKey, endKey []byte) *rawKVScanner {
	return &rawKVScanner{
		client:    client,
		batchSize: defaultScanBatchSize,

		currentKey: startKey,
		endKey:     endKey,

		noMore: false,
	}
}

func (s *rawKVScanner) Next() ([]byte, []byte, error) {
	if s.bufferCursor >= len(s.bufferKeys) {
		if s.noMore {
			return nil, nil, nil
		}

		s.bufferCursor = 0

		batchSize := s.batchSize
		var err error
		s.bufferKeys, s.bufferValues, err = s.client.Scan(s.currentKey, s.endKey, batchSize)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}

		if len(s.bufferKeys) < batchSize {
			s.noMore = true
		}

		if len(s.bufferKeys) == 0 {
			return nil, nil, nil
		}

		bufferKey := s.bufferKeys[len(s.bufferKeys)-1]
		bufferKey = append(bufferKey, 0)
		s.currentKey = bufferKey
	}

	key := s.bufferKeys[s.bufferCursor]
	value := s.bufferValues[s.bufferCursor]
	s.bufferCursor++
	return key, value, nil
}
