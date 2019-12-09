package utils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/store/tikv/oracle"
)

const (
	resetTSURL = "/pd/api/v1/admin/reset-ts"
)

// Timestamp is composed by a physical unix timestamp and a logical timestamp.
type Timestamp struct {
	Physical int64
	Logical  int64
}

const physicalShiftBits = 18

// DecodeTs decodes Timestamp from a uint64
func DecodeTs(ts uint64) Timestamp {
	physical := oracle.ExtractPhysical(ts)
	logical := ts - (uint64(physical) << physicalShiftBits)
	return Timestamp{
		Physical: physical,
		Logical:  int64(logical),
	}
}

// EncodeTs encodes Timestamp into a uint64
func EncodeTs(tp Timestamp) uint64 {
	return uint64((tp.Physical << physicalShiftBits) + tp.Logical)
}

// ResetTS resets the timestamp of PD to a bigger value
func ResetTS(pdAddr string, ts uint64) error {
	req, err := json.Marshal(struct {
		TSO string `json:"tso,omitempty"`
	}{TSO: fmt.Sprintf("%d", ts)})
	if err != nil {
		return err
	}
	// TODO: Support TLS
	reqURL := "http://" + pdAddr + resetTSURL
	resp, err := http.Post(reqURL, "application/json", strings.NewReader(string(req)))
	if err != nil {
		return errors.Trace(err)
	}
	if resp.StatusCode != 200 && resp.StatusCode != 403 {
		buf := new(bytes.Buffer)
		_, err := buf.ReadFrom(resp.Body)
		return errors.Errorf("pd resets TS failed: req=%v, resp=%v, err=%v", string(req), buf.String(), err)
	}
	return nil
}
