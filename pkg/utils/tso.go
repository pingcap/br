package utils

import (
	"bytes"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/pingcap/errors"
)

const (
	resetTSURL = "/pd/api/v1/admin/reset-ts"
)

// ResetTS resets the timestamp of PD to a bigger value
func ResetTS(pdAddr string, ts uint64, tlsConf *tls.Config) error {
	req, err := json.Marshal(struct {
		TSO string `json:"tso,omitempty"`
	}{TSO: fmt.Sprintf("%d", ts)})
	if err != nil {
		return err
	}
	cli := &http.Client{Timeout: 30 * time.Second}
	prefix := "http://"
	if tlsConf != nil {
		prefix = "https://"
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.TLSClientConfig = tlsConf
		cli.Transport = transport
	}
	reqURL := prefix + pdAddr + resetTSURL
	resp, err := cli.Post(reqURL, "application/json", strings.NewReader(string(req)))
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
