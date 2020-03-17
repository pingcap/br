// Copyright 2020 PingCAP, Inc. Licensed under Apache-2.0.

package utils

import (
	"bytes"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/pd/v4/server/schedule/placement"
	"github.com/pingcap/tidb/tablecodec"
	"github.com/tikv/client-go/codec"
)

const (
	resetTSURL       = "/pd/api/v1/admin/reset-ts"
	placementRuleURL = "/pd/api/v1/config/rules"
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
	defer resp.Body.Close()
	if resp.StatusCode != 200 && resp.StatusCode != 403 {
		buf := new(bytes.Buffer)
		_, _ = buf.ReadFrom(resp.Body)
		return errors.Errorf("pd resets TS failed: req=%v, resp=%v, err=%v", string(req), buf.String(), err)
	}
	return nil
}

// GetPlacementRules return the current placement rules
func GetPlacementRules(pdAddr string, tlsConf *tls.Config) ([]placement.Rule, error) {
	cli := &http.Client{Timeout: 30 * time.Second}
	prefix := "http://"
	if tlsConf != nil {
		prefix = "https://"
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.TLSClientConfig = tlsConf
		cli.Transport = transport
	}
	reqURL := prefix + pdAddr + placementRuleURL
	resp, err := cli.Get(reqURL)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer resp.Body.Close()
	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(resp.Body)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if resp.StatusCode != 200 {
		return nil, errors.Errorf("get placement rules failed: resp=%v, err=%v", buf.String(), err)
	}
	var rules []placement.Rule
	err = json.Unmarshal(buf.Bytes(), &rules)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return rules, nil
}

// SearchPlacementRule returns the placement rule matched to the table or nil
func SearchPlacementRule(tableID int64, placementRules []placement.Rule, role placement.PeerRoleType) *placement.Rule {
	for _, rule := range placementRules {
		key, err := hex.DecodeString(rule.StartKeyHex)
		if err != nil {
			continue
		}
		_, decoded, err := codec.DecodeBytes(key)
		if err != nil {
			continue
		}
		if rule.Role == role && tableID == tablecodec.DecodeTableID(decoded) {
			return &rule
		}
	}
	return nil
}
