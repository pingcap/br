package utils

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"io"
	"strings"

	"github.com/pingcap/errors"
	"github.com/spf13/pflag"
)

// ParseKey parse key by given format
func ParseKey(flags *pflag.FlagSet, key string) ([]byte, error) {
	switch flags.Lookup("format").Value.String() {
	case "raw":
		return []byte(key), nil
	case "escaped":
		return unescapedKey(key)
	case "hex":
		key, err := hex.DecodeString(key)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return key, nil
	}
	return nil, errors.New("unknown format")
}

func unescapedKey(text string) ([]byte, error) {
	var buf []byte
	r := bytes.NewBuffer([]byte(text))
	for {
		c, err := r.ReadByte()
		if err != nil {
			if err != io.EOF {
				return nil, errors.WithStack(err)
			}
			break
		}
		if c != '\\' {
			buf = append(buf, c)
			continue
		}
		n := r.Next(1)
		if len(n) == 0 {
			return nil, io.EOF
		}
		// See: https://golang.org/ref/spec#Rune_literals
		if idx := strings.IndexByte(`abfnrtv\'"`, n[0]); idx != -1 {
			buf = append(buf, []byte("\a\b\f\n\r\t\v\\'\"")[idx])
			continue
		}

		switch n[0] {
		case 'x':
			fmt.Sscanf(string(r.Next(2)), "%02x", &c)
			buf = append(buf, c)
		default:
			n = append(n, r.Next(2)...)
			_, err := fmt.Sscanf(string(n), "%03o", &c)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			buf = append(buf, c)
		}
	}
	return buf, nil
}
