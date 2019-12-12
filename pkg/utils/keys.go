package utils

import "bytes"

// CompareEndKey compared two keys that BOTH represent the exclusive end of some range. An empty end key is the very
// end, so an empty key is greater than any other keys.
func CompareEndKey(a, b []byte) int {
	if len(a) == 0 {
		if len(b) == 0 {
			return 0
		}
		return 1
	}

	if len(b) == 0 {
		return -1
	}

	return bytes.Compare(a, b)
}
