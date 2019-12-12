package utils

import "bytes"

// CompareEndKey compared two keys that BOTH represent the EXCLUSIVE ending of some range. An empty end key is the very
// end, so an empty key is greater than any other keys.
// Please note that this function is not applicable if any one argument is not an EXCLUSIVE ending of a range.
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
