// +build windows

package storage

import (
	"os"
)

func mkdirAll(base string) error {
	return os.MkdirAll(base, 0755)
}
