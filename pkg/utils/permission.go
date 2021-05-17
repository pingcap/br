package utils

import "strings"

var (
	notFoundMsg         = "notfound"
	permissionDeniedMsg = "permissiondenied"
)

// MessageIsNotFoundStorageError checks whether the message returning from TiKV is "NotFound" storage I/O error
func MessageIsNotFoundStorageError(msg string) bool {
	msgLower := strings.ToLower(msg)
	return strings.Contains(msgLower, notFoundMsg)
}

// MessageIsPermissionDeniedStorageError checks whether the message returning from TiKV is "PermissionDenied" storage I/O error
func MessageIsPermissionDeniedStorageError(msg string) bool {
	msgLower := strings.ToLower(msg)
	return strings.Contains(msgLower, permissionDeniedMsg)
}
