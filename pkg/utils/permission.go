package utils

import "strings"

var (
	NotFoundMsg         = "notfound"
	PermissionDeniedMsg = "permissiondenied"
)

func MessageIsNotFoundStorageError(msg string) bool {
	msgLower := strings.ToLower(msg)
	return strings.Contains(msgLower, NotFoundMsg)
}

func MessageIsPermissionDeniedStorageError(msg string) bool {
	msgLower := strings.ToLower(msg)
	return strings.Contains(msgLower, PermissionDeniedMsg)
}
