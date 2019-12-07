package storage

type noopStorage struct{}

// Write file to storage
func (*noopStorage) Write(name string, data []byte) error {
	return nil
}

// Read storage file
func (*noopStorage) Read(name string) ([]byte, error) {
	return []byte{}, nil
}

// FileExists return true if file exists
func (*noopStorage) FileExists(name string) bool {
	return false
}

func newNoopStorage() *noopStorage {
	return &noopStorage{}
}
