// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/pingcap/br/pkg/storage (interfaces: ExternalStorage)

<<<<<<< HEAD:pkg/lightning/mock/storage.go
// $ mockgen -package mock github.com/pingcap/br/pkg/storage ExternalStorage

=======
>>>>>>> 2652f252... lightning: refactor the `backend` package (#877):pkg/mock/storage.go
// Package mock is a generated GoMock package.
package mock

import (
	context "context"
	gomock "github.com/golang/mock/gomock"
	storage "github.com/pingcap/br/pkg/storage"
	reflect "reflect"
)

// MockExternalStorage is a mock of ExternalStorage interface
type MockExternalStorage struct {
	ctrl     *gomock.Controller
	recorder *MockExternalStorageMockRecorder
}

// MockExternalStorageMockRecorder is the mock recorder for MockExternalStorage
type MockExternalStorageMockRecorder struct {
	mock *MockExternalStorage
}

// NewMockExternalStorage creates a new mock instance
func NewMockExternalStorage(ctrl *gomock.Controller) *MockExternalStorage {
	mock := &MockExternalStorage{ctrl: ctrl}
	mock.recorder = &MockExternalStorageMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use
func (m *MockExternalStorage) EXPECT() *MockExternalStorageMockRecorder {
	return m.recorder
}

// CreateUploader mocks base method
func (m *MockExternalStorage) CreateUploader(arg0 context.Context, arg1 string) (storage.Uploader, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "CreateUploader", arg0, arg1)
	ret0, _ := ret[0].(storage.Uploader)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// CreateUploader indicates an expected call of CreateUploader
func (mr *MockExternalStorageMockRecorder) CreateUploader(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "CreateUploader", reflect.TypeOf((*MockExternalStorage)(nil).CreateUploader), arg0, arg1)
}

// FileExists mocks base method
func (m *MockExternalStorage) FileExists(arg0 context.Context, arg1 string) (bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "FileExists", arg0, arg1)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// FileExists indicates an expected call of FileExists
func (mr *MockExternalStorageMockRecorder) FileExists(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "FileExists", reflect.TypeOf((*MockExternalStorage)(nil).FileExists), arg0, arg1)
}

// Open mocks base method
func (m *MockExternalStorage) Open(arg0 context.Context, arg1 string) (storage.ReadSeekCloser, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Open", arg0, arg1)
	ret0, _ := ret[0].(storage.ReadSeekCloser)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Open indicates an expected call of Open
func (mr *MockExternalStorageMockRecorder) Open(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Open", reflect.TypeOf((*MockExternalStorage)(nil).Open), arg0, arg1)
}

// Read mocks base method
func (m *MockExternalStorage) Read(arg0 context.Context, arg1 string) ([]byte, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Read", arg0, arg1)
	ret0, _ := ret[0].([]byte)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Read indicates an expected call of Read
func (mr *MockExternalStorageMockRecorder) Read(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Read", reflect.TypeOf((*MockExternalStorage)(nil).Read), arg0, arg1)
}

// URI mocks base method
func (m *MockExternalStorage) URI() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "URI")
	ret0, _ := ret[0].(string)
	return ret0
}

// URI indicates an expected call of URI
func (mr *MockExternalStorageMockRecorder) URI() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "URI", reflect.TypeOf((*MockExternalStorage)(nil).URI))
}

// WalkDir mocks base method
func (m *MockExternalStorage) WalkDir(arg0 context.Context, arg1 *storage.WalkOption, arg2 func(string, int64) error) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "WalkDir", arg0, arg1, arg2)
	ret0, _ := ret[0].(error)
	return ret0
}

// WalkDir indicates an expected call of WalkDir
func (mr *MockExternalStorageMockRecorder) WalkDir(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "WalkDir", reflect.TypeOf((*MockExternalStorage)(nil).WalkDir), arg0, arg1, arg2)
}

// Write mocks base method
func (m *MockExternalStorage) Write(arg0 context.Context, arg1 string, arg2 []byte) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Write", arg0, arg1, arg2)
	ret0, _ := ret[0].(error)
	return ret0
}

// Write indicates an expected call of Write
func (mr *MockExternalStorageMockRecorder) Write(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Write", reflect.TypeOf((*MockExternalStorage)(nil).Write), arg0, arg1, arg2)
}
