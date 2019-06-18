package raw

import (
	"testing"

	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/kvproto/pkg/errorpb"
)

type testRaw struct {
}

var _ = Suite(&testRaw{})

func TestT(t *testing.T) {
	TestingT(t)
}

func (r *testRaw) TestHandleBackupError(c *C) {
	// Test step errors
	regionErr, err := handleBackupError(
		&backup.BackupResponse{
			Error: &backup.Error{
				Detail: &backup.Error_StateStepError{
					StateStepError: &backup.StateStepError{
						Current: backup.BackupState_StartFullBackup,
						Request: backup.BackupState_StartFullBackup,
					},
				},
			},
		},
		backup.BackupState_StartFullBackup,
	)
	c.Assert(regionErr, IsNil)
	c.Assert(err, IsNil)

	regionErr, err = handleBackupError(
		&backup.BackupResponse{
			Error: &backup.Error{
				Detail: &backup.Error_StateStepError{
					StateStepError: &backup.StateStepError{
						Current: backup.BackupState_StartFullBackup,
						Request: backup.BackupState_FinishFullBackup,
					},
				},
			},
		},
		backup.BackupState_FinishFullBackup,
	)
	c.Assert(regionErr, IsNil)
	c.Assert(err, NotNil)

	// Test region errors
	regionErr, err = handleBackupError(
		&backup.BackupResponse{
			Error: &backup.Error{
				Detail: &backup.Error_RegionError{
					RegionError: &errorpb.Error{
						NotLeader: &errorpb.NotLeader{},
					},
				},
			},
		},
		backup.BackupState_FinishFullBackup,
	)
	c.Assert(regionErr, NotNil)
	c.Assert(err, IsNil)

	// Test cluster ID error
	regionErr, err = handleBackupError(
		&backup.BackupResponse{
			Error: &backup.Error{
				Detail: &backup.Error_ClusterIdError{
					ClusterIdError: &backup.ClusterIDError{},
				},
			},
		},
		backup.BackupState_FinishFullBackup,
	)
	c.Assert(regionErr, IsNil)
	c.Assert(err, NotNil)
}
