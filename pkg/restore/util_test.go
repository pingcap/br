package restore

import (
	. "github.com/pingcap/check"
	"github.com/pingcap/kvproto/pkg/backup"
	"github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	restore_util "github.com/pingcap/tidb-tools/pkg/restore-util"
	"github.com/pingcap/tidb/tablecodec"
)

var _ = Suite(&testRestoreUtilSuite{})

type testRestoreUtilSuite struct {
}

func (s *testRestoreUtilSuite) TestGetSSTMetaFromFile(c *C) {
	file := &backup.File{
		Name:     "file_write.sst",
		StartKey: []byte("t1a"),
		EndKey:   []byte("t1ccc"),
	}
	rule := &import_sstpb.RewriteRule{
		OldKeyPrefix: []byte("t1"),
		NewKeyPrefix: []byte("t2"),
	}
	region := &metapb.Region{
		StartKey: []byte("t2abc"),
		EndKey:   []byte("t3a"),
	}
	sstMeta := getSSTMetaFromFile([]byte{}, file, region, rule)
	c.Assert(string(sstMeta.GetRange().GetStart()), Equals, "t2abc")
	c.Assert(string(sstMeta.GetRange().GetEnd()), Equals, "t2\xff")
}

func (s *testRestoreUtilSuite) TestValidateFileRanges(c *C) {
	rules := &restore_util.RewriteRules{
		Table: []*import_sstpb.RewriteRule{&import_sstpb.RewriteRule{
			OldKeyPrefix: []byte(tablecodec.EncodeTablePrefix(1)),
			NewKeyPrefix: []byte(tablecodec.EncodeTablePrefix(2)),
		}},
	}

	// Empty start/end key is not allowed.
	_, err := ValidateFileRanges(
		[]*backup.File{&backup.File{
			Name:     "file_write.sst",
			StartKey: []byte(""),
			EndKey:   []byte(""),
		}},
		rules,
	)
	c.Assert(err, ErrorMatches, ".*cannot find rewrite rule.*")

	// Range is not overlap, no rule found.
	_, err = ValidateFileRanges(
		[]*backup.File{{
			Name:     "file_write.sst",
			StartKey: tablecodec.EncodeTablePrefix(0),
			EndKey:   tablecodec.EncodeTablePrefix(1),
		}},
		rules,
	)
	c.Assert(err, ErrorMatches, ".*cannot find rewrite rule.*")

	// No rule for end key.
	_, err = ValidateFileRanges(
		[]*backup.File{{
			Name:     "file_write.sst",
			StartKey: tablecodec.EncodeTablePrefix(1),
			EndKey:   tablecodec.EncodeTablePrefix(2),
		}},
		rules,
	)
	c.Assert(err, ErrorMatches, ".*cannot find rewrite rule.*")

	// Add a rule for end key.
	rules.Table = append(rules.Table, &import_sstpb.RewriteRule{
		OldKeyPrefix: tablecodec.EncodeTablePrefix(2),
		NewKeyPrefix: tablecodec.EncodeTablePrefix(3),
	})
	_, err = ValidateFileRanges(
		[]*backup.File{{
			Name:     "file_write.sst",
			StartKey: tablecodec.EncodeTablePrefix(1),
			EndKey:   tablecodec.EncodeTablePrefix(2),
		}},
		rules,
	)
	c.Assert(err, ErrorMatches, "table ids dont match")

	// Add a bad rule for end key, after rewrite start key > end key.
	rules.Table = append(rules.Table[:1], &import_sstpb.RewriteRule{
		OldKeyPrefix: tablecodec.EncodeTablePrefix(2),
		NewKeyPrefix: tablecodec.EncodeTablePrefix(1),
	})
	_, err = ValidateFileRanges(
		[]*backup.File{{
			Name:     "file_write.sst",
			StartKey: tablecodec.EncodeTablePrefix(1),
			EndKey:   tablecodec.EncodeTablePrefix(2),
		}},
		rules,
	)
	c.Assert(err, ErrorMatches, "unexpected rewrite rules")
}
