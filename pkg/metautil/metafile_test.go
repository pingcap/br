// Copyright 2021 PingCAP, Inc. Licensed under Apache-2.0.

package metautil

import (
	"context"
	"crypto/sha256"
	"testing"

	"github.com/golang/mock/gomock"
	. "github.com/pingcap/check"
	backuppb "github.com/pingcap/kvproto/pkg/backup"

	"github.com/pingcap/br/pkg/mock"
)

type metaSuit struct{}

var _ = Suite(&metaSuit{})

func Test(t *testing.T) { TestingT(t) }

func checksum(m *backuppb.MetaFile) []byte {
	b, err := m.Marshal()
	if err != nil {
		panic(err)
	}
	sum := sha256.Sum256(b)
	return sum[:]
}

func (m *metaSuit) TestWalkMetaFileEmpty(c *C) {
	empty := &backuppb.MetaFile{}
	ch := make(chan *backuppb.MetaFile, 1)
	err := WalkLeafMetaFile(context.Background(), nil, empty, ch)
	c.Assert(err, IsNil)
	c.Assert(ch, HasLen, 1)
	c.Assert(<-ch, Equals, empty)
}

func (m *metaSuit) TestWalkMetaFileLeaf(c *C) {
	leaf := &backuppb.MetaFile{Schemas: []*backuppb.Schema{
		&backuppb.Schema{Db: []byte("db"), Table: []byte("table")},
	}}
	ch := make(chan *backuppb.MetaFile, 1)
	err := WalkLeafMetaFile(context.Background(), nil, leaf, ch)
	c.Assert(err, IsNil)
	c.Assert(ch, HasLen, 1)
	c.Assert(<-ch, Equals, leaf)
}

func (m *metaSuit) TestWalkMetaFileInvalid(c *C) {
	controller := gomock.NewController(c)
	defer controller.Finish()
	mockStorage := mock.NewMockExternalStorage(controller)

	ctx := context.Background()
	leaf := &backuppb.MetaFile{Schemas: []*backuppb.Schema{
		&backuppb.Schema{Db: []byte("db"), Table: []byte("table")},
	}}
	mockStorage.EXPECT().ReadFile(ctx, "leaf").Return(leaf.Marshal())

	root := &backuppb.MetaFile{MetaFiles: []*backuppb.File{
		&backuppb.File{Name: "leaf", Sha256: []byte{}},
	}}

	ch := make(chan *backuppb.MetaFile)
	err := WalkLeafMetaFile(ctx, mockStorage, root, ch)
	c.Assert(err, ErrorMatches, ".*ErrInvalidMetaFile.*")
}

func (m *metaSuit) TestWalkMetaFile(c *C) {
	controller := gomock.NewController(c)
	defer controller.Finish()
	mockStorage := mock.NewMockExternalStorage(controller)

	ctx := context.Background()
	expect := make([]*backuppb.MetaFile, 0, 6)
	got := make([]*backuppb.MetaFile, 0, 6)
	leaf31S1 := &backuppb.MetaFile{Schemas: []*backuppb.Schema{
		&backuppb.Schema{Db: []byte("db31S1"), Table: []byte("table31S1")},
	}}
	mockStorage.EXPECT().ReadFile(ctx, "leaf31S1").Return(leaf31S1.Marshal())
	expect = append(expect, leaf31S1)

	leaf31S2 := &backuppb.MetaFile{Schemas: []*backuppb.Schema{
		&backuppb.Schema{Db: []byte("db31S2"), Table: []byte("table31S2")},
	}}
	mockStorage.EXPECT().ReadFile(ctx, "leaf31S2").Return(leaf31S2.Marshal())
	expect = append(expect, leaf31S2)

	leaf32S1 := &backuppb.MetaFile{Schemas: []*backuppb.Schema{
		&backuppb.Schema{Db: []byte("db32S1"), Table: []byte("table32S1")},
	}}
	mockStorage.EXPECT().ReadFile(ctx, "leaf32S1").Return(leaf32S1.Marshal())
	expect = append(expect, leaf32S1)

	node21 := &backuppb.MetaFile{MetaFiles: []*backuppb.File{
		&backuppb.File{Name: "leaf31S1", Sha256: checksum(leaf31S1)},
		&backuppb.File{Name: "leaf31S2", Sha256: checksum(leaf31S2)},
	}}
	mockStorage.EXPECT().ReadFile(ctx, "node21").Return(node21.Marshal())

	node22 := &backuppb.MetaFile{MetaFiles: []*backuppb.File{
		&backuppb.File{Name: "leaf32S1", Sha256: checksum(leaf32S1)},
	}}
	mockStorage.EXPECT().ReadFile(ctx, "node22").Return(node22.Marshal())

	leaf23S1 := &backuppb.MetaFile{Schemas: []*backuppb.Schema{
		&backuppb.Schema{Db: []byte("db23S1"), Table: []byte("table23S1")},
	}}
	mockStorage.EXPECT().ReadFile(ctx, "leaf23S1").Return(leaf23S1.Marshal())
	expect = append(expect, leaf23S1)

	root := &backuppb.MetaFile{MetaFiles: []*backuppb.File{
		&backuppb.File{Name: "node21", Sha256: checksum(node21)},
		&backuppb.File{Name: "node22", Sha256: checksum(node22)},
		&backuppb.File{Name: "leaf23S1", Sha256: checksum(leaf23S1)},
	}}

	ch := make(chan *backuppb.MetaFile, 60)
	err := WalkLeafMetaFile(ctx, mockStorage, root, ch)
	c.Assert(err, IsNil)
	close(ch)
	for mf := range ch {
		got = append(got, mf)
	}

	c.Assert(got, HasLen, len(expect))
	for i := range expect {
		c.Assert(got[i], DeepEquals, expect[i])
	}
}
