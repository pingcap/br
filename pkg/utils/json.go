package utils

import (
	"encoding/hex"
	"encoding/json"

	"github.com/pingcap/errors"

	backuppb "github.com/pingcap/kvproto/pkg/backup"
)

type jsonFile struct {
	SHA256   string `json:"sha256,omitempty"`
	StartKey string `json:"start_key,omitempty"`
	EndKey   string `json:"end_key,omitempty"`
	*backuppb.File
}

func makeJSONFile(file *backuppb.File) *jsonFile {
	return &jsonFile{
		SHA256:   hex.EncodeToString(file.Sha256),
		StartKey: hex.EncodeToString(file.StartKey),
		EndKey:   hex.EncodeToString(file.EndKey),
		File:     file,
	}
}

func fromJSONFile(jFile *jsonFile) (*backuppb.File, error) {
	f := jFile.File
	var err error
	f.Sha256, err = hex.DecodeString(jFile.SHA256)
	if err != nil {
		return nil, errors.Trace(err)
	}
	f.StartKey, err = hex.DecodeString(jFile.StartKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	f.EndKey, err = hex.DecodeString(jFile.EndKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return f, nil
}

type jsonRawRange struct {
	StartKey string `json:"start_key,omitempty"`
	EndKey   string `json:"end_key,omitempty"`
	*backuppb.RawRange
}

func makeJSONRawRange(raw *backuppb.RawRange) *jsonRawRange {
	return &jsonRawRange{
		StartKey: hex.EncodeToString(raw.StartKey),
		EndKey:   hex.EncodeToString(raw.EndKey),
		RawRange: raw,
	}
}

func fromJSONRawRange(rng *jsonRawRange) (*backuppb.RawRange, error) {
	r := rng.RawRange
	var err error
	r.StartKey, err = hex.DecodeString(rng.StartKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	r.EndKey, err = hex.DecodeString(rng.EndKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return r, nil
}

type jsonSchema struct {
	// Use string here for compatibility
	Table string `json:"table,omitempty"`
	DB    string `json:"db,omitempty"`
	*backuppb.Schema
}

func makeJSONSchema(schema *backuppb.Schema) *jsonSchema {
	return &jsonSchema{
		Table:  string(schema.Table),
		DB:     string(schema.Db),
		Schema: schema,
	}
}

func fromJSONSchema(jSchema *jsonSchema) *backuppb.Schema {
	schema := jSchema.Schema
	schema.Db = []byte(jSchema.DB)
	schema.Table = []byte(jSchema.Table)
	return schema
}

type jsonBackupMeta struct {
	Files     []*jsonFile     `json:"files,omitempty"`
	RawRanges []*jsonRawRange `json:"raw_ranges,omitempty"`
	Schemas   []*jsonSchema   `json:"schemas,omitempty"`
	DDLs      string          `json:"ddls,omitempty"`

	*backuppb.BackupMeta
}

func makeJSONBackupMeta(meta *backuppb.BackupMeta) *jsonBackupMeta {
	result := &jsonBackupMeta{
		BackupMeta: meta,
	}
	for _, file := range meta.Files {
		result.Files = append(result.Files, makeJSONFile(file))
	}
	for _, rawRange := range meta.RawRanges {
		result.RawRanges = append(result.RawRanges, makeJSONRawRange(rawRange))
	}
	for _, schema := range meta.Schemas {
		result.Schemas = append(result.Schemas, makeJSONSchema(schema))
	}
	result.DDLs = string(meta.Ddls)
	return result
}

func fromJSONBackupMeta(jMeta *jsonBackupMeta) (*backuppb.BackupMeta, error) {
	meta := jMeta.BackupMeta
	for _, schema := range jMeta.Schemas {
		meta.Schemas = append(meta.Schemas, fromJSONSchema(schema))
	}
	for _, file := range jMeta.Files {
		f, err := fromJSONFile(file)
		if err != nil {
			return nil, err
		}
		meta.Files = append(meta.Files, f)
	}
	for _, rawRange := range jMeta.RawRanges {
		rng, err := fromJSONRawRange(rawRange)
		if err != nil {
			return nil, err
		}
		meta.RawRanges = append(meta.RawRanges, rng)
	}
	meta.Ddls = []byte(jMeta.DDLs)
	return meta, nil
}

// MarshalBackupMeta converts the backupmeta strcture to JSON.
// Unlike json.Marshal, this function also format some []byte fields for human reading.
func MarshalBackupMeta(meta *backuppb.BackupMeta) ([]byte, error) {
	return json.Marshal(makeJSONBackupMeta(meta))
}

// UnmarshalBackupMeta converts the prettied JSON format of backupmeta
// (made by MarshalBackupMeta) back to the go structure.
func UnmarshalBackupMeta(data []byte) (*backuppb.BackupMeta, error) {
	jMeta := &jsonBackupMeta{}
	if err := json.Unmarshal(data, jMeta); err != nil {
		return nil, errors.Trace(err)
	}
	return fromJSONBackupMeta(jMeta)
}
