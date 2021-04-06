package utils

import (
	"encoding/json"
	"fmt"
	"reflect"

	. "github.com/pingcap/check"
)

type testJSONSuite struct{}

var _ = Suite(&testJSONSuite{})

var testMetaJSONs = [][]byte{
	[]byte(`{
  "files": [
    {
      "sha256": "aa5cefba077644dbb2aa1d7fae2a0f879b56411195ad62d18caaf4ec76fae48f",
      "start_key": "7480000000000000365f720000000000000000",
      "end_key": "7480000000000000365f72ffffffffffffffff00",
      "name": "1_2_29_6e97c3b17c657c4413724f614a619f5b665b990187b159e7d2b92552076144b6_1617351201040_write.sst",
      "end_version": 423978913229963260,
      "crc64xor": 8093018294706077000,
      "total_kvs": 1,
      "total_bytes": 27,
      "cf": "write",
      "size": 1423
    }
  ],
  "schemas": [
    {
      "table": "{\"id\":54,\"name\":{\"O\":\"test\",\"L\":\"test\"},\"charset\":\"utf8mb4\",\"collate\":\"utf8mb4_bin\",\"cols\":[{\"id\":1,\"name\":{\"O\":\"pk\",\"L\":\"pk\"},\"offset\":0,\"origin_default\":null,\"origin_default_bit\":null,\"default\":null,\"default_bit\":null,\"default_is_expr\":false,\"generated_expr_string\":\"\",\"generated_stored\":false,\"dependences\":null,\"type\":{\"Tp\":15,\"Flag\":4099,\"Flen\":256,\"Decimal\":0,\"Charset\":\"utf8mb4\",\"Collate\":\"utf8mb4_bin\",\"Elems\":null},\"state\":5,\"comment\":\"\",\"hidden\":false,\"change_state_info\":null,\"version\":2}],\"index_info\":[{\"id\":1,\"idx_name\":{\"O\":\"PRIMARY\",\"L\":\"primary\"},\"tbl_name\":{\"O\":\"\",\"L\":\"\"},\"idx_cols\":[{\"name\":{\"O\":\"pk\",\"L\":\"pk\"},\"offset\":0,\"length\":-1}],\"state\":5,\"comment\":\"\",\"index_type\":1,\"is_unique\":true,\"is_primary\":true,\"is_invisible\":false,\"is_global\":false}],\"constraint_info\":null,\"fk_info\":null,\"state\":5,\"pk_is_handle\":false,\"is_common_handle\":true,\"common_handle_version\":1,\"comment\":\"\",\"auto_inc_id\":0,\"auto_id_cache\":0,\"auto_rand_id\":0,\"max_col_id\":1,\"max_idx_id\":1,\"max_cst_id\":0,\"update_timestamp\":423978913176223755,\"ShardRowIDBits\":0,\"max_shard_row_id_bits\":0,\"auto_random_bits\":0,\"pre_split_regions\":0,\"partition\":null,\"compression\":\"\",\"view\":null,\"sequence\":null,\"Lock\":null,\"version\":4,\"tiflash_replica\":null,\"is_columnar\":false}",
      "db": "{\"id\":1,\"db_name\":{\"O\":\"test\",\"L\":\"test\"},\"charset\":\"utf8mb4\",\"collate\":\"utf8mb4_bin\",\"state\":5}",
      "crc64xor": 8093018294706077000,
      "total_kvs": 1,
      "total_bytes": 27
    }
  ],
  "ddls": "[]",
  "cluster_id": 6946469498797568000,
  "cluster_version": "\"5.0.0-rc.x\"\n",
  "end_version": 423978913229963260,
  "br_version": "BR\nRelease Version: v5.0.0-master\nGit Commit Hash: c0d60dae4998cf9ac40f02e5444731c15f0b2522\nGit Branch: HEAD\nGo Version: go1.13.4\nUTC Build Time: 2021-03-25 08:10:08\nRace Enabled: false"
}
`),
	[]byte(`{
  "files": [
    {
      "sha256": "5759c4c73789d6ecbd771b374d42e72a309245d31911efc8553423303c95f22c",
      "end_key": "7480000000000000ff0500000000000000f8",
      "name": "1_4_2_default.sst",
      "total_kvs": 153,
      "total_bytes": 824218,
      "cf": "default",
      "size": 44931
    },
    {
      "sha256": "87597535ce0edbc9a9ef124777ad1d23388467e60c0409309ad33af505c1ea5b",
      "start_key": "7480000000000000ff0f00000000000000f8",
      "end_key": "7480000000000000ff1100000000000000f8",
      "name": "1_16_8_58be9b5dfa92efb6a7de2127c196e03c5ddc3dd8ff3a9b3e7cd4c4aa7c969747_1617689203876_default.sst",
      "total_kvs": 1,
      "total_bytes": 396,
      "cf": "default",
      "size": 1350
    },
    {
      "sha256": "97bd1b07f9cc218df089c70d454e23c694113fae63a226ae0433165a9c3d75d9",
      "start_key": "7480000000000000ff1700000000000000f8",
      "end_key": "7480000000000000ff1900000000000000f8",
      "name": "1_24_12_72fa67937dd58d654197abadeb9e633d92ebccc5fd993a8e54819a1bd7f81a8c_1617689203853_default.sst",
      "total_kvs": 35,
      "total_bytes": 761167,
      "cf": "default",
      "size": 244471
    },
    {
      "sha256": "6dcb6ba2ff11f4e7db349effc98210ba372bebbf2470e6cd600ed5f2294330e7",
      "start_key": "7480000000000000ff3100000000000000f8",
      "end_key": "7480000000000000ff3300000000000000f8",
      "name": "1_50_25_2f1abd76c185ec355039f5b4a64f04637af91f80e6cb05099601ec6b9b1910e8_1617689203867_default.sst",
      "total_kvs": 22,
      "total_bytes": 1438283,
      "cf": "default",
      "size": 1284851
    },
    {
      "sha256": "ba603af7ecb2e21c8f145d995ae85eea3625480cd8186d4cffb53ab1974d8679",
      "start_key": "7480000000000000ff385f72ffffffffffffffffff0000000000fb",
      "name": "1_2_33_07b745c3d5a614ed6cc1cf21723b161fcb3e8e7d537546839afd82a4f392988c_1617689203895_default.sst",
      "total_kvs": 260000,
      "total_bytes": 114425025,
      "cf": "default",
      "size": 66048845
    }
  ],
  "raw_ranges": [
    {
      "cf": "default"
    }
  ],
  "ddls": "null",
  "cluster_id": 6946469498797568000,
  "cluster_version": "\"5.0.0-rc.x\"\n",
  "is_raw_kv": true,
  "br_version": "BR\nRelease Version: v5.0.0-master\nGit Commit Hash: c0d60dae4998cf9ac40f02e5444731c15f0b2522\nGit Branch: HEAD\nGo Version: go1.13.4\nUTC Build Time: 2021-03-25 08:10:08\nRace Enabled: false"
}
`),
}

type jsonEquals struct{}

func (j jsonEquals) Info() *CheckerInfo {
	return &CheckerInfo{
		Name:   "JSONEquals",
		Params: []string{"lhs", "rhs"},
	}
}

func (j jsonEquals) Check(params []interface{}, _ []string) (result bool, error string) {
	lhs := params[0].([]byte)
	rhs := params[1].([]byte)

	lhsMap := map[string]interface{}{}
	rhsMap := map[string]interface{}{}

	if err := json.Unmarshal(lhs, &lhsMap); err != nil {
		return false, fmt.Sprintf("failed to unmarshal lhs: %s", error)
	}
	if err := json.Unmarshal(rhs, &rhsMap); err != nil {
		return false, fmt.Sprintf("failed to unmarshal rhs: %s", error)
	}
	if !reflect.DeepEqual(lhsMap, rhsMap) {
		return false, fmt.Sprintf("lhs %s not equals rhs %s", lhsMap, rhsMap)
	}
	return true, ""
}

func (testJSONSuite) TestEncodeAndDecode(c *C) {
	for _, testMetaJSON := range testMetaJSONs {
		meta, err := UnmarshalBackupMeta(testMetaJSON)
		c.Assert(err, IsNil)
		metaJSON, err := MarshalBackupMeta(meta)
		c.Assert(err, IsNil)
		c.Assert(metaJSON, jsonEquals{}, testMetaJSON)
	}
}