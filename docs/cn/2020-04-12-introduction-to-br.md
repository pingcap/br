Last updated: 2020-04-17

## BR 简介

BR(backup & restore) 恰如其名，是用来备份/恢复 TiDB 数据的工具，这个工具后期会加入 SQL 中。

它本质上是一个“客户端”：使用 TiKV 提供的接口，来将备份和恢复的任务“下推”到 TiKV 集群中，后者则会直接将键值对写入指定的 SST 文件中。相似的接口也被 tidb-lightning 使用；详细的架构见下图，相关的设计文档在[这里](./2019-09-24-BR-and-lightning-reorganization.md)：

![img](../resources/arch-of-reorganized-importer.svg)

这篇文章会讲述这个“客户端”的基本行为，同时也会提到一些相关接口的数据结构、请求格式，以及我们如何用这个“原始”的 KV 备份/恢复接口实现带 schema 的备份和恢复。

### 备份的基本流程

最开始，我们拥有一个 TiDB 集群。为了保证我们备份到某个“一致性快照”，我们这样做：

- 从 PD 获取当前 TS，用作 `backup_ts`。
- 依照 `backup_ts` 来获取当时的 schema。

在获取 schema 的过程中，我们会过滤掉空数据库和系统数据库。

之后，我们会依照用户的需求来选择需要备份的表的 Key range，之后我们便会将备份“下推”到 TiKV。这时候我们会调用 TiKV 上的 `Backup` RPC，其请求定义如下，完整的定义见[这里](https://github.com/pingcap/kvproto/blob/master/proto/backup.proto)：

```protobuf
message BackupRequest {
    uint64 cluster_id = 1;

    bytes start_key = 2;
    bytes end_key = 3;
    uint64 start_version = 4;
    uint64 end_version = 5;

    // path field is deprecated, use storage_backend instead
    reserved 6; reserved "path";

    // The I/O rate limit for backup request.
    uint64 rate_limit = 7;
    // The concurrency for executing the backup request in every tikv node.
    uint32 concurrency = 8;

    StorageBackend storage_backend = 9;

    // If raw kv mode is enabled, `start_version` and `end_version` will be ignored, and `cf`
    // specifies which cf to backup.
    bool is_raw_kv = 10;
    string cf = 11;
}
```

在 TiKV 端，收到这个消息之后，会找到这个 Key Range 所在的所有 Regions，利用 `backup_ts` 获取 MVCC 中的一个快照：对 `WriteCF` 进行扫描，获得区间内的 `Put` 还有 `Delete` 记录（当然还要带上 DefaultCF 的数据）。将这些记录写入到 storage 的 SST 文件中；将备份结果返回给 BR 端，由此让 BR 端可以获得一些统计信息还有 checksums。

最后，随着 BR 将 backupmeta 写入存储，备份流程结束。

### 恢复的基本流程

在经历了备份之后，可以发现我们获得了许许多多新文件：

```
1_2_24_(sha256 of range startkey).sst
1_46_23_(sha256 of range startkey).sst
backupmeta
....
```

还请注意，这些文件不一定在运行 `br` 的机器上；一般地，它们可能会在对象存储或者 TiKV 节点上；其中 backupmeta 由 `br` 生成，而 SST 文件则是由 TiKV 集群生成。

前面的 SST files 的文件名是由一些元数据（诸如说，Store 的 ID，region 的 ID，region 的 epoch）来生成的。这些 SST 文件又存储着对我们来说最重要的 Rocks DB 键值对，也就是关系型数据库里面的记录（元组），您大概可以把他们的格式想象成这样：

```
't'{table_id}_'r'{row_id} -> [col1, col2, col3] // 一般的记录
't'{table_id}_'r'{row_id}_{index_value} -> row_id // 索引
```

看起来一切都很安详，在理想的世界中，只要把这些文件写回 RocksDB，我们的任务就完成了！

当然，在混沌的真实世界，我们会发现许多让人伤心的事实：诸如说，假如直接把这些 SST 文件写回 RocksDB，我们会遇到一些窘境。

- `table_id` 对应的表在全新的集群上可能还完全没有被创建；而且我们几乎不可能保证备份和恢复时会有完全相同的 `table_id`。
- 我们的集群只有少得可怜的 `region`，如果我们的数据太多，这些 `regions` 可能需要经历多次分裂最终才能发挥 multi-raft 的横向拓展能力。然而，恢复过程中分裂可能会导致大量的集群数据更新、大量的失败重试，这可能是难以接受的。

这两项问题引出了两个主题：`Key rewrite` 还有 `Split & Scatter`；但是在探索这些之前，先让我们来看看基本的流程。

1. 先把需要的 Database 和 Table 给创建好，这样我们就可以知道新的 Table ID 了。
2. 进行一些初始化工作，诸如说，让 TiKV 进入导入模式（import mode）。
3. 然后，我们可以看看 SST 文件中的 KV 对，我们可以以键的范围来分批，对每一批执行这些操作：
   a. 根据来自 Table 和 SST 文件的元信息，执行 Split & Scatter。
   b. 向 TiKV 节点发起请求，让它们试着去下载 SST。
   c. 向 TiKV 节点发起请求，让它们去 ingest SST。
4. 进行一些收尾工作，诸如说，让 TiKV 恢复到常规模式（normal mode）。

您可能发现，刚才似乎并没有提到 `key rewrite` 相关的东西；事实上，我们会在 a 和 b 这两步中附带上一组 `keyRewriteRules`，其中，b 步骤由 TiKV 完成相应的重写工作，而 a 步骤则是 BR 自己动手。

#### Key rewrite

在一切开始之前，让我们来温习一下 `key rewrite` 对于 `br` 的意义：它允许我们按照某种规则批量修改 SST file 中的键，因此我们可以借助它的力量来批量修改备份之时的 Table ID 为恢复之时创建的新 Table 的 ID。

某条 `key rewrite` 的规则是一个 `RewriteRule`，它的 `protocol buffers` 定义长这样：

```protobuf
message RewriteRule {
    bytes old_key_prefix = 1;
    bytes new_key_prefix = 2;
    uint64 new_timestamp = 3;
}
```

其中，`new_timestamp` 和 MVCC 有关。我们暂时可以不用管它。事实上，对于 `br` 来讲，我们的 `new_timestamp` 是在恢复伊始就确定好的：

- 假如说，在恢复全量备份，那么我们可以直接将其置为 0；这意味着，在下载的时候，希望保留原有的时间戳不要改变——让我们从零开始即可。
- 假如说，在恢复增量备份，那么我们则要从 PD 获取当前的时间戳。这样，我们可以写入到当前的位置。

注意，这里仅仅支持对前缀重写。原因为我们的键是排好序的，所以仅仅对前缀批量替换能够有相当好的效率。

就和刚才提到的一样，Key rewrite 在两处进行：一处是在 Split & Scatter 的时候，我们会利用这些规则来确定划分 Region 的边缘；还有一处是在 Import 的时候，TiKV 会批量重写下载的 SST file。事实上，在早期，关于 Key rewrite 的具体时机，我们曾经有过[一次讨论](./2019-09-09-BR-key-rewrite-disscussion.md)。

#### Split & Scatter

刚才提到，我们不希望在恢复的时候需要时不时地去让 region 分裂，先不提在使用“非常手段”来载入 SST 的时候，multi-raft 是否可以正常分裂，即便可以，在回复过程中的时间损耗也足够我们喝一壶了。所以我们希望，至少能够在下载 SST 之前，将需要的 regions 给分好。

我们先来复习一下 Region 的概念：它是某个 Raft 集群，维护着某个范围内的 Keys。Regions 在发现自己承载了过多的 Keys 之后，可以将自己“分裂”成若干个司掌更小范围的 Regions，以此来减少自身的负担。

我们的工作便是提前让 region 完成分裂：这一切有点点像是在传统的关系型数据库中预先按照键范围来分区。

不妨让我们先想象比较简单的场景：集群伊始，天地之间仅有一 region 而已，在接下来的图例中，您可以把横轴想象为某种“数轴”，不过这里的“数”是 []byte ，并且是按照字典序来比较大小的：

![img](../resources/region-split-1.svg)

然后，我们看了看我们的 backupmeta，发现了我们需要恢复的键的区间，注意我们保证键范围不会有相交处：

![img](../resources/region-split-2.svg)

不要忘记，我们还有可能会需要 Key rewrite：

![img](../resources/region-split-3.svg)

一个 Split 可能会是这样，我们把原本的 Region 1 拆散成了更小的四个部分：

![img](../resources/region-split-4.svg)

事实上，TiKV 提供了一个 `SplitRegion` RPC，如下是简化过的版本：
```protobuf
rpc SplitRegion (kvrpcpb.SplitRegionRequest) returns (kvrpcpb.SplitRegionResponse)
message SplitRegionRequest {
    Context context = 1;
    repeated bytes split_keys = 3; 
}
message SplitRegionResponse {
    errorpb.Error region_error = 1;
    repeated metapb.Region regions = 4;
}
```
这个 RPC 允许我们依照某些键作为“边界”，对某个 Region 进行“撕裂”，在原有的 TiKV 节点上产生更多的 regions，在这里，我们选用的“边界”是 `rewrite rule` 的 `NewPrefix` 还有每个 `Range` 的 `End`，这样做的好处是：我们可以保证每个 Region 仅仅只有一个 Key Range，也就是一个 SST File 需要我们恢复。

BR 便是使用了这组接口来完成 Region Split，具体而言，对于某个 `batch : []Range` 还有对应的 `rules : RewriteRules`，我们的所作所为大概如下：

1.  找到 `batch` 中的每个键在经历了 Key rewrite 之后可能会在的范围，具体地，这个范围是 `[minKey, maxKey)`， 其中：

   ```javascript
   minKey = [batch.min().start, ...rewriteRules.map(r => r.NewPrefix)].min()
   maxKey = [batch.max().end, ...rewriteRules.map(r => r.NewPrefix)].max()
   ```

2. 向 TiKV 查询在这个范围内的 Regions。

3. 对于这些 Regions，我们对 `batch` 中每个 `range` 的  `EndKey` 、`rules` 中每个 `rule` 的 `NewPrefix`，收集到一个 `map[RegionID][]Key` 中去。换句话说，就是找到每个键所在的 Region，伪代码如下。

   ```javascript
   keySet = [...batch.map(range => range.EndKey), ...rules.map(rule => rule.NewPrefix)]
   result = {}
   for key of keySet {
       let region = regionOf(key).ID
   	if (!(region in result)) { result[region] = [] }
       result[region].append(key)
   }
   return result
   ```

4. 对于 3 中的 `result` 中的每个 `(RegionID, []Key)`，我们将其送给 `SplitRegion` RPC。静候来自 TiKV 的佳音。

5. 在确认分区已经完成之后，我们发送请求给 PD，希望他将新生的 Region 给 “打散”到各个 TiKV 节点去，以此来均衡负载。

#### Import

在经历了 Split & Scatter 之后，我们获得了和我们的键范围相匹配的 Regions。之后的任务便是将这些文件给写到 RocksDB 中去了。

就如同在最开始的时候提到的一样：这一步也分成两步：download 和 ingest。

这两步都需要用到 SST 文件，它的数据结构如下：

```protobuf
message SSTMeta {
    bytes uuid = 1;
    Range range = 2;
    uint32 crc32 = 3;
    uint64 length = 4;
    string cf_name = 5;
    uint64 region_id = 6;
    metapb.RegionEpoch region_epoch = 7;
}
```

在下载这一步中，我们会对目标 Region 中**所有**的 TiKV 节点发送 `Download` 请求，这个请求的定义如下，详见[完整的 service 定义](https://github.com/pingcap/kvproto/blob/master/proto/import_sstpb.proto)，这个请求会让 TiKV 前往我们指定的存储中下载 SST 文件，并且使用我们指定的 Rewrite rules 来进行 key rewrite：

```protobuf
message DownloadRequest {
    SSTMeta sst = 2 [(gogoproto.nullable) = false];
    reserved 8; reserved "url";
    string name = 9;
    RewriteRule rewrite_rule = 13 [(gogoproto.nullable) = false];

    backup.StorageBackend storage_backend = 14;
}
```

您可能注意到了，这里的 Rewrite rule 只有而且只需要单一一条；因为前面我们提到过，我们保证我们的每个 SST 文件都只会存在一张表。所以，仅仅一个 Rewrite rule 就够用了，这样做可以减少 RPC 的复杂度。

在确定所有的 TiKV 都已经下载完毕之后，我们会对目标 Region 中的 Leader 发送 `Ingest` 请求，这个请求的定义如下，它的完整定义和上面的 `Download` 在相同的地方，这个请求会让 TiKV 将 SST 文件恢复到 RocksDB 中，这个恢复过程会通过 Raft 算法来保证一致性。

```protobuf
message IngestRequest {
    kvrpcpb.Context context = 1;
    SSTMeta sst = 2;
}
```

### 其它命题

#### 存储后端支持

目前提供四种存储后端：

- local/file: 存储于 TiKV/PR 本地文件系统上，您可以挂载网盘来集合备份数据。
- noop: 不存储任何备份，仅仅用做测试。
- s3: 存储于 S3 对象存储上。
- gcs: 存储于 Google Cloud Storage 上。

相应地，我们也支持写入存储后端的流量控制。

#### Raw KV 备份/恢复

Raw KV 备份允许您跳过 SQL 层，直接备份 TiKV 中的某个 Range。这个利用到了刚才提到的 `Backup` RPC 的最后两个参数，来通过原始的区间直接生成 SST 文件。

在恢复的时候，我们也会跳过 SQL 层，直接通过您指定的范围获取 SST 文件，而后进行恢复，恢复过程和带有事务的相仿。

#### 增量备份

指定 `lastbackupts` 之后，BR 就会试着备份从 `lastbackupts` 到 `backup_ts` 之间的增量数据。

需要注意的是，在全量备份和增量备份之间，或者两次增量备份之间，有可能会发生 MVCC 的 GC，在这种时候，因为 GC 的特点（不会保留最后一次 Delete），有可能出现一个键本该被删除，但是却意外地在备份中“永生”了的状况，因此在增量备份的时候，我们必须赶在 GC 之前备份。

10 分钟的 GC 间隔可能会过短，您可以手动调节 GC 的时间长度；另一方面，自适应的 GC 时间的提案亦已经就绪。

#### 在线恢复

在线恢复要求我们能够将我们的恢复流程尽量和当前其它业务隔离开来。我们使用 [Placement rules](https://pingcap.com/docs-cn/stable/how-to/configure/placement-rules/#placement-rules-%E4%BD%BF%E7%94%A8%E6%96%87%E6%A1%A3) 来完成这项工作：我们为正在恢复的表设置单独的 Rule 来完成隔离。

具体来讲，首先您需要启动一个单独的 TiKV 节点，并且给它打上 “restore” 的 label；之后，BR 会利用 Placement rules 来保证待恢复的键只会被分配到那个 TiKV 节点上去。由是保持了其它节点上的业务不会被恢复拖累。