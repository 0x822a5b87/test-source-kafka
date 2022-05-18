# readme

## 描述

> 《kafka源码解析与实战》阅读笔记。

## 目录

1. [broker概述](./ch03.md)
2. [broker的基本模块](./ch04.md)

## 编译

### MavenDeployment

> 使用 `gradle 6.2` 可以直接编译，更高版本的部分插件被移除。

### ScalaCompileOptions.metaClass.useAnt ...

> 在 `build.gradle` 最上方添加
>
> ```java
> ScalaCompileOptions.metaClass.daemonServer = true
> ScalaCompileOptions.metaClass.fork = true
> ScalaCompileOptions.metaClass.useAnt = false
> ScalaCompileOptions.metaClass.useCompileDaemon = false
> ```

### allowInsecureProtocol

> gradle 要求用户使用私有库时显示的声明，所以在 buildscript.gradle 中增加配置
>
> ```json
> repositories {
> repositories {
>  // For license plugin.
>  maven {
>    url = 'http://dl.bintray.com/content/netflixoss/external-gradle-plugins/'
>    allowInsecureProtocol = true
>  }
> }
> }
> ```

## 问题记录

### OffsetIndex and FileMessageSet

> kafka 的实际日志由 `OffsetIndex` 和 `FileMessageSet` 两个部分组成，其中 `OffsetIndex` 被映射成了一个 MMap；那么 kafka 怎么保证它和 FileMessageStat 是对得上的呢？

### read(startOffset: Long, maxLength: Int, maxOffset: Option[Long] = None): 

> 为什么 LogManager 在读数据的时候，需要有一个 `maxOffset` 指定可以拉取的最大偏移数？
>
> - `maxOffset` 是因为在有些情况下Segment的可读数据与日志的实际存储数据不一致；例如，当数据已经写入 leader，但是 follower 尚未 fetch 到这些数据，此时 HighWatermark 就会使得 Segment 中有一部分数据不可读。

### 为什么每次提交 FetchRequest 时都需要带上自己的 offset

> 在我们 `AbstractFetcherThread#doWork()` 方法中，我们定期的读取 `partitionMap` 中保存的 `<TopicAndPartition> -> offset` 的值，并构建一个 `FetchRequest` 发送到服务端。
>
> 而 `partitionMap` 只会在以下几个情况下更新：
>
> 1. `AbstractFetcherThread#processFetchRequest` 方法中，当 FetchRequest 请求得到一个 `FetchResponse` 时，会使用 `FetchResponse` 得到的数据的 offset 来更新；
> 2. `AbstractFetcherThread#processFetchRequest` 方法中，在出现 `ErrorMapping.OffsetOutOfRangeCode` 异常时，更新；
> 3. `AbstractFetcherThread#addPartitions` 方法中，在 partition 初始化时更新；
>
> 那么现在 `partitionMap` 保留了每个 replica 的 `LogEndOffset`，之后我们在下一次 `FetchRequest` 的时候，将新的 offset 提交上去，这样 leader 就可以使用所有副本上报的 offset 来确定当前的 `HighWatermark` 了。

### 更新 offset 在数据持久化之前，怎么保证上报的 offset 是已经持久化的呢

>  `AbstractFetcherThread#doWork()` -> `AbstractFetcherThread#processFetchRequest()` ->`ReplicaFetcherThread#processPartitionData`  
>
> 实际的调用顺序如上，他们是在单线程中的，所以不用担心会上报未持久化的 offset。

### correlationId 的作用是什么？

> `correlationId` 是 int32 类型，由客户端在向服务端发送请求的时候标识的请求ID，服务端在处理完请求之后会将 `correlationId` 放置在 `Response` 中，这样客户端可以把自己发送的 `Request` 还有服务端的 `Response` 关联起来。

### ReplicaManager 中的 `allPartitions` 代表了什么

> `allPartitions` 是一个 key 为 `<topic, partitionId>`， value 为对应的 `Partition` 的 `Pool`，他保存了本台 broker 上的所有 Partition 信息。
>
> ```scala
> private val allPartitions = new Pool[(String, Int), Partition]
> ```
>
> `allPartitions` 仅仅在 `ReplicaManager#getOrCreatePartition` 方法中被修改。
>
> > `KafkaApis#handleLeaderAndIsrRequest` -> `ReplicaManager#becomeLeaderOrFollower` -> `ReplicaManager#getOrCreatePartition`
>
> **在 broker 启动的时候**，controller 会通过 `KafkaController#onBrokerStartup` 要求 broker 初始化 `allPartitions`
>
> > `KafkaController#onBrokerStartup` -> `KafkaController#sendUpdateMetadataRequest` -> `ControllerChannelManager#addUpdateMetadataRequestForBrokers` 或者 `ControllerChannelManager#sendRequestsToBrokers`

### `KafkaApis#handleOffsetRequest`  中 `fetchOffsets` 为什么返回的是 `<TopicAndPartition> -> Seq[Long]`？

> 一个 <TopicAndPartition> 对应一个 offset 没问题，但是实际上，我们的 Log 由多个 LogSegment 构成，我们在消费的时候，可能是指定了 **auto.offset.reset == ealiest**，那么我们可能需要从最早的 LogSegment 开始消费数据。

### `KafkaApis#metadataCache` 有什么用？

> `KafkaApis#metadataCache` 缓存了 **kafka topic 的 metadata 以及 aliveBrokers** 相关信息，**它接收从 controller 发来的 `UpdateMetadataRequest` 来更新本地缓存信息。**
>
> ```scala
> private[server] class MetadataCache {
>   private val cache: mutable.Map[String, mutable.Map[Int, PartitionStateInfo]] = new mutable.HashMap[String, mutable.Map[Int, PartitionStateInfo]]()
>   private var aliveBrokers: Map[Int, Broker] = Map()
>   
>   // 同时提供了 updateCache 方法更新缓存
>   def updateCache(updateMetadataRequest: UpdateMetadataRequest,
>                   brokerId: Int,
>                   stateChangeLogger: StateChangeLogger): Unit = {
> 
>     inWriteLock(partitionMetadataLock) {
>       // 根据 controller 发送的 <UpdateMetadataRequest> 来更新存活的 brokers
>       aliveBrokers = updateMetadataRequest.aliveBrokers.map(b => (b.id, b)).toMap
>       // 根据 controller 发送的 <UpdateMetadataRequest> 来更新 partition 状态信息
>       updateMetadataRequest.partitionStateInfos.foreach { case(tp, info) =>
>         if (info.leaderIsrAndControllerEpoch.leaderAndIsr.leader == LeaderAndIsr.LeaderDuringDelete) {
>           removePartitionInfo(tp.topic, tp.partition)
>         } else {
>           addOrUpdatePartitionInfo(tp.topic, tp.partition, info)
>         }
>       }
>     }
>   }
> }
> ```
>
> 调用链为
>
> > `controler 发送 UpdateMetadataRequest` -> `kafkaApis#handleUpdateMetadataRequest` -> `maybeUpdateMetadataCache#maybeUpdateMetadataCache` -> `MetadataCache#updateCache`

### Partition and Replica

> 注意到，在 `ReplicaManager#stopReplicas` 中，有一行：
>
> ```scala
> // First stop fetchers for all partitions, then stop the corresponding replicas
> replicaFetcherManager.removeFetcherForPartitions(stopReplicaRequest.partitions.map(r => TopicAndPartition(r.topic, r.partition)))
> ```
>
> 那么在实际的代码中 `Partition` 和 `Replica` 到底是一个怎么样的概念呢？

> ```scala
> /**
>  * Data structure that represents a topic partition. The leader maintains the AR, ISR, CUR, RAR
>  */
> class Partition(val topic: String,
>                 val partitionId: Int,
>                 time: Time,
>                 replicaManager: ReplicaManager) extends Logging with KafkaMetricsGroup {
>   private val localBrokerId = replicaManager.config.brokerId
>   // AR
>   private val assignedReplicaMap = new Pool[Int, Replica]
>   @volatile var leaderReplicaIdOpt: Option[Int] = None
>   // ISR
>   @volatile var inSyncReplicas: Set[Replica] = Set.empty[Replica]
> }
> ```
>
> 以上是 Partition 的定义，每个 Partition 都保存了 `topic`, `partitionId`, `localBrokerId`,同时 leader 保存了 `AR`, `ISR`：**从这里可以看得出来， Partition 和  Replica 是一个一对多的关系。** 
>
> 
>
> 另外还可以参考 [FetchRequest](#FetchRequest) 这里的解释。

### AbstractFetcherManager#shutdownIdleFetcherThreads

> 在 `AbstractFetcherManager#shutdownIdleFetcherThreads` 中，我们回收了空闲 fetcher，那么 fetcher 到底是什么呢？
>
> ```scala
>   def shutdownIdleFetcherThreads(): Unit = {
>     mapLock synchronized {
>       val keysToBeRemoved = new mutable.HashSet[BrokerAndFetcherId]
>       for ((key, fetcher) <- fetcherThreadMap) {
>         if (fetcher.partitionCount <= 0) {
>           fetcher.shutdown()
>           keysToBeRemoved += key
>         }
>       }
>       fetcherThreadMap --= keysToBeRemoved
>     }
>   }
> ```

> `fetcher` `AbstractFetcherThread` 包含了 `ConsumerFetcherThread` 和 `ReplicaFetcherThread` 两个不同的实现，我们以 `ReplicaFetcherThread` 为例子：
>
> Partition 通过 `ReplicaFetcherThread` 来拉取 leader 上的数据。
>
> - fetcher 的实际线程数根据 `num.replica.fetchers(默认为1)` 以及 `要连接的broker` 共同决定；
> - 一台 broker 上会包含很多 <TopicAndPartition> 的 follower，并且这些 <TopicAndPartition> 的 leader 分布在不同的 broker 上；每个 fetcher 线程只会连接其中一台 broker 并同步分布在这台 broker 上的 <TopicAndPartition>；也就是说，实际的 fetcher 线程的数量最大等于 `num.replica.fetchers` * `num.of.brokers - 1`（但是实际不一定，因为有可能某台broker的所有 leader 都不在另外一台 broker 上，此时不会有 fetcher 线程去连接这两台 broker。）
> - **不同的broker只会起一个线程进行同步**，而这个线程会同步这个broker上的所有不同topic和partition的数据。

### ControlledShutdownRequest 

#### 问题1

> `controllerContext.shuttingDownBrokerIds.add(id)` 在将 broker 添加到下线名单之后会有什么影响？

#### 问题2

> 为什么在 `replicationFactor > 1` 只处理了副本数大于1的 partition？

#### 问题3

> 对于 `currLeaderIsrAndControllerEpoch.leaderAndIsr.leader == id` 这种是leader的分区，为什么是设置 partition 状态为 OnlinePartition？

### OffsetRequest 和 OffsetFetchRequest 的差别

### OffsetManager#getOffsets 中只有 leader 提供服务

>`KafkaApis#handleOffsetFetchRequest` -> `OffsetManager#getOffsets`，本方法只有在 `KafkaApis#handleOffsetFetchRequest` 中会调用，而这个调用则来自于 `RequestChannel` 中取出的 `Request`。
>
>controller 只会向 leader 发送这个消息。

