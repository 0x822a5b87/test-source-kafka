# readme

## 描述

> 《kafka源码解析与实战》阅读笔记。

## 目录

1. [broker概述](./ch03.md)
2. [broker的基本模块](./ch04.md)
3. [broker的控制管理模块](./ch05.md)

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

### ZkUtils#createEphemeralPathExpectConflictHandleZKBug 中为何需要在 while(true) 循环中进行重试

>`ZkUtils#createEphemeralPathExpectConflictHandleZKBug` 会在三种情况下被调用：
>
>- ZookeeperLeaderElector#elect 该函数使用当前broker进行leader选举
>- ZkUtils#registerBrokerInZk 该函数将 brokerId 注册到 zk 的 `/brokers/ids/[id]`
>- ZookeeperConsumerConnector#registerConsumerInZK 注册消费者到 zk
>
>**当zk客户端 session expire 时，zk 集群会释放之前创建的瞬时节点，如果此时zk由于某种原因挂起（hang），此时瞬时节点的删除会延迟。**当时当 zk 客户端重连成功的时候，会认为之前创建的节点已经删除，就会去重新创建，**此时会抛出 NodeExistException，为此 `createEphemeralPathExpectConflictHandleZKBug` 方法使用了一个策略。**即在 while 循环中去重试，当实际数据和写入数据一致时，说明是同一个客户端的旧连接数据未清理，需要等到其被释放后重试。
>
>```scala
>  def createEphemeralPathExpectConflictHandleZKBug(zkClient: ZkClient, path: String, data: String, expectedCallerData: Any, checker: (String, Any) => Boolean, backoffTime: Int): Unit = {
>    // 在 while 循环中尝试，因为zk的bug，ephemeral node 可能在session expired之后继续存活
>    while (true) {
>      try {
>        // 注册节点，如果注册成功则直接返回
>        createEphemeralPathExpectConflict(zkClient, path, data)
>        return
>      } catch {
>        case e: ZkNodeExistsException => {
>          ZkUtils.readDataMaybeNull(zkClient, path)._1 match {
>            // 如果能读到数据，说明节点还存在：
>            case Some(writtenData) => {
>              // 实际数据和写入数据一致，说明是同一个客户端不同的旧连接的数据没有被释放，需要等待其被释放后重试。
>              if (checker(writtenData, expectedCallerData)) {
>                info("I wrote this conflicted ephemeral node [%s] at %s a while back in a different session, ".format(data, path)
>                  + "hence I will backoff for this node to be deleted by Zookeeper and retry")
>
>                Thread.sleep(backoffTime)
>              } else {
>                // 实际数据和写入数据不一致，说明已经被其他的客户端抢占
>                throw e
>              }
>            }
>            // 如果读不到数据，说明节点已经不存在了，尝试重新创建 ephemeral node
>            case None =>
>          }
>        }
>        case e2: Throwable => throw e2
>      }
>    }
>  }
>```
>
>`ZookeeperLeaderElector#elect`：在这里，brokerId 作为 expectedValue，如果返回值与 brokerId 相同的话说明 brokerId 就是 leader，继续尝试成为leader，否则退出循环（也就是退出leader选举）。
>
>```scala
>      createEphemeralPathExpectConflictHandleZKBug(controllerContext.zkClient, electionPath, electString, brokerId,
>        (controllerString : String, leaderId : Any) => KafkaController.parseControllerId(controllerString) == leaderId.asInstanceOf[Int],
>        controllerContext.zkSessionTimeout)
>```

### kafka 创建 topic 的实际流程是怎么样的？

> kafka 创建 topic 在 `AdminUtils#createTopic` 下实现，而有以下几种情况可能会创建 topic：
>
> - 在 `TopicCommand#createTopic` 下被调用；
> - 开启了 `autoCreateTopic` 的状态下，在处理 `ConsumerMetadataRequest`、`TopicMetadataRequest` 时会自动的创建 topic；
>
> 我们以 `TopicCommand#createTopic` 为例说明。

#### TopicCommand#createTopic

> TopicCommand#createTopic 接收命令行脚本的命令，并创建topic

```scala
  def createTopic(zkClient: ZkClient, opts: TopicCommandOptions): Unit = {
    val topic = opts.options.valueOf(opts.topicOpt)
    val configs = parseTopicConfigsToBeAdded(opts)
    if (opts.options.has(opts.replicaAssignmentOpt)) {
      // 手动的分配 partition -> broker
      val assignment = parseReplicaAssignment(opts.options.valueOf(opts.replicaAssignmentOpt))
      AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic, assignment, configs)
    } else {
      // 只指定partition count和replica count，由controller自动创建
      CommandLineUtils.checkRequiredArgs(opts.parser, opts.options, opts.partitionsOpt, opts.replicationFactorOpt)
      val partitions = opts.options.valueOf(opts.partitionsOpt).intValue
      val replicas = opts.options.valueOf(opts.replicationFactorOpt).intValue
      AdminUtils.createTopic(zkClient, topic, partitions, replicas, configs)
    }
    println("Created topic \"%s\".".format(topic))
  }
```

#### AdminUtils#createTopic

```scala
  def createTopic(zkClient: ZkClient,
                  topic: String,
                  partitions: Int, 
                  replicationFactor: Int, 
                  topicConfig: Properties = new Properties): Unit = {
    // 获取排序后的 brokerId 列表
    val brokerList = ZkUtils.getSortedBrokerList(zkClient)
    // 使用特定的算法，将replica放到不同的broker上
    val replicaAssignment = AdminUtils.assignReplicasToBrokers(brokerList, partitions, replicationFactor)
    // 根据我们刚才分配好的brokerId->[replicas0, replicas1, ...]在zookeeper上注册节点
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic, replicaAssignment, topicConfig)
  }
```

#### AdminUtils#assignReplicasToBrokers

> assignReplicasToBrokers 有两个诉求：
>
> 1. 均匀的将replicas分配到所有的broker；
> 2. 对于分布到某个broker的replica，其他的replica应该尽量分配到其他的broker上；
>
> 实际算法是这样的：
>
> 1. 随机选取一台broker，将partition以round-robin的方式递增的去分配到broker列表；
> 2. 选中在上一次分配中选中的broker的下一台broker（incresing shift），并重复步骤<1>；
> 3. 如果还有剩余的replica，重复执行步骤<2>；
>
> ```
> * Here is an example of assigning
> * broker-0  broker-1  broker-2  broker-3  broker-4
> * p0        p1        p2        p3        p4       (1st replica)
> * p5        p6        p7        p8        p9       (1st replica)
> * p4        p0        p1        p2        p3       (2nd replica)
> * p8        p9        p5        p6        p7       (2nd replica)
> * p3        p4        p0        p1        p2       (3nd replica)
> * p7        p8        p9        p5        p6       (3nd replica)
> ```

```scala
  /**
   * There are 2 goals of replica assignment:
   * 1. Spread the replicas evenly among brokers.
   * 2. For partitions assigned to a particular broker, their other replicas are spread over the other brokers.
   *
   * To achieve this goal, we:
   * 1. Assign the first replica of each partition by round-robin, starting from a random position in the broker list.
   * 2. Assign the remaining replicas of each partition with an increasing shift.
   *
   * Here is an example of assigning
   * broker-0  broker-1  broker-2  broker-3  broker-4
   * p0        p1        p2        p3        p4       (1st replica)
   * p5        p6        p7        p8        p9       (1st replica)
   * p4        p0        p1        p2        p3       (2nd replica)
   * p8        p9        p5        p6        p7       (2nd replica)
   * p3        p4        p0        p1        p2       (3nd replica)
   * p7        p8        p9        p5        p6       (3nd replica)
   */
  def assignReplicasToBrokers(brokerList: Seq[Int],
                              nPartitions: Int,
                              replicationFactor: Int,
                              fixedStartIndex: Int = -1,
                              startPartitionId: Int = -1)
  : Map[Int, Seq[Int]] = {
    if (nPartitions <= 0)
      throw new AdminOperationException("number of partitions must be larger than 0")
    if (replicationFactor <= 0)
      throw new AdminOperationException("replication factor must be larger than 0")
    if (replicationFactor > brokerList.size)
      throw new AdminOperationException("replication factor: " + replicationFactor +
        " larger than available brokers: " + brokerList.size)
    val ret = new mutable.HashMap[Int, List[Int]]()

    // 参数 fixedStartIndex 和 startPartitionId 是为了在 AdminUtils#addPartition 中能够正确的分配replica
    // fixedStartIndex  -> existingReplicaList.head
    // startPartitionId -> existingPartitionsReplicaList.size

    // 选中一台broker作为开始
    val startIndex = if (fixedStartIndex >= 0) fixedStartIndex else rand.nextInt(brokerList.size)
    var currentPartitionId = if (startPartitionId >= 0) startPartitionId else 0
    var nextReplicaShift = if (fixedStartIndex >= 0) fixedStartIndex else rand.nextInt(brokerList.size)
    for (i <- 0 until nPartitions) {
      if (currentPartitionId > 0 && (currentPartitionId % brokerList.size == 0))
        nextReplicaShift += 1
      val firstReplicaIndex = (currentPartitionId + startIndex) % brokerList.size
      var replicaList = List(brokerList(firstReplicaIndex))
      for (j <- 0 until replicationFactor - 1)
        replicaList ::= brokerList(replicaIndex(firstReplicaIndex, nextReplicaShift, j, brokerList.size))
      ret.put(currentPartitionId, replicaList.reverse)
      currentPartitionId = currentPartitionId + 1
    }
    ret.toMap
  }

```

#### AdminUtils#createOrUpdateTopicPartitionAssignmentPathInZK

> 随后，我们将 topic 相关的配置信息写入到 `/brokers/topics/[topic]` 下， `KafkaController` 在启动的时候在这个path上注册了监听函数 `AddPartitionsListener`，写入的信息大概形式如下：
>
> ```json
> {
>     "version": 1,
>     "partitions": {
>         "0": [
>             0
>         ],
>         "1": [
>             0
>         ]
>     }
> }
> ```

```scala
  def createOrUpdateTopicPartitionAssignmentPathInZK(zkClient: ZkClient,
                                                     topic: String,
                                                     partitionReplicaAssignment: Map[Int, Seq[Int]],
                                                     config: Properties = new Properties,
                                                     update: Boolean = false): Unit = {
    // validate arguments
    Topic.validate(topic)
    LogConfig.validate(config)
    // 遍历 partitionReplicaAssignment 的 values，并转换为 value.size()，随后将所有的 value.size() 放到一个 set 中
    // 如果所有的 partition 都有相同的副本数，那么 set.size == 1
    require(partitionReplicaAssignment.values.map(_.size).toSet.size == 1, "All partitions should have the same number of replicas.")

    // /brokers/topics/[topic]
    val topicPath = ZkUtils.getTopicPath(topic)
    if(!update && zkClient.exists(topicPath))
      throw new TopicExistsException("Topic \"%s\" already exists.".format(topic))
    // 不能存在相同的分区
    partitionReplicaAssignment.values.foreach(reps => require(reps.size == reps.toSet.size, "Duplicate replica assignment found: "  + partitionReplicaAssignment))
    
    // 将配置文件写入到 /config/topics/[topic] 内
    writeTopicConfig(zkClient, topic, config)
    
    // 将 [topic] 相关的 partition 信息写入到节点 /brokers/topics/[topic] 中
    // get /brokers/topics/test
    // {"version":1,"partitions":{"12":[0],"8":[0],"19":[0],"4":[0],"15":[0],"11":[0],"9":[0],"13":[0],"16":[0],"5":[0],"10":[0],"21":[0],"6":[0],"1":[0],"17":[0],"14":[0],"0":[0],"20":[0],"2":[0],"18":[0],"7":[0],"3":[0]}}
    writeTopicPartitionAssignment(zkClient, topic, partitionReplicaAssignment, update)
  }
```

#### PartitionStateMachine#TopicChangeListener

> TopicChangeListener 监听了 `/brokers/topics` 的子目录变更，他会负责处理创建topic，删除topic的变更。
>
> 可以看到 TopicChangeListener 主要负责几个事情：
>
> 1. 响应创建/删除topic事件；
> 2. 维护ControllerContext 内的 RA 信息；

```scala
  /**
   * This is the zookeeper listener that triggers all the state transitions for a partition
   */
  class TopicChangeListener extends IZkChildListener with Logging {
    this.logIdent = "[TopicChangeListener on Controller " + controller.config.brokerId + "]: "

    @throws(classOf[Exception])
    def handleChildChange(parentPath : String, children : java.util.List[String]): Unit = {
      inLock(controllerContext.controllerLock) {
        if (hasStarted.get) {
          try {
            // 将 /brokers/topics 的子目录转换成 set
            val currentChildren = {
              import JavaConversions._
              debug("Topic change listener fired for path %s with children %s".format(parentPath, children.mkString(",")))
              (children: Buffer[String]).toSet
            }
            // 得到新建的topic
            val newTopics = currentChildren -- controllerContext.allTopics
            // 得到删除的topic
            val deletedTopics = controllerContext.allTopics -- currentChildren
            // 修改当前的所有topics
            controllerContext.allTopics = currentChildren

            val addedPartitionReplicaAssignment = ZkUtils.getReplicaAssignmentForTopics(zkClient, newTopics.toSeq)
            // 在RA中删除deletedTopics
            controllerContext.partitionReplicaAssignment = controllerContext.partitionReplicaAssignment.filter(p =>
              !deletedTopics.contains(p._1.topic))
            // 在RA中添加新创建的topics对应的RA
            controllerContext.partitionReplicaAssignment.++=(addedPartitionReplicaAssignment)
            info("New topics: [%s], deleted topics: [%s], new partition replica assignment [%s]".format(newTopics,
              deletedTopics, addedPartitionReplicaAssignment))
            if(newTopics.nonEmpty) {
              // 创建新的topic
              controller.onNewTopicCreation(newTopics, addedPartitionReplicaAssignment.keySet.toSet)
            }
          } catch {
            case e: Throwable => error("Error while handling new topic", e )
          }
        }
      }
    }
  }
```

#### PartitionStateMachine#onTopicCreation

```scala
  /**
   * This callback is invoked by the partition state machine's topic change listener with the list of new topics
   * and partitions as input. It does the following -
   * 1. Registers partition change listener. This is not required until KAFKA-347
   * 2. Invokes the new partition callback
   * 3. Send metadata request with the new topic to all brokers so they allow requests for that topic to be served
   */
  def onNewTopicCreation(topics: Set[String], newPartitions: Set[TopicAndPartition]): Unit = {
    info("New topic creation callback for %s".format(newPartitions.mkString(",")))
    // subscribe to partition changes
    topics.foreach(topic => partitionStateMachine.registerPartitionChangeListener(topic))
    onNewPartitionCreation(newPartitions)
  }
```

#### PartitionStateMachine#TopicChangeListener

> AddPartitionsListener 监听了 `/brokers/topics/[topic]`，并且执行了创建新分区的操作。

```scala
  class AddPartitionsListener(topic: String) extends IZkDataListener with Logging {

    this.logIdent = "[AddPartitionsListener on " + controller.config.brokerId + "]: "

    @throws(classOf[Exception])
    def handleDataChange(dataPath : String, data: Object): Unit = {
      inLock(controllerContext.controllerLock) {
        try {
          info("Add Partition triggered " + data.toString + " for path " + dataPath)
          // 读取 /brokers/topics/[topic] 中的数据并解析成 Map[TopicAndPartition, Seq[Int]]
          val partitionReplicaAssignment = ZkUtils.getReplicaAssignmentForTopics(zkClient, List(topic))
          // 如果当前RA已经包含了这个replica，那么直接跳过
          val partitionsToBeAdded = partitionReplicaAssignment.filter(p =>
            !controllerContext.partitionReplicaAssignment.contains(p._1))
          // 如果topic已经被删除了，那么抛出异常
          if(controller.deleteTopicManager.isTopicQueuedUpForDeletion(topic)) {
            error("Skipping adding partitions %s for topic %s since it is currently being deleted"
                  .format(partitionsToBeAdded.map(_._1.partition).mkString(","), topic))
          } else {
            if (partitionsToBeAdded.nonEmpty) {
              info("New partitions to be added %s".format(partitionsToBeAdded))
              // 创建新分区
              controller.onNewPartitionCreation(partitionsToBeAdded.keySet.toSet)
            }
          }
        } catch {
          case e: Throwable => error("Error while handling add partitions for data path " + dataPath, e )
        }
      }
    }

    @throws(classOf[Exception])
    def handleDataDeleted(parentPath : String): Unit = {
      // this is not implemented for partition change
    }
  }
```

#### KafkaController#onNewPartitionCreation

> 最后执行新建partition的操作。

```scala
  /**
   * This callback is invoked by the topic change callback with the list of failed brokers as input.
   * It does the following -
   * 1. Move the newly created partitions to the NewPartition state
   * 2. Move the newly created partitions from NewPartition->OnlinePartition state
   */
  def onNewPartitionCreation(newPartitions: Set[TopicAndPartition]): Unit = {
    info("New partition creation callback for %s".format(newPartitions.mkString(",")))
    partitionStateMachine.handleStateChanges(newPartitions, NewPartition)
    replicaStateMachine.handleStateChanges(controllerContext.replicasForPartition(newPartitions), NewReplica)
    partitionStateMachine.handleStateChanges(newPartitions, OnlinePartition, offlinePartitionSelector)
    replicaStateMachine.handleStateChanges(controllerContext.replicasForPartition(newPartitions), OnlineReplica)
  }
```

### ControllerContext 内部对象的作用

> ControllerContext 内部包含了大量的对象：
>
> - `partitionReplicaAssignment` : **<TopicAndPartition> -> Seq[Int]**，包含了 <TopicAndPartition>  所包含的 AR。
> - `partitionLeadershipInfo`
> - `partitionsBeingReassigned`
> - `partitionsUndergoingPreferredReplicaElection`
> - `liveBrokersUnderlying`
> - `liveBrokerIdsUnderlying`

### ControllerBrokerRequestBatch 的作用是什么？它的 `newBatch` 方法中在不同情况下抛出的异常有什么用？

### ControllerContext#liveBrokersUnderlying 表示的是什么？它在什么时候会变更呢？

> `ControllerContext` 内部包含了很多broker相关的数据，主要包括：
>
> ```scala
> class ControllerContext(val zkClient: ZkClient,
>                         val zkSessionTimeout: Int) {
>     // 已经shutdown的broker的id集合
>   var shuttingDownBrokerIds: mutable.Set[Int] = mutable.Set.empty
>   // 所有可能处于在线状态的broker列表
>   private var liveBrokersUnderlying: Set[Broker] = Set.empty
>   // 所有可能在线状态的broker对应的id列表
>   private var liveBrokerIdsUnderlying: Set[Int] = Set.empty
> }
> ```
>
> 除此之外，还提供了很多的函数通过以上三个Set来获取相应的信息，为了便于理解，我把他们修改成了函数的形式：
>
> ```scala
>   // setter
>   def liveBrokers_=(brokers: Set[Broker]): Unit = {
>     liveBrokersUnderlying = brokers
>     liveBrokerIdsUnderlying = liveBrokersUnderlying.map(_.id)
>   }
> 
>   // getter
>   def liveBrokers: Set[Broker] = {
>     return liveBrokersUnderlying.filter(broker => !shuttingDownBrokerIds.contains(broker.id))
>   }
>   def liveBrokerIds: Set[Int] = {
>     return liveBrokerIdsUnderlying.filter(brokerId => !shuttingDownBrokerIds.contains(brokerId))
>   }
> 
>   def liveOrShuttingDownBrokerIds: Set[Int] = {
>     return liveBrokerIdsUnderlying
>   }
>   def liveOrShuttingDownBrokers: Set[Broker] = {
>     return liveBrokersUnderlying
>   }
> ```
>
> 而为什么是 `underlying` 的呢，首先看看 `setter` 的调用：
>
> - `KafkaController#onControllerFailover` -> `KafkaController#initializeControllerContext`  在 controller 当选时初始化
> - `ReplicaStateMachine#BrokerChangeListener` 在 `/broers/ids` 变更时修改
>
> 而 `shuttingDownBrokerIds` ：
>
> - `KafkaController#onBrokerFailure` -> `controllerContext.shuttingDownBrokerIds.remove(id)`
> - `KafkaController#shutdownBroker` -> `controllerContext.shuttingDownBrokerIds.add(id)`

































