# readme

## 描述

> 《kafka源码解析与实战》阅读笔记。

## 问题记录

1. kafka 的实际日志由 `OffsetIndex` 和 `FileMessageSet` 两个部分组成，其中 `OffsetIndex` 被映射成了一个 MMap；那么 kafka 怎么保证它和 FileMessageStat 是对得上的呢？

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
>   repositories {
>     // For license plugin.
>     maven {
>       url = 'http://dl.bintray.com/content/netflixoss/external-gradle-plugins/'
>       allowInsecureProtocol = true
>     }
>   }
> }
> ```

## Broker 概述

### 3.1 Broker 的启动

#### bash

```bash
exec $base_dir/kafka-run-class.sh $EXTRA_ARGS kafka.Kafka "$@"
```

#### kafka.Kafka

```scala
object Kafka extends Logging {

  def main(args: Array[String]): Unit = {
    if (args.length != 1) {
      println("USAGE: java [options] %s server.properties".format(classOf[KafkaServer].getSimpleName()))
      System.exit(1)
    }
  
    try {
      val props = Utils.loadProps(args(0))
      val serverConfig = new KafkaConfig(props)
      KafkaMetricsReporter.startReporters(serverConfig.props)
      val kafkaServerStartable = new KafkaServerStartable(serverConfig)

      // attach shutdown handler to catch control-c
      Runtime.getRuntime().addShutdownHook(new Thread() {
        override def run() = {
          kafkaServerStartable.shutdown
        }
      })

      kafkaServerStartable.startup
      kafkaServerStartable.awaitShutdown
    }
    catch {
      case e: Throwable => fatal(e)
    }
    System.exit(0)
  }
}
```

#### KafkaServer#startup()

```scala
  /**
   * Start up API for bringing up a single instance of the Kafka server.
   * Instantiates the LogManager, the SocketServer and the request handlers - KafkaRequestHandlers
   */
  def startup() {
    try {
      info("starting")
      brokerState.newState(Starting)
      isShuttingDown = new AtomicBoolean(false)
      shutdownLatch = new CountDownLatch(1)

      /* start scheduler */
      kafkaScheduler.startup()
    
      /* setup zookeeper */
      zkClient = initZk()

      /* start log manager */
      logManager = createLogManager(zkClient, brokerState)
      logManager.startup()

      socketServer = new SocketServer(config.brokerId,
                                      config.hostName,
                                      config.port,
                                      config.numNetworkThreads,
                                      config.queuedMaxRequests,
                                      config.socketSendBufferBytes,
                                      config.socketReceiveBufferBytes,
                                      config.socketRequestMaxBytes,
                                      config.maxConnectionsPerIp,
                                      config.connectionsMaxIdleMs,
                                      config.maxConnectionsPerIpOverrides)
      socketServer.startup()

      replicaManager = new ReplicaManager(config, time, zkClient, kafkaScheduler, logManager, isShuttingDown)

      /* start offset manager */
      offsetManager = createOffsetManager()

      kafkaController = new KafkaController(config, zkClient, brokerState)
    
      /* start processing requests */
      apis = new KafkaApis(socketServer.requestChannel, replicaManager, offsetManager, zkClient, config.brokerId, config, kafkaController)
      requestHandlerPool = new KafkaRequestHandlerPool(config.brokerId, socketServer.requestChannel, apis, config.numIoThreads)
      brokerState.newState(RunningAsBroker)
   
      Mx4jLoader.maybeLoad()

      replicaManager.startup()

      kafkaController.startup()
    
      topicConfigManager = new TopicConfigManager(zkClient, logManager)
      topicConfigManager.startup()
    
      /* tell everyone we are alive */
      kafkaHealthcheck = new KafkaHealthcheck(config.brokerId, config.advertisedHostName, config.advertisedPort, config.zkSessionTimeoutMs, zkClient)
      kafkaHealthcheck.startup()

    
      registerStats()
      startupComplete.set(true)
      info("started")
    }
    catch {
      case e: Throwable =>
        fatal("Fatal error during KafkaServer startup. Prepare to shutdown", e)
        shutdown()
        throw e
    }
  }
```

| components          | desc                                                         |
| ------------------- | ------------------------------------------------------------ |
| SocketServer        | An NIO socket server, The threading model is : <br /><br />1 Acceptor thread that handles new connections <br />N Processor threads that each have their own selector and read requests from sockets <br />M Handler threads that handle requests and produce responses back to the processor threads for writing. |
| KafkaRequestHandler | A thread that answers kafka requests.                        |
| LogManager          | The entry point to the kafka log management subsystem. The log manager is responsible for log creation, retrieval, and cleaning. All read and write operations are delegated to the individual log instances.<br/>The log manager maintains logs in one or more directories. New logs are created in the data directory with the fewest logs. No attempt is made to move partitions after the fact or balance based on size or I/O rate.<br/>A background thread handles log retention by periodically truncating excess log segments |
| ReplicaManager      |                                                              |
| OffsetManagerConfig | Configuration settings for in-built offset management        |
| KafkaScheduler      | A scheduler based on java.util.concurrent.ScheduledThreadPoolExecutor<br/>It has a pool of kafka-scheduler- threads that do the actual work. |
| KafkaApis           | Logic to handle the various Kafka requests                   |
| KafkaHealthcheck    | This class registers the broker in zookeeper to allow other brokers and consumers to detect failures. It uses an ephemeral znode with the path: /brokers/[0...N] --> advertisedHost:advertisedPort<br/>Right now our definition of health is fairly naive. If we register in zk we are healthy, otherwise we are dead. |
| TopicConfigManager  | This class initiates and carries out topic config changes.<br/>It works as follows.<br/>Config is stored under the path `/brokers/topics/<topic_name>/config` This znode stores the topic-overrides for this topic (but no defaults) in properties format. To avoid watching all topics for changes instead we have a notification path `/brokers/config_changes` The TopicConfigManager has a child watch on this path. To update a topic config we first update the topic config properties. Then we create a new sequential znode under the change path which contains the name of the topic that was updated, say /brokers/config_changes/config_change_13321 This is just a notification--the actual config change is stored only once under the /brokers/topics/ /config path. This will fire a watcher on all brokers. This watcher works as follows. It reads all the config change notifications. It keeps track of the highest config change suffix number it has applied previously. For any previously applied change it finds it checks if this notification is larger than a static expiration time (say 10mins) and if so it deletes this notification. For any new changes it reads the new configuration, combines it with the defaults, and updates the log config for all logs for that topic (if any) that it has. Note that config is always read from the config path in zk, the notification is just a trigger to do so. So if a broker is down and misses a change that is fine--when it restarts it will be loading the full config anyway. Note also that if there are two consecutive config changes it is possible that only the last one will be applied (since by the time the broker reads the config the both changes may have been made). In this case the broker would needlessly refresh the config twice, but that is harmless. On restart the config manager re-processes all notifications. This will usually be wasted work, but avoids any race conditions on startup where a change might be missed between the initial config load and registering for change notifications. |
| KafkaController     |                                                              |

### 4. Broker 的基本模块

#### 4.1 SocketServer

```scala
class SocketServer(val brokerId: Int,
                   val host: String,
                   val port: Int,
                   val numProcessorThreads: Int,
                   val maxQueuedRequests: Int,
                   val sendBufferSize: Int,
                   val recvBufferSize: Int,
                   val maxRequestSize: Int = Int.MaxValue,
                   val maxConnectionsPerIp: Int = Int.MaxValue,
                   val connectionsMaxIdleMs: Long,
                   val maxConnectionsPerIpOverrides: Map[String, Int] ) extends Logging with KafkaMetricsGroup {
  // ...
  private val processors = new Array[Processor](numProcessorThreads)
  @volatile private var acceptor: Acceptor = null
  val requestChannel = new RequestChannel(numProcessorThreads, maxQueuedRequests)

  /**
   * Start the socket server
   */
  def startup() {

  }

  /**
   * Shutdown the socket server
   */
  def shutdown() = {

  }
}
```

#### Acceptor

```scala
  /**
   * Accept loop that checks for new connection attempts
   */
  def run() {
    // NIO 注册时间接收连接请求
    serverChannel.register(selector, SelectionKey.OP_ACCEPT)
    startupComplete()
    var currentProcessor = 0
    while(isRunning) {
      // 查看新增的连接请求
      val ready = selector.select(500)
      if(ready > 0) {
        val keys = selector.selectedKeys()
        val iter = keys.iterator()
        while(iter.hasNext && isRunning) {
          var key: SelectionKey = null
          try {
            key = iter.next
            iter.remove()
            if(key.isAcceptable) {
               // 基于轮序获取一个处理线程
               accept(key, processors(currentProcessor))
            } else
               throw new IllegalStateException("Unrecognized key state for acceptor thread.")

            // round robin to the next processor thread
            currentProcessor = (currentProcessor + 1) % processors.length
          } catch {
            case e: Throwable => error("Error while accepting connection", e)
          }
        }
      }
    }
    // 退出 kafka，清理资源
    debug("Closing server socket and selector.")
    swallowError(serverChannel.close())
    swallowError(selector.close())
    shutdownComplete()
  }
```

#### Processor

> `newConnections` 保存了由 `Acceptor` 线程转移过来的 `SocketChannel` 对象。
>
> 1. 在 `configureNewConnections()` 方法中，我们针对 `SocketChannel` 对象注册了 `OP_READ` 方法以接收客户端的请求；
> 2. 在 `processNewResponses()` 方法中，我们从 `RequestChannnel` 中获取对应客户端请求的响应产生对应的事件；
> 3. 在 `selector.select(300)` 返回的连接中监听事件：
>    1. 如果是 `OP_READ`，说明是新的 request，将请求添加到 RequestChannel 中；
>    2. 如果是 `OP_WRITE`，说明需要通过 `RequestChannel.Response` 写入，可能是各种返回值
>    3. 如果是 valid，说明连接已经关闭。

```scala
/**
 * Thread that processes all requests from a single connection. There are N of these running in parallel
 * each of which has its own selectors
 */
private[kafka] class Processor(val id: Int,
                               val time: Time,
                               val maxRequestSize: Int,
                               val aggregateIdleMeter: Meter,
                               val idleMeter: Meter,
                               val totalProcessorThreads: Int,
                               val requestChannel: RequestChannel,
                               connectionQuotas: ConnectionQuotas,
                               val connectionsMaxIdleMs: Long) extends AbstractServerThread(connectionQuotas) {

  private val newConnections = new ConcurrentLinkedQueue[SocketChannel]()
  private val connectionsMaxIdleNanos = connectionsMaxIdleMs * 1000 * 1000
  private var currentTimeNanos = SystemTime.nanoseconds
  private val lruConnections = new util.LinkedHashMap[SelectionKey, Long]
  private var nextIdleCloseCheckTime = currentTimeNanos + connectionsMaxIdleNanos

  override def run() {
    startupComplete()
    while(isRunning) {
      // setup any new connections that have been queued up
      // 在 accept 方法中，会把所有的 connection 放到一个队列中
      configureNewConnections()
      // 从 RequestChannel 中获取响应并根据 ResponseAction 类型返回
      processNewResponses()
      val startSelectTime = SystemTime.nanoseconds
      val ready = selector.select(300)
      currentTimeNanos = SystemTime.nanoseconds
      val idleTime = currentTimeNanos - startSelectTime
      // 标记工作线程 selector 阻塞时间
      idleMeter.mark(idleTime)
      // We use a single meter for aggregate idle percentage for the thread pool.
      // Since meter is calculated as total_recorded_value / time_window and
      // time_window is independent of the number of threads, each recorded idle
      // time should be discounted by # threads.
      aggregateIdleMeter.mark(idleTime / totalProcessorThreads)

      trace("Processor id " + id + " selection time = " + idleTime + " ns")
      if(ready > 0) {
        val keys = selector.selectedKeys()
        val iter = keys.iterator()
        while(iter.hasNext && isRunning) {
          var key: SelectionKey = null
          try {
            key = iter.next
            iter.remove()
            if(key.isReadable)
              read(key)
            else if(key.isWritable)
              write(key)
            else if(!key.isValid)
              close(key)
            else
              throw new IllegalStateException("Unrecognized key state for processor thread.")
          } catch {
            case e: EOFException => {
              info("Closing socket connection to %s.".format(channelFor(key).socket.getInetAddress))
              close(key)
            } case e: InvalidRequestException => {
              info("Closing socket connection to %s due to invalid request: %s".format(channelFor(key).socket.getInetAddress, e.getMessage))
              close(key)
            } case e: Throwable => {
              error("Closing socket for " + channelFor(key).socket.getInetAddress + " because of error", e)
              close(key)
            }
          }
        }
      }
      maybeCloseOldestConnection
    }
    debug("Closing selector.")
    closeAll()
    swallowError(selector.close())
    shutdownComplete()
  }

  /**
   * Close the given key and associated socket
   */
  override def close(key: SelectionKey): Unit = {
    lruConnections.remove(key)
    super.close(key)
  }

  private def processNewResponses() {
    // requestChannel 的类型是 RequestChannel，他在 sendRequest 阶段
    // 添加了 RequestChannel.Request(processor = id, requestKey = key, buffer = receive.buffer, startTimeMs = time.milliseconds, remoteAddress = address)
    // 到队列
    var curr = requestChannel.receiveResponse(id)
    while(curr != null) {
      val key = curr.request.requestKey.asInstanceOf[SelectionKey]
      try {
        curr.responseAction match {
          case RequestChannel.NoOpAction => {
            // There is no response to send to the client, we need to read more pipelined requests
            // that are sitting in the server's socket buffer
            curr.request.updateRequestMetrics
            trace("Socket server received empty response to send, registering for read: " + curr)
            key.interestOps(SelectionKey.OP_READ)
            key.attach(null)
          }
          case RequestChannel.SendAction => {
            trace("Socket server received response to send, registering for write: " + curr)
            key.interestOps(SelectionKey.OP_WRITE)
            key.attach(curr)
          }
          case RequestChannel.CloseConnectionAction => {
            curr.request.updateRequestMetrics
            trace("Closing socket connection actively according to the response code.")
            close(key)
          }
          case responseCode => throw new KafkaException("No mapping found for response code " + responseCode)
        }
      } catch {
        case e: CancelledKeyException => {
          debug("Ignoring response for closed socket.")
          close(key)
        }
      } finally {
        curr = requestChannel.receiveResponse(id)
      }
    }
  }

  /**
   * Queue up a new connection for reading
   */
  def accept(socketChannel: SocketChannel) {
    newConnections.add(socketChannel)
    wakeup()
  }

  /**
   * Register any new connections that have been queued up
   */
  private def configureNewConnections() {
    while(newConnections.size() > 0) {
      val channel = newConnections.poll()
      debug("Processor " + id + " listening to new connection from " + channel.socket.getRemoteSocketAddress)
      // 注意，这个 selector 是 Processor 自己的 selector
      channel.register(selector, SelectionKey.OP_READ)
    }
  }

  /*
   * Process reads from ready sockets
   */
  def read(key: SelectionKey) {
  }

  /*
   * Process writes to ready sockets
   */
  def write(key: SelectionKey) {

  }
}
```

> `maybeCloseOldestConnection` 在每次循环中都会处理一下 LRU 的连接，并且清理空闲时间过久的链接。

```scala
  private def maybeCloseOldestConnection {
    if(currentTimeNanos > nextIdleCloseCheckTime) {
      if(lruConnections.isEmpty) {
        nextIdleCloseCheckTime = currentTimeNanos + connectionsMaxIdleNanos
      } else {
        val oldestConnectionEntry = lruConnections.entrySet.iterator().next()
        val connectionLastActiveTime = oldestConnectionEntry.getValue
        nextIdleCloseCheckTime = connectionLastActiveTime + connectionsMaxIdleNanos
        if(currentTimeNanos > nextIdleCloseCheckTime) {
          val key: SelectionKey = oldestConnectionEntry.getKey
          trace("About to close the idle connection from " + key.channel.asInstanceOf[SocketChannel].socket.getRemoteSocketAddress
            + " due to being idle for " + (currentTimeNanos - connectionLastActiveTime) / 1000 / 1000 + " millis")
          close(key)
        }
      }
    }
  }
```

#### RequestChannel

>`RequestChannel` 内部包含两个阻塞队列，用于解耦 `SocketServer` 和 `KafkaApis`：
>
>- SocketServer 收到请求后放到 requestQueue 中
>- KafkaApis 从 requestQueue 中获取请求，并生成相应，写入到 responseQueue 中

```scala
class RequestChannel(val numProcessors: Int, val queueSize: Int) extends KafkaMetricsGroup {
  private var responseListeners: List[(Int) => Unit] = Nil
  private val requestQueue = new ArrayBlockingQueue[RequestChannel.Request](queueSize)
  private val responseQueues = new Array[BlockingQueue[RequestChannel.Response]](numProcessors)
  for(i <- 0 until numProcessors)
    responseQueues(i) = new LinkedBlockingQueue[RequestChannel.Response]()
}
```

### 4.2 KafkaRequestHandlerPool

>`KafkaRequestHandlerPool` 单纯的对 `KafkaRequestHandler` 进行了一个包装，将 KafkaRequestHandler 作为一个 daemon thread。

```scala
class KafkaRequestHandlerPool(val brokerId: Int,
                              val requestChannel: RequestChannel,
                              val apis: KafkaApis,
                              numThreads: Int) extends Logging with KafkaMetricsGroup {
  // runnable 是 KafkaRequestHandler，而 threads 包含了 runnable 的守护线程
  val threads = new Array[Thread](numThreads)
  val runnables = new Array[KafkaRequestHandler](numThreads)
  for(i <- 0 until numThreads) {
    runnables(i) = new KafkaRequestHandler(i, brokerId, aggregateIdleMeter, numThreads, requestChannel, apis)
    threads(i) = Utils.daemonThread("kafka-request-handler-" + i, runnables(i))
    threads(i).start()
  }

  def shutdown() {

  }
}
```

### 4.3 KafkaApis

#### 4.3.1 LogManager

> LogManager 负责提供kafka的 topic 日志的读取和写入功能，负责 **读取和写入位于 broker 上的所有分区的副本数据**；
>
> LogManager 管理是以 broker 为维度，而不是以 topic 或者 partition 作为维度。也就是，LogManager 只会管理 **它所在的 broker 上的所有 partition**。

##### 4.3.1.1 Kafka 的日志组成

> `kafka` 的日志本质上是一个 append-only 的文件，同时具有 `topic + partition` 的属性，所以 `LogManager` 将实际的日志抽象成了一个 `TopicAndPartitition` -> `Log` 的结构。

```scala
@threadsafe
class LogManager(val logDirs: Array[File],
                 val topicConfigs: Map[String, LogConfig],
                 val defaultConfig: LogConfig,
                 val cleanerConfig: CleanerConfig,
                 ioThreads: Int,
                 val flushCheckMs: Long,
                 val flushCheckpointMs: Long,
                 val retentionCheckMs: Long,
                 scheduler: Scheduler,
                 val brokerState: BrokerState,
                 private val time: Time) extends Logging {

  private val logs = new Pool[TopicAndPartition, Log]()
}
```

> 1. `Log` 的实现是基于 `ConcurrentNavigableMap[Long, LogSegment]`，因为每一个 `topic + partition` 对应一个 `log`，而一个 `log` 对应多个 `LogSegment`；
> 2. Log 的实现是基于跳跃表，使用跳跃表的原因是，可以比较简单的基于 `subMap` 来修改引用的 LogSegment 范围；

```scala
@threadsafe
class Log(val dir: File,
          @volatile var config: LogConfig,
          @volatile var recoveryPoint: Long = 0L,
          scheduler: Scheduler,
          time: Time = SystemTime) extends Logging with KafkaMetricsGroup {

  /* the actual segments of the log */
  private val segments: ConcurrentNavigableMap[java.lang.Long, LogSegment] = new ConcurrentSkipListMap[java.lang.Long, LogSegment]
  
}
```

![LogSegment and Log](source/LogSegment and Log.png)

> LogSegment ：**Each segment has two components: a log and an index.**

```scala
 /**
 * A segment of the log. Each segment has two components: a log and an index. The log is a FileMessageSet containing
 * the actual messages. The index is an OffsetIndex that maps from logical offsets to physical file positions. Each 
 * segment has a base offset which is an offset <= the least offset of any message in this segment and > any offset in
 * any previous segment.
 * 
 * A segment with a base offset of [base_offset] would be stored in two files, a [base_offset].index and a [base_offset].log file. 
 * 
 * @param log The message set containing log entries
 * @param index The offset index
 * @param baseOffset A lower bound on the offsets in this segment
 * @param indexIntervalBytes The approximate number of bytes between entries in the index
 * @param time The time instance
 */
@nonthreadsafe
class LogSegment(val log: FileMessageSet, 
                 val index: OffsetIndex, 
                 val baseOffset: Long, 
                 // 索引粒度，代表多少字节之后生成一条索引
                 val indexIntervalBytes: Int,
                 val rollJitterMs: Long,
                 time: Time) extends Logging {


}
```

> 1. 一个 Kafka 包含了多个 topic
> 2. 一个 topic 包含了多个 partition
> 3. 一个 LogManager 管理一台 broker 上的所有 `Log`
> 4. 由 <topic, partition> 可以定位到一个唯一的 `Log`
> 5. 一个 `Log` 包含了多个不同的 `LogSegment`
> 6. 一个 `LogSegment` 包含了 `FileMessageSet` 和 `OffsetIndex` 分别用于数据存储以及索引；
> 7. `OffsetIndex` 包含了一个 baseOffset，并且每经过 indexIntervalBytes 字节会建立一个索引；例如在下面的例子中，`3, 497` 指明了 **offset 为 368769 + 3 的 message 在 OffsetIndex 对应的 FileMessageSet 的第 497 个字节** 

![FileMessageSet and OffsetIndex](source/FileMessageSet and OffsetIndex.png)

##### 4.3.1.2 kafka 的消息读取

```scala
  /**
   * Read messages from the log
   *
   * @param startOffset The offset to begin reading at
   * @param maxLength The maximum number of bytes to read
   * @param maxOffset -The offset to read up to, exclusive. (i.e. the first offset NOT included in the resulting message set).
   * 
   * @throws OffsetOutOfRangeException If startOffset is beyond the log end offset or before the base offset of the first segment.
   * @return The fetch data information including fetch starting offset metadata and messages read
   */
  def read(startOffset: Long, maxLength: Int, maxOffset: Option[Long] = None): FetchDataInfo = {
    trace("Reading %d bytes from offset %d in log %s of length %d bytes".format(maxLength, startOffset, name, size))

    // check if the offset is valid and in range
    val next = nextOffsetMetadata.messageOffset
    // 如果已经是当前最大的 offset，那么无数据读取
    if(startOffset == next)
      return FetchDataInfo(nextOffsetMetadata, MessageSet.Empty)

    // 查找具体在哪个 LogSegment，这就是为什么 segments 的底层实现是一个跳跃表
    var entry = segments.floorEntry(startOffset)
      
    // attempt to read beyond the log end offset is an error
    if(startOffset > next || entry == null)
      throw new OffsetOutOfRangeException("Request for offset %d but we only have log segments in the range %d to %d.".format(startOffset, segments.firstKey, next))

    // 尝试从 LogSegment 中读取数据，如果当前 LogSegment 没有我们要读的数据
    // 就找到下一个 entry 尝试读取数据，直到读到数据或者没有最新的 LogSegment
    while(entry != null) {
      // entry.getValue 是我们刚才基于 startOffset 找到的 LogSegment
      val fetchInfo = entry.getValue.read(startOffset, maxOffset, maxLength)
      if(fetchInfo == null) {
        entry = segments.higherEntry(entry.getKey)
      } else {
        return fetchInfo
      }
    }
    
    // 我们已经查找到最后的 Segment，但是我们还是无法读取到数据，虽然给定的 startOffset 是在我们 LogSegment 的范围中。
    // 当所有偏移量大于起始偏移量的消息都被删除时，就会发生这种情况。
    FetchDataInfo(nextOffsetMetadata, MessageSet.Empty)
  }
```

> LogSegment 的 read 方法，包含了三个参数：
>
> 1. `startOffset` 日志的 offset
> 2. `maxOffset` 最大读取多少条消息，**maxOffset 最大的作用是，可以用来实现 `watermark`，比如我们读数据的时候可能是读的 partition 的 leader 或者 slaver，在一些策略下我们可能不允许对 offset 进行限制。**
> 3. `maxSize` 最大读取多少 byte
>
> 这个方法主要是执行以下操作：
>
> 1. 根据 `startOffset` 查找 OffsetIndex 得到 OffsetPosition，这样我们知道日志应该从 FileMessageSet 的哪里开始读；
> 2. 根据 `maxOffset` 和 `maxSize` 判断我们所能读取的最大数据长度；

```scala
  /**
   * 从 LogSegment 读取第一条满足 offset >= startOffset 的消息。
   * 消息不会超过 maxSize 指定的 bytes，并且会在 maxOffset 之前结束（如果指定了 maxOffset 的话）
   *
   * @param startOffset A lower bound on the first offset to include in the message set we read
   * @param maxSize The maximum number of bytes to include in the message set we read
   * @param maxOffset An optional maximum offset for the message set we read
   * 
   * @return The fetched data and the offset metadata of the first message whose offset is >= startOffset,
   *         or null if the startOffset is larger than the largest offset in this log
   */
  @threadsafe
  def read(startOffset: Long, maxOffset: Option[Long], maxSize: Int): FetchDataInfo = {
    if(maxSize < 0)
      throw new IllegalArgumentException("Invalid max size for log read (%d)".format(maxSize))

    val logSize = log.sizeInBytes() // this may change, need to save a consistent copy
    // 通过查找 OffsetIndex 来查找 OffsetPosition，这里包含了消息的 offset 以及在对应的 LogSegment 的字节偏移量
    val startPosition = translateOffset(startOffset)

    // if the start position is already off the end of the log, return null
    if(startPosition == null)
      return null

    // LogOffsetMetadata 包含了消息的 offset，LogSegment 的 baseOffset，以及在 LogSegment 的字节偏移量
    val offsetMetadata = new LogOffsetMetadata(startOffset, this.baseOffset, startPosition.position)

    // if the size is zero, still return a log segment but with zero size
    if(maxSize == 0)
      return FetchDataInfo(offsetMetadata, MessageSet.Empty)

    // 计算可以读取的最大长度，这个由 maxOffset，maxSize 共同限定。
    // 例如，我们开启了某些策略的时候，虽然部分日志已经写入到 LogSegment，但是仍然不允许他们被消费，这个时候我们需要对 maxOffset 进行限制。
    // 如果没有指定 maxOffset，那么我们可以直接使用 maxSize 作为 length
    // 如果指定了 maxOffset，那么我们需要找到 maxOffset 对应的日志的 OffsetPosition
    // 那么进而可以计算得到我们需要读取的数据长度了。
    val length =
      maxOffset match {
        case None =>
          maxSize
        case Some(offset) => {
          if(offset < startOffset)
            throw new IllegalArgumentException("Attempt to read with a maximum offset (%d) less than the start offset (%d).".format(offset, startOffset))
          val mapping = translateOffset(offset, startPosition.position)
          val endPosition =
            if(mapping == null)
              logSize // the max offset is off the end of the log, use the end of the file
            else
              mapping.position
          min(endPosition - startPosition.position, maxSize)
        }
      }
    FetchDataInfo(offsetMetadata, log.read(startPosition.position, length))
  }
```

> `translateOffset`：接收两个参数
>
> 1. `offset` 消息在 partition 的 offset，等于 LogSegment 的 baseOffset + relativeOffset；
> 2. `startingFilePosition` 是一个优化项，在某些情况下我们已经知道了我们搜索的 offset 一定是在 LogSegment 的某个 position 之后，就可以通过这个选项来指定偏移量来减少查询次数。
>
> 搜索并返回 offset 对应的 OffsetPosition。

```scala
  /**
   * Find the physical file position for the first message with offset >= the requested offset.
   * 
   * The lowerBound argument is an optimization that can be used if we already know a valid starting position
   * in the file higher than the greatest-lower-bound from the index.
   * 
   * @param offset The offset we want to translate
   * @param startingFilePosition A lower bound on the file position from which to begin the search. This is purely an optimization and
   * when omitted, the search will begin at the position in the offset index.
   * 
   * @return The position in the log storing the message with the least offset >= the requested offset or null if no message meets this criteria.
   */
  @threadsafe
  private[log] def translateOffset(offset: Long, startingFilePosition: Int = 0): OffsetPosition = {
    // 通过offset在 OffsetIndex 中查找第一条 offset >= targetOffset 的消息。
    val mapping = index.lookup(offset)
    // 从 startingFilePosition 指定的文件位置开始搜索，查找某条消息的物理位置，该消息是最后一条 offset >= targetOffset 的消息，并返回该消息的物理位置
    log.searchFor(offset, max(mapping.position, startingFilePosition))
  }

```

>`translateOffset` 分为两个阶段：
>
>1. 二分查找；
>2. 查找文件物理位置。

>translateOffset 二分查找：这里需要关注的是 `relativeOffset` 和 `physical` 两个方法
>
>```scala
>  /* return the nth offset relative to the base offset */
>  private def relativeOffset(buffer: ByteBuffer, n: Int): Int = buffer.getInt(n * 8)
>  
>  /* return the nth physical position */
>  private def physical(buffer: ByteBuffer, n: Int): Int = buffer.getInt(n * 8 + 4)
>```
>
>再回忆一下，OffsetIndex 的数据结构：**[offset0, position0, offset1, position1,...]**，就知道为什么是这样获取相对offset以及物理position了。
>
>**需要注意的是，lookup 方法返回的并不一定是我们查找的那条日志的offset与position，因为 OffsetIndex 是稀疏索引**

```scala
  /**
   * Find the largest offset less than or equal to the given targetOffset 
   * and return a pair holding this offset and it's corresponding physical file position.
   * 
   * @param targetOffset The offset to look up.
   * 
   * @return The offset found and the corresponding file position for this offset. 
   * If the target offset is smaller than the least entry in the index (or the index is empty),
   * the pair (baseOffset, 0) is returned.
   */
  def lookup(targetOffset: Long): OffsetPosition = {
    maybeLock(lock) {
      val idx = mmap.duplicate
      val slot = indexSlotFor(idx, targetOffset)
      if(slot == -1)
        return OffsetPosition(baseOffset, 0)
      else
        return OffsetPosition(baseOffset + relativeOffset(idx, slot), physical(idx, slot))
      }
  }

  /**
   * Find the slot in which the largest offset less than or equal to the given
   * target offset is stored.
   * 
   * @param idx The index buffer
   * @param targetOffset The offset to look for
   * 
   * @return The slot found or -1 if the least entry in the index is larger than the target offset or the index is empty
   */
  private def indexSlotFor(idx: ByteBuffer, targetOffset: Long): Int = {
    
    // we only store the difference from the base offset so calculate that
    val relOffset = targetOffset - baseOffset
    
    // check if the index is empty
    if(entries == 0)
      return -1
    
    // check if the target offset is smaller than the least offset
    if(relativeOffset(idx, 0) > relOffset)
      return -1
      
    // binary search for the entry
    var lo = 0
    var hi = entries-1
    while(lo < hi) {
      val mid = ceil(hi/2.0 + lo/2.0).toInt
      val found = relativeOffset(idx, mid)
      if(found == relOffset)
        return mid
      else if(found < relOffset)
        lo = mid
      else
        hi = mid - 1
    }
    lo
  }

  /* return the nth offset relative to the base offset */
  private def relativeOffset(buffer: ByteBuffer, n: Int): Int = buffer.getInt(n * 8)
  
  /* return the nth physical position */
  private def physical(buffer: ByteBuffer, n: Int): Int = buffer.getInt(n * 8 + 4)
```

> `OffsetIndex`：OffsetIndex 内部包含了一个 MappedByteBuffer，用于索引文件。因为使用 MappedByteBuffer 所以会很占内存。

##### 4.3.1.3 LogManager 的启动

> 在 `LogManager` 下，有一个文件 `recovery-point-offset-checkpoint`，这个文件保存了 `topic/partition=>offsets` 的映射关系。它记录了所有已经 flush 到硬盘的偏移量，这样在我们遇到崩溃重启时不需要从最开始的位置 recovery。
>
> 以 `data_distribution_data_mining` 这个 topic 为例子，在某一台 broker 下存放了三个分区：
>
> ```
> data_distribution_data_mining-0
> data_distribution_data_mining-15
> data_distribution_data_mining-19
> ```
>
> 而实际的 recovery-point-offset-checkpoint 中保存了如下信息
>
> ```
> data_distribution_data_mining 0 xxx
> data_distribution_data_mining 15 yyy
> data_distribution_data_mining 19 zzz
> ```
>
> 表明 topic 的分区 `0`,`15`,`19` 三个分区对应的 offset 是 xxx, yyy, zzz。

###### 初始化 logs

> 在这一步，主要是初始化 `Log`

```scala
/**
 * The entry point to the kafka log management subsystem. The log manager is responsible for log creation, retrieval, and cleaning.
 * All read and write operations are delegated to the individual log instances.
 * 
 * The log manager maintains logs in one or more directories. New logs are created in the data directory
 * with the fewest logs. No attempt is made to move partitions after the fact or balance based on
 * size or I/O rate.
 * 
 * A background thread handles log retention by periodically truncating excess log segments.
 */
@threadsafe
class LogManager(val logDirs: Array[File],
                 val topicConfigs: Map[String, LogConfig],
                 val defaultConfig: LogConfig,
                 val cleanerConfig: CleanerConfig,
                 ioThreads: Int,
                 val flushCheckMs: Long,
                 val flushCheckpointMs: Long,
                 val retentionCheckMs: Long,
                 scheduler: Scheduler,
                 val brokerState: BrokerState,
                 private val time: Time) extends Logging {

  
  val RecoveryPointCheckpointFile = "recovery-point-offset-checkpoint"
  val LockFile = ".lock"
  val InitialTaskDelayMs = 30*1000
  private val logCreationOrDeletionLock = new Object
  // 初始化 Log
  private val logs = new Pool[TopicAndPartition, Log]()

  createAndValidateLogDirs(logDirs)
  private val dirLocks = lockLogDirs(logDirs)
  private val recoveryPointCheckpoints = logDirs.map(dir => (dir, new OffsetCheckpoint(new File(dir, RecoveryPointCheckpointFile)))).toMap
  loadLogs()
}
```

###### 在 `startup` 中开启后台定时线程

> 1. cleanupLogs 清理过期日志；
> 2. flushDirtyLogs 刷新内存中的数据到硬盘；
> 3. checkpointRecoveryPointOffsets 将检查点写入到文件防止在启动的时候恢复整个日志；
> 4. 启动 `cleaner`（cleaner 是一个 `LogCleaner` 对象，用于清理 Key 重复的数据）：对于一个 message 分为 <Topic, Key, Message> 三个部分，当 `<Topic, Key>` 相同的消息出现多次的时候，如果开启了 cleaner，针对 `<Topic, Key>` 相同的消息，只会保留最后一条。

```scala
  /**
   *  Start the background threads to flush logs and do log cleanup
   */
  def startup() {
    /* Schedule the cleanup task to delete old logs */
    if(scheduler != null) {
      info("Starting log cleanup with a period of %d ms.".format(retentionCheckMs))
      scheduler.schedule("kafka-log-retention", 
                         cleanupLogs, 
                         delay = InitialTaskDelayMs, 
                         period = retentionCheckMs, 
                         TimeUnit.MILLISECONDS)
      info("Starting log flusher with a default period of %d ms.".format(flushCheckMs))
      scheduler.schedule("kafka-log-flusher", 
                         flushDirtyLogs, 
                         delay = InitialTaskDelayMs, 
                         period = flushCheckMs, 
                         TimeUnit.MILLISECONDS)
      scheduler.schedule("kafka-recovery-point-checkpoint",
                         checkpointRecoveryPointOffsets,
                         delay = InitialTaskDelayMs,
                         period = flushCheckpointMs,
                         TimeUnit.MILLISECONDS)
    }
    if(cleanerConfig.enableCleaner)
      cleaner.startup()
  }
```



























