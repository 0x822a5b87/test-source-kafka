# readme

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









































