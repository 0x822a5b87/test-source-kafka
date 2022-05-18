/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import kafka.api._
import kafka.common._
import kafka.log._
import kafka.message._
import kafka.network._
import kafka.admin.AdminUtils
import kafka.network.RequestChannel.Response
import kafka.controller.KafkaController
import kafka.utils.{ZkUtils, ZKGroupTopicDirs, SystemTime, Logging}

import scala.collection._

import org.I0Itec.zkclient.ZkClient

/**
 * Logic to handle the various Kafka requests
 */
class KafkaApis(val requestChannel: RequestChannel,
                val replicaManager: ReplicaManager,
                val offsetManager: OffsetManager,
                val zkClient: ZkClient,
                val brokerId: Int,
                val config: KafkaConfig,
                val controller: KafkaController) extends Logging {

  val producerRequestPurgatory = new ProducerRequestPurgatory(replicaManager, offsetManager, requestChannel)
  val fetchRequestPurgatory = new FetchRequestPurgatory(replicaManager, requestChannel)
  // TODO: the following line will be removed in 0.9
  replicaManager.initWithRequestPurgatory(producerRequestPurgatory, fetchRequestPurgatory)
  var metadataCache = new MetadataCache
  this.logIdent = "[KafkaApi-%d] ".format(brokerId)

  /**
   * Top-level method that handles all requests and multiplexes to the right api
   */
  def handle(request: RequestChannel.Request) {
    try{
      trace("Handling request: " + request.requestObj + " from client: " + request.remoteAddress)
      request.requestId match {
        case RequestKeys.ProduceKey => handleProducerOrOffsetCommitRequest(request)
        case RequestKeys.FetchKey => handleFetchRequest(request)
        case RequestKeys.OffsetsKey => handleOffsetRequest(request)
        case RequestKeys.MetadataKey => handleTopicMetadataRequest(request)
        case RequestKeys.LeaderAndIsrKey => handleLeaderAndIsrRequest(request)
        case RequestKeys.StopReplicaKey => handleStopReplicaRequest(request)
        case RequestKeys.UpdateMetadataKey => handleUpdateMetadataRequest(request)
        case RequestKeys.ControlledShutdownKey => handleControlledShutdownRequest(request)
        case RequestKeys.OffsetCommitKey => handleOffsetCommitRequest(request)
        case RequestKeys.OffsetFetchKey => handleOffsetFetchRequest(request)
        case RequestKeys.ConsumerMetadataKey => handleConsumerMetadataRequest(request)
        case requestId => throw new KafkaException("Unknown api code " + requestId)
      }
    } catch {
      case e: Throwable =>
        request.requestObj.handleError(e, requestChannel, request)
        error("error when handling request %s".format(request.requestObj), e)
    } finally
      request.apiLocalCompleteTimeMs = SystemTime.milliseconds
  }

  def handleOffsetCommitRequest(request: RequestChannel.Request) {
    val offsetCommitRequest = request.requestObj.asInstanceOf[OffsetCommitRequest]
    if (offsetCommitRequest.versionId == 0) {
      // version 0 stores the offsets in ZK
      val responseInfo = offsetCommitRequest.requestInfo.map{
        case (topicAndPartition, metaAndError) => {
          val topicDirs = new ZKGroupTopicDirs(offsetCommitRequest.groupId, topicAndPartition.topic)
          try {
            ensureTopicExists(topicAndPartition.topic)
            if(metaAndError.metadata != null && metaAndError.metadata.length > config.offsetMetadataMaxSize) {
              (topicAndPartition, ErrorMapping.OffsetMetadataTooLargeCode)
            } else {
              ZkUtils.updatePersistentPath(zkClient, topicDirs.consumerOffsetDir + "/" +
                topicAndPartition.partition, metaAndError.offset.toString)
              (topicAndPartition, ErrorMapping.NoError)
            }
          } catch {
            case e: Throwable => (topicAndPartition, ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]]))
          }
        }
      }
      val response = new OffsetCommitResponse(responseInfo, offsetCommitRequest.correlationId)
      requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(response)))
    } else {
      // version 1 and above store the offsets in a special Kafka topic
      handleProducerOrOffsetCommitRequest(request)
    }
  }

  private def ensureTopicExists(topic: String) = {
    if (metadataCache.getTopicMetadata(Set(topic)).size <= 0)
      throw new UnknownTopicOrPartitionException("Topic " + topic + " either doesn't exist or is in the process of being deleted")
  }

  def handleLeaderAndIsrRequest(request: RequestChannel.Request): Unit = {
    // ensureTopicExists is only for client facing requests
    // We can't have the ensureTopicExists check here since the controller sends it as an advisory to all brokers so they
    // stop serving data to clients for the topic being deleted
    val leaderAndIsrRequest = request.requestObj.asInstanceOf[LeaderAndIsrRequest]
    try {
      val (response, error) = replicaManager.becomeLeaderOrFollower(leaderAndIsrRequest, offsetManager)
      val leaderAndIsrResponse = new LeaderAndIsrResponse(leaderAndIsrRequest.correlationId, response, error)
      requestChannel.sendResponse(new Response(request, new BoundedByteBufferSend(leaderAndIsrResponse)))
    } catch {
      case e: KafkaStorageException =>
        fatal("Disk error during leadership change.", e)
        Runtime.getRuntime.halt(1)
    }
  }

  def handleStopReplicaRequest(request: RequestChannel.Request): Unit = {
    // ensureTopicExists is only for client facing requests
    // We can't have the ensureTopicExists check here since the controller sends it as an advisory to all brokers so they
    // stop serving data to clients for the topic being deleted
    val stopReplicaRequest = request.requestObj.asInstanceOf[StopReplicaRequest]
    // 停止所有 replica
    val (response, error) = replicaManager.stopReplicas(stopReplicaRequest)
    val stopReplicaResponse = new StopReplicaResponse(stopReplicaRequest.correlationId, response.toMap, error)
    requestChannel.sendResponse(new Response(request, new BoundedByteBufferSend(stopReplicaResponse)))
    // 关闭所有 Fetcher 线程
    replicaManager.replicaFetcherManager.shutdownIdleFetcherThreads()
  }

  def handleUpdateMetadataRequest(request: RequestChannel.Request) {
    val updateMetadataRequest = request.requestObj.asInstanceOf[UpdateMetadataRequest]
    replicaManager.maybeUpdateMetadataCache(updateMetadataRequest, metadataCache)

    val updateMetadataResponse = new UpdateMetadataResponse(updateMetadataRequest.correlationId)
    requestChannel.sendResponse(new Response(request, new BoundedByteBufferSend(updateMetadataResponse)))
  }

  def handleControlledShutdownRequest(request: RequestChannel.Request) {
    // ensureTopicExists is only for client facing requests
    // We can't have the ensureTopicExists check here since the controller sends it as an advisory to all brokers so they
    // stop serving data to clients for the topic being deleted
    val controlledShutdownRequest = request.requestObj.asInstanceOf[ControlledShutdownRequest]
    val partitionsRemaining = controller.shutdownBroker(controlledShutdownRequest.brokerId)
    val controlledShutdownResponse = new ControlledShutdownResponse(controlledShutdownRequest.correlationId,
      ErrorMapping.NoError, partitionsRemaining)
    requestChannel.sendResponse(new Response(request, new BoundedByteBufferSend(controlledShutdownResponse)))
  }

  private def producerRequestFromOffsetCommit(offsetCommitRequest: OffsetCommitRequest) = {
    // 过滤过大的offset
    val msgs = offsetCommitRequest.filterLargeMetadata(config.offsetMetadataMaxSize).map {
      case (topicAndPartition, offset) =>
        new Message(
          bytes = OffsetManager.offsetCommitValue(offset),
          key = OffsetManager.offsetCommitKey(offsetCommitRequest.groupId, topicAndPartition.topic, topicAndPartition.partition)
        )
    }.toSeq

    val producerData = mutable.Map(
      TopicAndPartition(OffsetManager.OffsetsTopicName, offsetManager.partitionFor(offsetCommitRequest.groupId)) ->
        new ByteBufferMessageSet(config.offsetsTopicCompressionCodec, msgs:_*)
    )

    val request = ProducerRequest(
      correlationId = offsetCommitRequest.correlationId,
      clientId = offsetCommitRequest.clientId,
      requiredAcks = config.offsetCommitRequiredAcks,
      ackTimeoutMs = config.offsetCommitTimeoutMs,
      data = producerData)
    trace("Created producer request %s for offset commit request %s.".format(request, offsetCommitRequest))
    request
  }

  /**
   * Handle a produce request or offset commit request (which is really a specialized producer request)
   * kafka acks ==  0，意味着producer不等待broker同步完成的确认，继续发送下一条(批)信息
   * kafka acks ==  1，意味着producer要等待leader成功收到数据并得到确认，才发送下一条message。此选项提供了较好的持久性较低的延迟性。
   * kafka acks == -1，意味着producer得到follower确认，才发送下一条数据
   */
  def handleProducerOrOffsetCommitRequest(request: RequestChannel.Request): Unit = {
    // ProducerRequest 和 OffsetCommitRequest 公共这个方法
    val (produceRequest, offsetCommitRequestOpt) = {
      // 如果是 OffsetCommitRequest，那么构造对应的实体
      if (request.requestId == RequestKeys.OffsetCommitKey) {
        val offsetCommitRequest = request.requestObj.asInstanceOf[OffsetCommitRequest]
        OffsetCommitRequest.changeInvalidTimeToCurrentTime(offsetCommitRequest)
        (producerRequestFromOffsetCommit(offsetCommitRequest), Some(offsetCommitRequest))
      } else {
        (request.requestObj.asInstanceOf[ProducerRequest], None)
      }
    }

    if (produceRequest.requiredAcks > 1 || produceRequest.requiredAcks < -1) {
      warn(("Client %s from %s sent a produce request with request.required.acks of %d, which is now deprecated and will " +
            "be removed in next release. Valid values are -1, 0 or 1. Please consult Kafka documentation for supported " +
            "and recommended configuration.").format(produceRequest.clientId, request.remoteAddress, produceRequest.requiredAcks))
    }

    val sTime = SystemTime.milliseconds
    // 将日志写入到本地文件
    val localProduceResults = appendToLocalLog(produceRequest, offsetCommitRequestOpt.nonEmpty)
    debug("Produce to local log in %d ms".format(SystemTime.milliseconds - sTime))

    // 获取异常错误
    val firstErrorCode = localProduceResults.find(ret => ret.errorCode != ErrorMapping.NoError).map(_.errorCode).getOrElse(ErrorMapping.NoError)

    // 统计出现异常的 partition 数量
    val numPartitionsInError = localProduceResults.count(_.error.isDefined)
    if(produceRequest.requiredAcks == 0) {
      // 如果生产者 request.required.acks = 0，则无需操作；
      // 但是，如果在处理请求时出现任何异常，由于生产者不期望响应，处理程序将向套接字服务器发送关闭连接响应以关闭套接字，
      // 以便生产者客户端知道发生了一些异常并将刷新其元数据
      if (numPartitionsInError != 0) {
        info(("Send the close connection response due to error handling produce request " +
          "[clientId = %s, correlationId = %s, topicAndPartition = %s] with Ack=0")
          .format(produceRequest.clientId, produceRequest.correlationId, produceRequest.topicPartitionMessageSizeMap.keySet.mkString(",")))
        requestChannel.closeConnection(request.processor, request)
      } else {
        if (firstErrorCode == ErrorMapping.NoError) {
          // 如果是 offset 提交，那么更新 offset cache
          offsetCommitRequestOpt.foreach(ocr => offsetManager.putOffsets(ocr.groupId, ocr.requestInfo))
        }

        if (offsetCommitRequestOpt.isDefined) {
          // 如果是 offset 提交，那么即使是 ack 为 0 也需要将消息的持久化结果返回给用户
          val response = offsetCommitRequestOpt.get.responseFor(firstErrorCode, config.offsetMetadataMaxSize)
          requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(response)))
        } else {
          // acks == 0，客户端不关心服务端细节，只关心服务端是否收到请求，因此不需要返回执行结果
          requestChannel.noOperation(request.processor, request)
        }
      }
    } else if (produceRequest.requiredAcks == 1 ||
        produceRequest.numPartitions <= 0 ||
        numPartitionsInError == produceRequest.numPartitions) {
      // 如果 acks == 1，那么客户端需要等 leader 的响应；
      // 如果客户端需要等待服务端响应，或者目标分区数无效，或者所有的分区持久化全部失败，需要返回具体执行结果
      if (firstErrorCode == ErrorMapping.NoError) {
        // 如果是 offset 提交，那么更新 offset cache
        offsetCommitRequestOpt.foreach(ocr => offsetManager.putOffsets(ocr.groupId, ocr.requestInfo) )
      }

      // 返回响应
      // r.start 是消息的 firstOffset
      val statuses = localProduceResults.map(r => r.key -> ProducerResponseStatus(r.errorCode, r.start)).toMap
      val response = offsetCommitRequestOpt.map(_.responseFor(firstErrorCode, config.offsetMetadataMaxSize))
                                           .getOrElse(ProducerResponse(produceRequest.correlationId, statuses))

      requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(response)))
    } else {
      // 如果 acks == -1，需要等待(min.insync.replica-1)个副本同步数据后才返回响应
      // create a list of (topic, partition) pairs to use as keys for this delayed request
      val producerRequestKeys = produceRequest.data.keys.toSeq
      // 生成一个延迟响应
      val statuses = localProduceResults.map(r =>
        r.key -> DelayedProduceResponseStatus(r.end + 1, ProducerResponseStatus(r.errorCode, r.start))).toMap
      val delayedRequest =  new DelayedProduce(
        producerRequestKeys,
        request,
        produceRequest.ackTimeoutMs.toLong,
        produceRequest,
        statuses,
        offsetCommitRequestOpt)

      // add the produce request for watch if it's not satisfied, otherwise send the response back
      val satisfiedByMe = producerRequestPurgatory.checkAndMaybeWatch(delayedRequest)
      if (satisfiedByMe)
        producerRequestPurgatory.respond(delayedRequest)
    }

    // we do not need the data anymore
    produceRequest.emptyData()
  }

  case class ProduceResult(key: TopicAndPartition, start: Long, end: Long, error: Option[Throwable] = None) {
    def this(key: TopicAndPartition, throwable: Throwable) = 
      this(key, -1L, -1L, Some(throwable))
    
    def errorCode = error match {
      case None => ErrorMapping.NoError
      case Some(error) => ErrorMapping.codeFor(error.getClass.asInstanceOf[Class[Throwable]])
    }
  }

  /**
   * Helper method for handling a parsed producer request
   */
  private def appendToLocalLog(producerRequest: ProducerRequest, isOffsetCommit: Boolean): Iterable[ProduceResult] = {
    val partitionAndData: Map[TopicAndPartition, MessageSet] = producerRequest.data
    trace("Append [%s] to local log ".format(partitionAndData.toString))
    partitionAndData.map {case (topicAndPartition, messages) =>
      try {
        if (Topic.InternalTopics.contains(topicAndPartition.topic) &&
            !(isOffsetCommit && topicAndPartition.topic == OffsetManager.OffsetsTopicName)) {
          throw new InvalidTopicException("Cannot append to internal topic %s".format(topicAndPartition.topic))
        }
        val partitionOpt = replicaManager.getPartition(topicAndPartition.topic, topicAndPartition.partition)
        val info = partitionOpt match {
          case Some(partition) =>
            partition.appendMessagesToLeader(messages.asInstanceOf[ByteBufferMessageSet],producerRequest.requiredAcks)
          case None => throw new UnknownTopicOrPartitionException("Partition %s doesn't exist on %d"
            .format(topicAndPartition, brokerId))
        }

        val numAppendedMessages = if (info.firstOffset == -1L || info.lastOffset == -1L) 0 else (info.lastOffset - info.firstOffset + 1)

        // update stats for successfully appended bytes and messages as bytesInRate and messageInRate
        BrokerTopicStats.getBrokerTopicStats(topicAndPartition.topic).bytesInRate.mark(messages.sizeInBytes)
        BrokerTopicStats.getBrokerAllTopicsStats.bytesInRate.mark(messages.sizeInBytes)
        BrokerTopicStats.getBrokerTopicStats(topicAndPartition.topic).messagesInRate.mark(numAppendedMessages)
        BrokerTopicStats.getBrokerAllTopicsStats.messagesInRate.mark(numAppendedMessages)

        trace("%d bytes written to log %s-%d beginning at offset %d and ending at offset %d"
              .format(messages.size, topicAndPartition.topic, topicAndPartition.partition, info.firstOffset, info.lastOffset))
        ProduceResult(topicAndPartition, info.firstOffset, info.lastOffset)
      } catch {
        // NOTE: Failed produce requests is not incremented for UnknownTopicOrPartitionException and NotLeaderForPartitionException
        // since failed produce requests metric is supposed to indicate failure of a broker in handling a produce request
        // for a partition it is the leader for
        case e: KafkaStorageException =>
          fatal("Halting due to unrecoverable I/O error while handling produce request: ", e)
          Runtime.getRuntime.halt(1)
          null
        case ite: InvalidTopicException =>
          warn("Produce request with correlation id %d from client %s on partition %s failed due to %s".format(
            producerRequest.correlationId, producerRequest.clientId, topicAndPartition, ite.getMessage))
          new ProduceResult(topicAndPartition, ite)
        case utpe: UnknownTopicOrPartitionException =>
          warn("Produce request with correlation id %d from client %s on partition %s failed due to %s".format(
               producerRequest.correlationId, producerRequest.clientId, topicAndPartition, utpe.getMessage))
          new ProduceResult(topicAndPartition, utpe)
        case nle: NotLeaderForPartitionException =>
          warn("Produce request with correlation id %d from client %s on partition %s failed due to %s".format(
               producerRequest.correlationId, producerRequest.clientId, topicAndPartition, nle.getMessage))
          new ProduceResult(topicAndPartition, nle)
        case nere: NotEnoughReplicasException =>
          warn("Produce request with correlation id %d from client %s on partition %s failed due to %s".format(
            producerRequest.correlationId, producerRequest.clientId, topicAndPartition, nere.getMessage))
          new ProduceResult(topicAndPartition, nere)
        case e: Throwable =>
          BrokerTopicStats.getBrokerTopicStats(topicAndPartition.topic).failedProduceRequestRate.mark()
          BrokerTopicStats.getBrokerAllTopicsStats.failedProduceRequestRate.mark()
          error("Error processing ProducerRequest with correlation id %d from client %s on partition %s"
            .format(producerRequest.correlationId, producerRequest.clientId, topicAndPartition), e)
          new ProduceResult(topicAndPartition, e)
       }
    }
  }

  /**
   * Handle a fetch request
   */
  def handleFetchRequest(request: RequestChannel.Request): Unit = {
    val fetchRequest = request.requestObj.asInstanceOf[FetchRequest]
    val dataRead = replicaManager.readMessageSets(fetchRequest)

    // if the fetch request comes from the follower,
    // update its corresponding log end offset
    // 之前提到过，follower 在发送 FetchRequest 的时候也会带上自己的 offset
    if(fetchRequest.isFromFollower) {
      // 这里之前看的时候疑惑了很久，因为开始觉得 replicaId 是 partition 维度的
      // 但是更新的时候却是一个 replicaId 对应多个 <TopicAndPartition, Offset>
      // 仔细阅读了一下文档后发现，虽然 replica 是对应 TopicAndPartition 的。
      // 但是 replicaId 实际是对应的 brokerId
      // 比如我们 describe 某个 topic 可能是这样的
      // Topic: xxx      Partition: 49   Leader: 22      Replicas: 22,23 Isr: 22,23
      // 这代表 <xxx, 49> 的 leader 的 22，而他的 Replicas 是 22 和 23
      recordFollowerLogEndOffsets(fetchRequest.replicaId, dataRead.mapValues(_.offset))
    }

    // check if this fetch request can be satisfied right away
    val bytesReadable = dataRead.values.map(_.data.messages.sizeInBytes).sum
    val errorReadingData = dataRead.values.foldLeft(false)((errorIncurred, dataAndOffset) =>
      errorIncurred || (dataAndOffset.data.error != ErrorMapping.NoError))
    // send the data immediately if 1) fetch request does not want to wait
    //                              2) fetch request does not require any data
    //                              3) has enough data to respond
    //                              4) some error happens while reading data
    if(fetchRequest.maxWait <= 0 ||
       fetchRequest.numPartitions <= 0 ||
       bytesReadable >= fetchRequest.minBytes ||
       errorReadingData) {
      debug("Returning fetch response %s for fetch request with correlation id %d to client %s"
        .format(dataRead.values.map(_.data.error).mkString(","), fetchRequest.correlationId, fetchRequest.clientId))
      val response = new FetchResponse(fetchRequest.correlationId, dataRead.mapValues(ret => ret.data))
      requestChannel.sendResponse(new RequestChannel.Response(request, new FetchResponseSend(response)))
    } else {
      debug("Putting fetch request with correlation id %d from client %s into purgatory".format(fetchRequest.correlationId,
        fetchRequest.clientId))
      // create a list of (topic, partition) pairs to use as keys for this delayed request
      val delayedFetchKeys = fetchRequest.requestInfo.keys.toSeq
      val delayedFetch = new DelayedFetch(delayedFetchKeys, request, fetchRequest.maxWait, fetchRequest,
        dataRead.mapValues(_.offset))

      // add the fetch request for watch if it's not satisfied, otherwise send the response back
      val satisfiedByMe = fetchRequestPurgatory.checkAndMaybeWatch(delayedFetch)
      if (satisfiedByMe)
        fetchRequestPurgatory.respond(delayedFetch)
    }
  }

  // <Topic, Partition, replicaId> 可以定位到一个唯一的 Replication
  private def recordFollowerLogEndOffsets(replicaId: Int, offsets: Map[TopicAndPartition, LogOffsetMetadata]) {
    debug("Record follower log end offsets: %s ".format(offsets))
    offsets.foreach {
      case (topicAndPartition, offset) =>
        replicaManager.updateReplicaLEOAndPartitionHW(topicAndPartition.topic,
          topicAndPartition.partition, replicaId, offset)

        // for producer requests with ack > 1, we need to check
        // if they can be unblocked after some follower's log end offsets have moved
        replicaManager.unblockDelayedProduceRequests(topicAndPartition)
    }
  }

  /**
   * Service the offset request API 
   */
  def handleOffsetRequest(request: RequestChannel.Request): Unit = {
    val offsetRequest = request.requestObj.asInstanceOf[OffsetRequest]
    val responseMap = offsetRequest.requestInfo.map(elem => {
      val (topicAndPartition, partitionOffsetRequestInfo) = elem
      try {
        val localReplica = if(!offsetRequest.isFromDebuggingClient) {
          // 请求来自于普通的消费者，那么 replica 的 leader 必须在本台 broker 上。
          // 本函数会在 <allPartition> 中获取 <TopicAndPartition> 对应的 Replica，并且在获取不到时抛出异常。
          replicaManager.getLeaderReplicaIfLocal(topicAndPartition.topic, topicAndPartition.partition)
        } else {
          // 仅仅是为了调试，则不需要关心是否是 leader
          replicaManager.getReplicaOrException(topicAndPartition.topic, topicAndPartition.partition)
        }
        val offsets = {
          // 获取所有 <TopicAndPartition> 对应的 offset
          val allOffsets = fetchOffsets(replicaManager.logManager,
                                        topicAndPartition,
                                        partitionOffsetRequestInfo.time,
                                        partitionOffsetRequestInfo.maxNumOffsets)
          if (!offsetRequest.isFromOrdinaryClient) {
            // 如果是debug，则可以直接返回
            allOffsets
          } else {
            // 不是 debug 则需要根据 HighWatermark 来进行判断。
            val hw = localReplica.highWatermark.messageOffset
            if (allOffsets.exists(ret => (ret > hw))) {
              // 将 offset 替换成 HighWatermark
              hw +: allOffsets.dropWhile(_ > hw)
            } else
              allOffsets
          }
        }
        (topicAndPartition, PartitionOffsetsResponse(ErrorMapping.NoError, offsets))
      } catch {
        // NOTE: UnknownTopicOrPartitionException and NotLeaderForPartitionException are special cased since these error messages
        // are typically transient and there is no value in logging the entire stack trace for the same
        case utpe: UnknownTopicOrPartitionException =>
          warn("Offset request with correlation id %d from client %s on partition %s failed due to %s".format(
               offsetRequest.correlationId, offsetRequest.clientId, topicAndPartition, utpe.getMessage))
          (topicAndPartition, PartitionOffsetsResponse(ErrorMapping.codeFor(utpe.getClass.asInstanceOf[Class[Throwable]]), Nil) )
        case nle: NotLeaderForPartitionException =>
          warn("Offset request with correlation id %d from client %s on partition %s failed due to %s".format(
               offsetRequest.correlationId, offsetRequest.clientId, topicAndPartition,nle.getMessage))
          (topicAndPartition, PartitionOffsetsResponse(ErrorMapping.codeFor(nle.getClass.asInstanceOf[Class[Throwable]]), Nil) )
        case e: Throwable =>
          warn("Error while responding to offset request", e)
          (topicAndPartition, PartitionOffsetsResponse(ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]]), Nil) )
      }
    })
    val response = OffsetResponse(offsetRequest.correlationId, responseMap)
    requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(response)))
  }

  // 拉取指定 <TopicAndPartition> 在
  def fetchOffsets(logManager: LogManager, topicAndPartition: TopicAndPartition, timestamp: Long, maxNumOffsets: Int): Seq[Long] = {
    logManager.getLog(topicAndPartition) match {
      case Some(log) => 
        fetchOffsetsBefore(log, timestamp, maxNumOffsets)
      case None => 
        if (timestamp == OffsetRequest.LatestTime || timestamp == OffsetRequest.EarliestTime)
          Seq(0L)
        else
          Nil
    }
  }

  /**
   * 拉取 <Log> 指定修改时间 timestamp 之前的 <LogSegment> 的详细信息，最大不超过 maxNumOffsets
   *
   * @param log           Log
   * @param timestamp     指定修改时间
   * @param maxNumOffsets 获取的最大 LogSegment 个数
   * @return
   */
  def fetchOffsetsBefore(log: Log, timestamp: Long, maxNumOffsets: Int): Seq[Long] = {
    val segsArray = log.logSegments.toArray
    var offsetTimeArray: Array[(Long, Long)] = null
    // 如果最新的 LogSegment 已经有数据的话，这个 LogSegment 也需要读
    if(segsArray.last.size > 0)
      offsetTimeArray = new Array[(Long, Long)](segsArray.length + 1)
    else
      offsetTimeArray = new Array[(Long, Long)](segsArray.length)

    // 初始化 LogSegment 的 baseOffset 以及 lastModified timestamp
    for(i <- segsArray.indices)
      offsetTimeArray(i) = (segsArray(i).baseOffset, segsArray(i).lastModified)
    if(segsArray.last.size > 0) {
      // 对于最后一个 LogSegment，它的 offset 不是 baseOffset 而是 LogEndOffset
      offsetTimeArray(segsArray.length) = (log.logEndOffset, SystemTime.milliseconds)
    }

    var startIndex = -1
    // What to do when there is no initial offset in Kafka or if the current offset does not exist any more on the server (e.g. because that data has been deleted):
    //  earliest: automatically reset the offset to the earliest offset
    //  latest: automatically reset the offset to the latest offset
    //  none: throw exception to the consumer if no previous offset is found for the consumer's group
    //  anything else: throw exception to the consumer.
    timestamp match {
      // latest，只需要获取最后一个 LogSegment 的 offset 即可，因为我们直接从最新的 offset 开始消费
      case OffsetRequest.LatestTime =>
        startIndex = offsetTimeArray.length - 1
      // earliest，则需要从第一个 LogSegment 开始消费
      case OffsetRequest.EarliestTime =>
        startIndex = 0
      case _ =>
        // 如果都没有指定，我们从后往前找到第一个 lastModified timestamp <= timestamp 的并返回。
        var isFound = false
        debug("Offset time array = " + offsetTimeArray.foreach(o => "%d, %d".format(o._1, o._2)))
        startIndex = offsetTimeArray.length - 1
        while (startIndex >= 0 && !isFound) {
          if (offsetTimeArray(startIndex)._2 <= timestamp)
            isFound = true
          else
            startIndex -=1
        }
    }

    // maxNumOffsets 指定了最大返回的 LogSegment offset 个数
    val retSize = maxNumOffsets.min(startIndex + 1)
    // 返回找到的 offset
    val ret = new Array[Long](retSize)
    for(j <- 0 until retSize) {
      ret(j) = offsetTimeArray(startIndex)._1
      startIndex -= 1
    }
    // ensure that the returned seq is in descending order of offsets
    ret.toSeq.sortBy(- _)
  }

  private def getTopicMetadata(topics: Set[String]): Seq[TopicMetadata] = {
    // 从 metadataCache 中获取 topic metadata
    val topicResponses = metadataCache.getTopicMetadata(topics)
    // 如果请求的 topic 列表不为空，并且响应的 metadata size 与请求的topic列表长度不同
    // 说明有一部分数据是没有存放在 metadataCache 中的
    // 因为我们 metadataCache.getTopicMetadata(topics) 只会请求部分 topics
    // 所以一定是数据没有存在 metadataCache 中
    if (topics.nonEmpty && topicResponses.size != topics.size) {
      // 找到 cache 中不存在的 topic 列表
      val nonExistentTopics = topics -- topicResponses.map(_.topic).toSet
      // 为 cache 中不存在的 topic 列表生成 metadata
      val responsesForNonExistentTopics = nonExistentTopics.map { topic =>
        // 如果是 kafka 内部用于记录 offset 的 topic，或者自动创建 topic 打开了
        if (topic == OffsetManager.OffsetsTopicName || config.autoCreateTopicsEnable) {
          try {
            // 创建 OffsetsTopicName 对应的 topic
            if (topic == OffsetManager.OffsetsTopicName) {
              val aliveBrokers = metadataCache.getAliveBrokers
              // 指定副本数为配置的副本数与在线brokers的最小值
              val offsetsTopicReplicationFactor =
                if (aliveBrokers.nonEmpty)
                  Math.min(config.offsetsTopicReplicationFactor, aliveBrokers.length)
                else
                  config.offsetsTopicReplicationFactor
              AdminUtils.createTopic(zkClient, topic, config.offsetsTopicPartitions,
                                     offsetsTopicReplicationFactor,
                                     offsetManager.offsetsTopicConfig)
              info("Auto creation of topic %s with %d partitions and replication factor %d is successful!"
                .format(topic, config.offsetsTopicPartitions, offsetsTopicReplicationFactor))
            }
            else {
              // 创建 topic
              AdminUtils.createTopic(zkClient, topic, config.numPartitions, config.defaultReplicationFactor)
              info("Auto creation of topic %s with %d partitions and replication factor %d is successful!"
                   .format(topic, config.numPartitions, config.defaultReplicationFactor))
            }
          } catch {
            case e: TopicExistsException => // let it go, possibly another broker created this topic
          }
          new TopicMetadata(topic, Seq.empty[PartitionMetadata], ErrorMapping.LeaderNotAvailableCode)
        } else {
          new TopicMetadata(topic, Seq.empty[PartitionMetadata], ErrorMapping.UnknownTopicOrPartitionCode)
        }
      }
      topicResponses.appendAll(responsesForNonExistentTopics)
    }
    topicResponses
  }

  /**
   * Service the topic metadata request API
   */
  def handleTopicMetadataRequest(request: RequestChannel.Request): Unit = {
    val metadataRequest = request.requestObj.asInstanceOf[TopicMetadataRequest]
    // 获取 topic metadata
    val topicMetadata = getTopicMetadata(metadataRequest.topics.toSet)
    // 获取在线的broker列表
    val brokers = metadataCache.getAliveBrokers
    trace("Sending topic metadata %s and brokers %s for correlation id %d to client %s".format(topicMetadata.mkString(","), brokers.mkString(","), metadataRequest.correlationId, metadataRequest.clientId))
    val response = new TopicMetadataResponse(brokers, topicMetadata, metadataRequest.correlationId)
    requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(response)))
  }

  /*
   * Service the Offset fetch API
   */
  def handleOffsetFetchRequest(request: RequestChannel.Request) {
    val offsetFetchRequest = request.requestObj.asInstanceOf[OffsetFetchRequest]

    if (offsetFetchRequest.versionId == 0) {
      // version 0 reads offsets from ZK
      val responseInfo = offsetFetchRequest.requestInfo.map( t => {
        val topicDirs = new ZKGroupTopicDirs(offsetFetchRequest.groupId, t.topic)
        try {
          ensureTopicExists(t.topic)
          val payloadOpt = ZkUtils.readDataMaybeNull(zkClient, topicDirs.consumerOffsetDir + "/" + t.partition)._1
          payloadOpt match {
            case Some(payload) => {
              (t, OffsetMetadataAndError(offset=payload.toLong, error=ErrorMapping.NoError))
            }
            case None => (t, OffsetMetadataAndError(OffsetAndMetadata.InvalidOffset, OffsetAndMetadata.NoMetadata,
              ErrorMapping.UnknownTopicOrPartitionCode))
          }
        } catch {
          case e: Throwable =>
            (t, OffsetMetadataAndError(OffsetAndMetadata.InvalidOffset, OffsetAndMetadata.NoMetadata,
              ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]])))
        }
      })
      val response = new OffsetFetchResponse(collection.immutable.Map(responseInfo: _*), offsetFetchRequest.correlationId)
      requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(response)))
    } else {
      // version 1 reads offsets from Kafka
      val (unknownTopicPartitions, knownTopicPartitions) = offsetFetchRequest.requestInfo.partition(topicAndPartition =>
        metadataCache.getPartitionInfo(topicAndPartition.topic, topicAndPartition.partition).isEmpty
      )
      val unknownStatus = unknownTopicPartitions.map(topicAndPartition => (topicAndPartition, OffsetMetadataAndError.UnknownTopicOrPartition)).toMap
      val knownStatus =
        if (knownTopicPartitions.size > 0)
          offsetManager.getOffsets(offsetFetchRequest.groupId, knownTopicPartitions).toMap
        else
          Map.empty[TopicAndPartition, OffsetMetadataAndError]
      val status = unknownStatus ++ knownStatus

      val response = OffsetFetchResponse(status, offsetFetchRequest.correlationId)

      trace("Sending offset fetch response %s for correlation id %d to client %s."
            .format(response, offsetFetchRequest.correlationId, offsetFetchRequest.clientId))
      requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(response)))
    }
  }

  /*
   * Service the consumer metadata API
   */
  def handleConsumerMetadataRequest(request: RequestChannel.Request) {
    val consumerMetadataRequest = request.requestObj.asInstanceOf[ConsumerMetadataRequest]

    val partition = offsetManager.partitionFor(consumerMetadataRequest.group)

    // get metadata (and create the topic if necessary)
    val offsetsTopicMetadata = getTopicMetadata(Set(OffsetManager.OffsetsTopicName)).head

    val errorResponse = ConsumerMetadataResponse(None, ErrorMapping.ConsumerCoordinatorNotAvailableCode, consumerMetadataRequest.correlationId)

    val response =
      offsetsTopicMetadata.partitionsMetadata.find(_.partitionId == partition).map { partitionMetadata =>
        partitionMetadata.leader.map { leader =>
          ConsumerMetadataResponse(Some(leader), ErrorMapping.NoError, consumerMetadataRequest.correlationId)
        }.getOrElse(errorResponse)
      }.getOrElse(errorResponse)

    trace("Sending consumer metadata %s for correlation id %d to client %s."
          .format(response, consumerMetadataRequest.correlationId, consumerMetadataRequest.clientId))
    requestChannel.sendResponse(new RequestChannel.Response(request, new BoundedByteBufferSend(response)))
  }

  def close() {
    debug("Shutting down.")
    fetchRequestPurgatory.shutdown()
    producerRequestPurgatory.shutdown()
    debug("Shut down complete.")
  }
}

