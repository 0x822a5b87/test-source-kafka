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

package kafka.producer.async

import kafka.api.{ProducerRequest, TopicMetadata}
import kafka.common._
import kafka.message.{ByteBufferMessageSet, Message, NoCompressionCodec}
import kafka.producer._
import kafka.serializer.Encoder
import kafka.utils.{Logging, SystemTime, Utils}

import java.util.concurrent.atomic._
import scala.collection.mutable.ArrayBuffer
import scala.collection.{Map, Seq, mutable}
import scala.util.Random

class DefaultEventHandler[K,V](config: ProducerConfig,
                               private val partitioner: Partitioner,
                               private val encoder: Encoder[V],
                               private val keyEncoder: Encoder[K],
                               private val producerPool: ProducerPool,
                               private val topicPartitionInfos: mutable.HashMap[String, TopicMetadata] = new mutable.HashMap[String, TopicMetadata])
  extends EventHandler[K,V] with Logging {
  //是否同步发送
  val isSync: Boolean = "sync" == config.producerType

  //获取客户端发送消息的correlationId，注意，producer是客户端。
  val correlationId = new AtomicInteger(0)
  val brokerPartitionInfo = new BrokerPartitionInfo(config, producerPool, topicPartitionInfos)

  /**
   * [[TopicMetadata]]的刷新间隔，producer会在两种情况下刷新metadata
   * 1. 查询元数据失败（partition missing， leader not available）
   * 2. 通过此参数间隔定时的刷新
   */
  private val topicMetadataRefreshInterval = config.topicMetadataRefreshIntervalMs
  //metadata最后刷新时间
  private var lastTopicMetadataRefreshTime = 0L
  private val topicMetadataToRefresh = mutable.Set.empty[String]
  //message在未指定partitionKey时会随机的发往一个partition，就缓存在这个partition中
  //该缓存会定期刷新，或者在发送失败时刷新
  //所以如果不指定partitionKey的话，可以发现每一个producer会在某一段时间内写入到某一个特定的partition
  //经过一段时间后，又写入另一个特定的partition
  private val sendPartitionPerTopicCache = mutable.HashMap.empty[String, Int]

  private val producerStats = ProducerStatsRegistry.getProducerStats(config.clientId)
  private val producerTopicStats = ProducerTopicStatsRegistry.getProducerTopicStats(config.clientId)

  def handle(events: Seq[KeyedMessage[K,V]]): Unit = {
    //序列化数据
    val serializedData = serialize(events)
    //metrics
    serializedData.foreach {
      keyed =>
        val dataSize = keyed.message.payloadSize
        producerTopicStats.getProducerTopicStats(keyed.topic).byteRate.mark(dataSize)
        producerTopicStats.getProducerAllTopicsStats().byteRate.mark(dataSize)
    }
    var outstandingProduceRequests = serializedData
    var remainingRetries = config.messageSendMaxRetries + 1
    val correlationIdStart = correlationId.get()
    debug("Handling %d events".format(events.size))
    while (remainingRetries > 0 && outstandingProduceRequests.nonEmpty) {
      //将发送的所有KeyMessage中包含的topic增加到带刷新的元数据中，在合适的时机刷新这些topic对应的元数据
      topicMetadataToRefresh ++= outstandingProduceRequests.map(_.topic)
      if (topicMetadataRefreshInterval >= 0 &&
          SystemTime.milliseconds - lastTopicMetadataRefreshTime > topicMetadataRefreshInterval) {
        Utils.swallowError(brokerPartitionInfo.updateInfo(topicMetadataToRefresh.toSet, correlationId.getAndIncrement))
        sendPartitionPerTopicCache.clear()
        topicMetadataToRefresh.clear
        lastTopicMetadataRefreshTime = SystemTime.milliseconds
      }
      //发送数据
      outstandingProduceRequests = dispatchSerializedData(outstandingProduceRequests)
      //如果返回不为空，说明存在数据发送失败，更新元数据，增加重试次数
      if (outstandingProduceRequests.nonEmpty) {
        info("Back off for %d ms before retrying send. Remaining retries = %d".format(config.retryBackoffMs, remainingRetries-1))
        // back off and update the topic metadata cache before attempting another send operation
        Thread.sleep(config.retryBackoffMs)
        // get topics of the outstanding produce requests and refresh metadata for those
        Utils.swallowError(brokerPartitionInfo.updateInfo(outstandingProduceRequests.map(_.topic).toSet, correlationId.getAndIncrement))
        sendPartitionPerTopicCache.clear()
        remainingRetries -= 1
        producerStats.resendRate.mark()
      }
    }
    //重试次数用完，仍然有数据发送失败，则抛出异常
    if(outstandingProduceRequests.nonEmpty) {
      producerStats.failedSendRate.mark()
      val correlationIdEnd = correlationId.get()
      error("Failed to send requests for topics %s with correlation ids in [%d,%d]"
        .format(outstandingProduceRequests.map(_.topic).toSet.mkString(","),
        correlationIdStart, correlationIdEnd-1))
      throw new FailedToSendMessageException("Failed to send messages after " + config.messageSendMaxRetries + " tries.", null)
    }
  }

  /**
   * 将messages发送到对应的partition，并返回发送失败的messages
   *
   * @param messages 等待发送的messages
   * @return 发送失败的messages
   */
  private def dispatchSerializedData(messages: Seq[KeyedMessage[K,Message]]): Seq[KeyedMessage[K, Message]] = {
    //brokerId -> {[TopicAndPartition] -> Seq[KeyedMessage]}
    val partitionedDataOpt = partitionAndCollate(messages)
    partitionedDataOpt match {
      case Some(partitionedData) =>
        val failedProduceRequests = new ArrayBuffer[KeyedMessage[K,Message]]
        try {
          for ((brokerId, messagesPerBrokerMap) <- partitionedData) {
            //按照压缩相关配置，将Seq[KeyedMessage[K, Message]]转换为[[ByteBufferMessageSet]]
            val messageSetPerBroker = groupMessagesToSet(messagesPerBrokerMap)
            //发送数据到对应的broker，并返回发送失败的partitions
            val failedTopicPartitions = send(brokerId, messageSetPerBroker)
            failedTopicPartitions.foreach(topicPartition => {
              messagesPerBrokerMap.get(topicPartition) match {
                //记录发送失败的消息。
                //TODO
                //注意，这里的data是整个partition的全部消息，需要确认一下
                //kafka的写入机制对于某个partition是否存在事务
                case Some(data) => failedProduceRequests.appendAll(data)
                case None => // nothing
              }
            })
          }
        } catch {
          case t: Throwable => error("Failed to send messages", t)
        }
        failedProduceRequests
      case None =>
        //分区失败，所有的消息都发送失败
        messages
    }
  }

  def serialize(events: Seq[KeyedMessage[K,V]]): Seq[KeyedMessage[K,Message]] = {
    val serializedMessages = new ArrayBuffer[KeyedMessage[K,Message]](events.size)
    events.foreach{e =>
      try {
        if(e.hasKey)
          serializedMessages += new KeyedMessage[K,Message](topic = e.topic, key = e.key, partKey = e.partKey, message = new Message(key = keyEncoder.toBytes(e.key), bytes = encoder.toBytes(e.message)))
        else
          serializedMessages += new KeyedMessage[K,Message](topic = e.topic, key = e.key, partKey = e.partKey, message = new Message(bytes = encoder.toBytes(e.message)))
      } catch {
        case t: Throwable =>
          producerStats.serializationErrorRate.mark()
          if (isSync) {
            throw t
          } else {
            // currently, if in async mode, we just log the serialization error. We need to revisit
            // this when doing kafka-496
            error("Error serializing message for topic %s".format(e.topic), t)
          }
      }
    }
    serializedMessages
  }

  /**
   * 分拣messages，分拣之后的结果是：
   * brokerId -> {[TopicAndPartition] -> Seq[KeyedMessage]}
   * 记得我们前面提到的[[SyncProducer]]是连接到每个broker的，此时我们只需要将结果包含的数据找到对应的[[SyncProducer]]即可发送
   * @param messages 等待发送的消息
   * @return
   */
  def partitionAndCollate(messages: Seq[KeyedMessage[K,Message]]): Option[Map[Int, collection.mutable.Map[TopicAndPartition, Seq[KeyedMessage[K,Message]]]]] = {
    val ret = new mutable.HashMap[Int, collection.mutable.Map[TopicAndPartition, Seq[KeyedMessage[K,Message]]]]
    try {
      for (message <- messages) {
        //获取message对应的Seq[PartitionAndLeader]
        val topicPartitionsList = getPartitionListForTopic(message)
        //根据message.partitionKey来获取对应的partitionId
        val partitionIndex = getPartition(message.topic, message.partitionKey, topicPartitionsList)
        //获取PartitionAndLeader
        val brokerPartition = topicPartitionsList(partitionIndex)

        //将失败推迟到send操作，这样到其他broker的request是正常处理的
        val leaderBrokerId = brokerPartition.leaderBrokerIdOpt.getOrElse(-1)

        //获取发送到发往每个broker的数据，如果ret中不存在就创建它
        var dataPerBroker: mutable.HashMap[TopicAndPartition, Seq[KeyedMessage[K,Message]]] = null
        ret.get(leaderBrokerId) match {
          case Some(element) =>
            dataPerBroker = element.asInstanceOf[mutable.HashMap[TopicAndPartition, Seq[KeyedMessage[K,Message]]]]
          case None =>
            dataPerBroker = new mutable.HashMap[TopicAndPartition, Seq[KeyedMessage[K,Message]]]
            //注意这里，所有不可用的partition都存放在-1这个key下
            ret.put(leaderBrokerId, dataPerBroker)
        }

        val topicAndPartition = TopicAndPartition(message.topic, brokerPartition.partitionId)
        //获取发送到每个broker的数据，并且按照TopicAndPartition分组
        var dataPerTopicPartition: ArrayBuffer[KeyedMessage[K,Message]] = null
        dataPerBroker.get(topicAndPartition) match {
          case Some(element) =>
            dataPerTopicPartition = element.asInstanceOf[ArrayBuffer[KeyedMessage[K,Message]]]
          case None =>
            dataPerTopicPartition = new ArrayBuffer[KeyedMessage[K,Message]]
            dataPerBroker.put(topicAndPartition, dataPerTopicPartition)
        }
        dataPerTopicPartition.append(message)
      }
      Some(ret)
    }catch {    // Swallow recoverable exceptions and return None so that they can be retried.
      case ute: UnknownTopicOrPartitionException => warn("Failed to collate messages by topic,partition due to: " + ute.getMessage); None
      case lnae: LeaderNotAvailableException => warn("Failed to collate messages by topic,partition due to: " + lnae.getMessage); None
      case oe: Throwable => error("Failed to collate messages by topic, partition due to: " + oe.getMessage); None
    }
  }

  private def getPartitionListForTopic(m: KeyedMessage[K,Message]): Seq[PartitionAndLeader] = {
    val topicPartitionsList = brokerPartitionInfo.getBrokerPartitionInfo(m.topic, correlationId.getAndIncrement)
    debug("Broker partitions registered for topic: %s are %s"
      .format(m.topic, topicPartitionsList.map(p => p.partitionId).mkString(",")))
    val totalNumPartitions = topicPartitionsList.length
    if(totalNumPartitions == 0)
      throw new NoBrokersForPartitionException("Partition key = " + m.key)
    topicPartitionsList
  }

  /**
   * 根据topic和[[KeyedMessage.partitionKey]]获取指定的partitionIndex。
   * 注意，这里返回的是在topicPartitionList内的索引，而不是partitionId
   * 在获取的partitionIndex不在0和numPartitions-1内时抛出[[UnknownTopicOrPartitionException]]
   * @param topic The topic
   * @param key the partition key
   * @param topicPartitionList the list of available partitions
   * @return the partition id
   */
  private def getPartition(topic: String, key: Any, topicPartitionList: Seq[PartitionAndLeader]): Int = {
    val numPartitions = topicPartitionList.size
    if(numPartitions <= 0)
      throw new UnknownTopicOrPartitionException("Topic " + topic + " doesn't exist")
    val partition =
      if(key == null) {
        //如果partitionKey为空，我们从缓存中随机选择一个partition
        val id = sendPartitionPerTopicCache.get(topic)
        id match {
          case Some(partitionId) =>
            //直接返回partitionId而不用检查leader可用性
            //因为我们都想将失败推迟到send
            partitionId
          case None =>
            //从leader可用的partition中随机的选择一个partition作为目标地址，并更新缓存
            val availablePartitions = topicPartitionList.filter(_.leaderBrokerIdOpt.isDefined)
            if (availablePartitions.isEmpty)
              throw new LeaderNotAvailableException("No leader for any partition in topic " + topic)
            val index = Utils.abs(Random.nextInt) % availablePartitions.size
            val partitionId = availablePartitions(index).partitionId
            sendPartitionPerTopicCache.put(topic, partitionId)
            partitionId
        }
      } else {
        /**
         * 如果partitionKey不为null，则使用分区函数进行分区
         * [[DefaultPartitioner]]其实就是随机发送到某个分区
         */
        partitioner.partition(key, numPartitions)
      }
    if(partition < 0 || partition >= numPartitions)
      throw new UnknownTopicOrPartitionException("Invalid partition id: " + partition + " for topic " + topic +
        "; Valid values are in the inclusive range of [0, " + (numPartitions-1) + "]")
    trace("Assigning message of topic %s and key %s to a selected partition %d".format(topic, if (key == null) "[none]" else key.toString, partition))
    partition
  }

  /**
   * 基于一个(topic, partition) -> messages的map构造并发送produce request
   *
   * @param brokerId the broker that will receive the request
   * @param messagesPerTopic the messages as a map from (topic, partition) -> messages
   * @return the set (topic, partitions) messages which incurred an error sending or processing
   */
  private def send(brokerId: Int, messagesPerTopic: collection.mutable.Map[TopicAndPartition, ByteBufferMessageSet]) = {
    /**
     * 在[[partitionAndCollate]]中，我们将所有leader不可用的都放在了-1这个key下
     */
    if(brokerId < 0) {
      warn("Failed to send data since partitions %s don't have a leader".format(messagesPerTopic.keys.mkString(",")))
      messagesPerTopic.keys.toSeq
    } else if(messagesPerTopic.nonEmpty) {
      val currentCorrelationId = correlationId.getAndIncrement
      //构造produce request
      val producerRequest = new ProducerRequest(currentCorrelationId, config.clientId, config.requestRequiredAcks,
        config.requestTimeoutMs, messagesPerTopic)
      var failedTopicPartitions = Seq.empty[TopicAndPartition]
      try {
        //从ProducerPool中获取对应的SyncProducer
        val syncProducer = producerPool.getProducer(brokerId)
        //发送消息
        val response = syncProducer.send(producerRequest)
        if(response != null) {
          //request  是 Map[TopicAndPartition, ByteBufferMessageSet]
          //response 是 Map[TopicAndPartition, ProducerResponseStatus]
          //request和response是按照partition一一对应的
          if (response.status.size != producerRequest.data.size) {
            throw new KafkaException("Incomplete response (%s) for producer request (%s)".format(response, producerRequest))
          }
          //找到所有的失败的partition
          val failedPartitionsAndStatus = response.status.filter(_._2.error != ErrorMapping.NoError).toSeq
          failedTopicPartitions = failedPartitionsAndStatus.map(partitionStatus => partitionStatus._1)
          failedTopicPartitions
        } else {
          Seq.empty[TopicAndPartition]
        }
      } catch {
        case t: Throwable =>
          warn("Failed to send producer request with correlation id %d to broker %d with data for partitions %s"
            .format(currentCorrelationId, brokerId, messagesPerTopic.keys.mkString(",")), t)
          messagesPerTopic.keys.toSeq
      }
    } else {
      List.empty
    }
  }

  /**
   * 将按[[TopicAndPartition]]分组后的消息序列转换为[[ByteBufferMessageSet]]
   * @param messagesPerTopicAndPartition 等待转换的消息
   * @return 转换后的[[ByteBufferMessageSet]]
   */
  private def groupMessagesToSet(messagesPerTopicAndPartition: collection.mutable.Map[TopicAndPartition, Seq[KeyedMessage[K, Message]]]) = {
    val messagesPerTopicPartition = messagesPerTopicAndPartition.map { case (topicAndPartition, messages) =>
      //当到达这里时已经确定messages是需要发送到同一个partition了
      val rawMessages = messages.map(_.message)
      (topicAndPartition,
        //不压缩消息
        config.compressionCodec match {
          case NoCompressionCodec =>
            debug("Sending %d messages with no compression to %s".format(messages.size, topicAndPartition))
            new ByteBufferMessageSet(NoCompressionCodec, rawMessages: _*)
          case _ =>
            //如果compressedTopics为空，则对所有的topic开启压缩，否则对特定的topic开启压缩
            config.compressedTopics.size match {
              case 0 =>
                new ByteBufferMessageSet(config.compressionCodec, rawMessages: _*)
              case _ =>
                if (config.compressedTopics.contains(topicAndPartition.topic)) {
                  new ByteBufferMessageSet(config.compressionCodec, rawMessages: _*)
                }
                else {
                  new ByteBufferMessageSet(NoCompressionCodec, rawMessages: _*)
                }
            }
        }
      )
    }
    messagesPerTopicPartition
  }

  def close(): Unit = {
    if (producerPool != null)
      producerPool.close()
  }
}
