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
package kafka.producer

import kafka.api.TopicMetadata
import kafka.client.ClientUtils
import kafka.cluster.Broker
import kafka.common.{ErrorMapping, KafkaException}
import kafka.utils.Logging

import scala.collection.mutable

/**
 * 保存了topic->[[TopicMetadata]]的相关信息，提供了查询topic的partition信息的能力
 *
 * @param producerConfig     配置文件，在初始化时通过配置文件解析broker列表，因为查询TopicMetadata需要向随机的broker发送TopicMetadataRequest
 * @param producerPool       [[ProducerPool]]，保存了到broker的[[SyncProducer]]
 * @param topicPartitionInfo topic -> [[TopicMetadata]]，会在某些场景下更新
 */
class BrokerPartitionInfo(producerConfig: ProducerConfig,
                          producerPool: ProducerPool,
                          topicPartitionInfo: mutable.HashMap[String, TopicMetadata])
        extends Logging {
  val brokerList: String = producerConfig.brokerList
  val brokers: Seq[Broker] = ClientUtils.parseBrokerList(brokerList)

  /**
   * 查询某个topic的所有Partition对应的leaderBrokerIdOption
   * @param topic 查询的topic
   * @return Seq[[[kafka.common.TopicAndPartition]] -> leaderBrokerIdOption]，如果没有可用broker返回长度为0的序列
   */
  def getBrokerPartitionInfo(topic: String, correlationId: Int): Seq[PartitionAndLeader] = {
    debug("Getting broker partition info for topic %s".format(topic))
    val topicMetadata = topicPartitionInfo.get(topic)
    val metadata: TopicMetadata =
      topicMetadata match {
        case Some(m) => m
        case None =>
          // 如果没有找到，则通过向broker发送TopicMetadataRequest来更新
          updateInfo(Set(topic), correlationId)
          val topicMetadata = topicPartitionInfo.get(topic)
          topicMetadata match {
            case Some(m) => m
            case None => throw new KafkaException("Failed to fetch topic metadata for topic: " + topic)
          }
      }
    val partitionMetadata = metadata.partitionsMetadata
    //如果没有topic对应的partition信息则抛出异常
    if(partitionMetadata.isEmpty) {
      if(metadata.errorCode != ErrorMapping.NoError) {
        throw new KafkaException(ErrorMapping.exceptionFor(metadata.errorCode))
      } else {
        throw new KafkaException("Topic metadata %s has empty partition metadata and no error code".format(metadata))
      }
    }
    //获取partition对应的leaderIdOption，并按照partitionId从小到大进行排序并返回
    partitionMetadata.map { m =>
      m.leader match {
        case Some(leader) =>
          debug("Partition [%s,%d] has leader %d".format(topic, m.partitionId, leader.id))
          PartitionAndLeader(topic, m.partitionId, Some(leader.id))
        case None =>
          debug("Partition [%s,%d] does not have a leader yet".format(topic, m.partitionId))
          PartitionAndLeader(topic, m.partitionId, None)
      }
    }.sortWith((s, t) => s.partitionId < t.partitionId)
  }

  /**
   * 通过向随机的broker发送[[kafka.javaapi.TopicMetadataRequest]]来更新[[ProducerPool]]内部的缓存
   * @param topics the topics for which the metadata is to be fetched
   */
  def updateInfo(topics: Set[String], correlationId: Int): Unit = {
    var topicsMetadata: Seq[TopicMetadata] = Nil
    val topicMetadataResponse = ClientUtils.fetchTopicMetadata(topics, brokers, producerConfig, correlationId)
    topicsMetadata = topicMetadataResponse.topicsMetadata

    /**
     * 在[[TopicMetadata]]没有错误时，更新[[topicPartitionInfo]]，并在存在错误时输出topic和partition相关的告警日志
     */
    topicsMetadata.foreach(tmd =>{
      trace("Metadata for topic %s is %s".format(tmd.topic, tmd))
      if(tmd.errorCode == ErrorMapping.NoError) {
        topicPartitionInfo.put(tmd.topic, tmd)
      } else {
        warn("Error while fetching metadata [%s] for topic [%s]: %s ".format(tmd, tmd.topic, ErrorMapping.exceptionFor(tmd.errorCode).getClass))
      }
      tmd.partitionsMetadata.foreach(pmd => {
        if (pmd.errorCode != ErrorMapping.NoError && pmd.errorCode == ErrorMapping.LeaderNotAvailableCode) {
          warn("Error while fetching metadata %s for topic partition [%s,%d]: [%s]".format(pmd, tmd.topic, pmd.partitionId,
            ErrorMapping.exceptionFor(pmd.errorCode).getClass))
        } // any other error code (e.g. ReplicaNotAvailable) can be ignored since the producer does not need to access the replica and isr metadata
      })
    })
    producerPool.updateProducer(topicsMetadata)
  }

}

case class PartitionAndLeader(topic: String, partitionId: Int, leaderBrokerIdOpt: Option[Int])
