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

import scala.collection.{Seq, Set, mutable}
import kafka.api._
import kafka.cluster.Broker
import java.util.concurrent.locks.ReentrantReadWriteLock
import kafka.utils.Utils._
import kafka.common.{ErrorMapping, ReplicaNotAvailableException, LeaderNotAvailableException}
import kafka.common.TopicAndPartition
import kafka.controller.KafkaController.StateChangeLogger

/**
 *  A cache for the state (e.g., current leader) of each partition. This cache is updated through
 *  UpdateMetadataRequest from the controller. Every broker maintains the same cache, asynchronously.
 */
private[server] class MetadataCache {
  private val cache: mutable.Map[String, mutable.Map[Int, PartitionStateInfo]] =
    new mutable.HashMap[String, mutable.Map[Int, PartitionStateInfo]]()
  private var aliveBrokers: Map[Int, Broker] = Map()
  private val partitionMetadataLock = new ReentrantReadWriteLock()

  def getTopicMetadata(topics: Set[String]) = {
    // 如果 topics 为空，则返回所有的 topic 信息
    val isAllTopics = topics.isEmpty
    val topicsRequested = if(isAllTopics) cache.keySet else topics

    val topicResponses: mutable.ListBuffer[TopicMetadata] = new mutable.ListBuffer[TopicMetadata]
    inReadLock(partitionMetadataLock) {
      for (topic <- topicsRequested) {
        if (isAllTopics || cache.contains(topic)) {
          // 获取 topic 的所有 partition 信息， Map[Int, PartitionStateInfo]
          val partitionStateInfos = cache(topic)
          val partitionMetadata = partitionStateInfos.map {
            // 现在我们进入了某一个 topic 的某一个分区相关的逻辑
            case (partitionId, partitionState) =>
              // 获取所有的副本ID： Set[Int]
              val replicas = partitionState.allReplicas
              // 从在线的brokers中获取所有的副本ID对应的broker
              // 此时，我们已经拿到了我们需要访问的topics列表的所有副本所在的broker
              // 比如，假设我们请求 test 的 metadata，并且 test 只在 brokerId == 1 和 brokerId == 2 的 broker 上
              // 那现在 replicaInfo 就包含了 broker1 以及 broker2
              val replicaInfo: Seq[Broker] = replicas.map(replicaId => aliveBrokers.getOrElse(replicaId, null)).filter(_ != null).toSeq
              var leaderInfo: Option[Broker] = None
              var isrInfo: Seq[Broker] = Nil
              // cache 里缓存了 broker 的 leader，isr 相关的信息
              val leaderIsrAndEpoch = partitionState.leaderIsrAndControllerEpoch
              val leader = leaderIsrAndEpoch.leaderAndIsr.leader
              val isr = leaderIsrAndEpoch.leaderAndIsr.isr
              val topicPartition = TopicAndPartition(topic, partitionId)
              try {
                // 从在线的broker中获取当前的 [topic, partition] 的leader broker
                leaderInfo = aliveBrokers.get(leader)
                if (leaderInfo.isEmpty)
                  throw new LeaderNotAvailableException("Leader not available for %s".format(topicPartition))
                isrInfo = isr.map(aliveBrokers.getOrElse(_, null)).filter(_ != null)
                // 如果从在线的broker拿到的实际replica大小与cache中缓存的replica大小不同，则说明可能有broker不可用
                if (replicaInfo.size < replicas.size)
                  throw new ReplicaNotAvailableException("Replica information not available for following brokers: " +
                    replicas.filterNot(replicaInfo.map(_.id).contains(_)).mkString(","))
                // 如果从在线的broker中拿到isr信息与cache的isr信息不符，说明有isr不可用
                if (isrInfo.size < isr.size)
                  throw new ReplicaNotAvailableException("In Sync Replica information not available for following brokers: " +
                    isr.filterNot(isrInfo.map(_.id).contains(_)).mkString(","))
                new PartitionMetadata(partitionId, leaderInfo, replicaInfo, isrInfo, ErrorMapping.NoError)
              } catch {
                case e: Throwable =>
                  debug("Error while fetching metadata for %s. Possible cause: %s".format(topicPartition, e.getMessage))
                  new PartitionMetadata(partitionId, leaderInfo, replicaInfo, isrInfo,
                    ErrorMapping.codeFor(e.getClass.asInstanceOf[Class[Throwable]]))
              }
          }
          topicResponses += new TopicMetadata(topic, partitionMetadata.toSeq)
        }
      }
    }
    topicResponses
  }

  def getAliveBrokers = {
    inReadLock(partitionMetadataLock) {
      aliveBrokers.values.toSeq
    }
  }

  def addOrUpdatePartitionInfo(topic: String,
                               partitionId: Int,
                               stateInfo: PartitionStateInfo) {
    inWriteLock(partitionMetadataLock) {
      cache.get(topic) match {
        case Some(infos) => infos.put(partitionId, stateInfo)
        case None => {
          val newInfos: mutable.Map[Int, PartitionStateInfo] = new mutable.HashMap[Int, PartitionStateInfo]
          cache.put(topic, newInfos)
          newInfos.put(partitionId, stateInfo)
        }
      }
    }
  }

  def getPartitionInfo(topic: String, partitionId: Int): Option[PartitionStateInfo] = {
    inReadLock(partitionMetadataLock) {
      cache.get(topic) match {
        case Some(partitionInfos) => partitionInfos.get(partitionId)
        case None => None
      }
    }
  }

  def updateCache(updateMetadataRequest: UpdateMetadataRequest,
                  brokerId: Int,
                  stateChangeLogger: StateChangeLogger): Unit = {

    inWriteLock(partitionMetadataLock) {
      // 根据 controller 发送的 <UpdateMetadataRequest> 来更新存活的 brokers
      aliveBrokers = updateMetadataRequest.aliveBrokers.map(b => (b.id, b)).toMap
      // 根据 controller 发送的 <UpdateMetadataRequest> 来更新 partition 状态信息
      updateMetadataRequest.partitionStateInfos.foreach { case(tp, info) =>
        if (info.leaderIsrAndControllerEpoch.leaderAndIsr.leader == LeaderAndIsr.LeaderDuringDelete) {
          removePartitionInfo(tp.topic, tp.partition)
        } else {
          addOrUpdatePartitionInfo(tp.topic, tp.partition, info)
        }
      }
    }
  }

  private def removePartitionInfo(topic: String, partitionId: Int) = {
    cache.get(topic) match {
      case Some(infos) => {
        infos.remove(partitionId)
        if(infos.isEmpty) {
          cache.remove(topic)
        }
        true
      }
      case None => false
    }
  }
}

