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

package kafka.consumer

import kafka.client.ClientUtils
import kafka.cluster.{Broker, Cluster}
import kafka.common.TopicAndPartition
import kafka.server.{AbstractFetcherManager, AbstractFetcherThread, BrokerAndInitialOffset}
import kafka.utils.Utils.inLock
import kafka.utils.ZkUtils._
import kafka.utils.{ShutdownableThread, SystemTime}
import org.I0Itec.zkclient.ZkClient

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock
import scala.collection.{immutable, mutable}

/**
 *  Usage:
 *  Once ConsumerFetcherManager is created, startConnections() and stopAllConnections() can be called repeatedly
 *  until shutdown() is called.
 */
class ConsumerFetcherManager(private val consumerIdString: String,
                             private val config: ConsumerConfig,
                             private val zkClient : ZkClient)
        extends AbstractFetcherManager("ConsumerFetcherManager-%d".format(SystemTime.milliseconds),
                                       config.clientId, config.numConsumerFetchers) {
  private var partitionMap: immutable.Map[TopicAndPartition, PartitionTopicInfo] = _
  private var cluster: Cluster = _
  private val noLeaderPartitionSet = new mutable.HashSet[TopicAndPartition]
  private val lock = new ReentrantLock
  private val cond = lock.newCondition()
  private var leaderFinderThread: ShutdownableThread = _
  private val correlationId = new AtomicInteger(0)

  private class LeaderFinderThread(name: String) extends ShutdownableThread(name) {
    // thread responsible for adding the fetcher to the right broker when leader is available
    override def doWork(): Unit = {
      val leaderForPartitionsMap = new mutable.HashMap[TopicAndPartition, Broker]
      lock.lock()
      try {
        while (noLeaderPartitionSet.isEmpty) {
          trace("No partition for leader election.")
          cond.await()
        }

        trace("Partitions without leader %s".format(noLeaderPartitionSet))
        //遍历brokers并发送TopicMetadataRequest
        val brokers = getAllBrokersInCluster(zkClient)
        val topicsMetadata = ClientUtils.fetchTopicMetadata(noLeaderPartitionSet.map(m => m.topic).toSet,
                                                            brokers,
                                                            config.clientId,
                                                            config.socketTimeoutMs,
                                                            correlationId.getAndIncrement).topicsMetadata
        if(logger.isDebugEnabled) topicsMetadata.foreach(topicMetadata => debug(topicMetadata.toString()))
        //使用response更新leader不可用的set，更新成功从noLeaderPartitionSet中删除
        topicsMetadata.foreach { tmd =>
          val topic = tmd.topic
          tmd.partitionsMetadata.foreach { pmd =>
            val topicAndPartition = TopicAndPartition(topic, pmd.partitionId)
            if(pmd.leader.isDefined && noLeaderPartitionSet.contains(topicAndPartition)) {
              val leaderBroker = pmd.leader.get
              leaderForPartitionsMap.put(topicAndPartition, leaderBroker)
              noLeaderPartitionSet -= topicAndPartition
            }
          }
        }
      } catch {
        case t: Throwable => {
            if (!isRunning.get())
              throw t /* If this thread is stopped, propagate this exception to kill the thread. */
            else
              warn("Failed to find leader for %s".format(noLeaderPartitionSet), t)
          }
      } finally {
        lock.unlock()
      }

      try {
        //为新加入的leader添加ConsumerFetcherThread
        addFetcherForPartitions(leaderForPartitionsMap.map{
          case (topicAndPartition, broker) =>
            topicAndPartition -> BrokerAndInitialOffset(broker, partitionMap(topicAndPartition).getFetchOffset())}
        )
      } catch {
        case t: Throwable => {
          if (!isRunning.get())
            throw t /* If this thread is stopped, propagate this exception to kill the thread. */
          else {
            warn("Failed to add leader for partitions %s; will retry".format(leaderForPartitionsMap.keySet.mkString(",")), t)
            lock.lock()
            noLeaderPartitionSet ++= leaderForPartitionsMap.keySet
            lock.unlock()
          }
        }
      }

      shutdownIdleFetcherThreads()
      Thread.sleep(config.refreshLeaderBackoffMs)
    }
  }

  override def createFetcherThread(fetcherId: Int, sourceBroker: Broker): AbstractFetcherThread = {
    new ConsumerFetcherThread(
      "ConsumerFetcherThread-%s-%d-%d".format(consumerIdString, fetcherId, sourceBroker.id),
      config, sourceBroker, partitionMap, this)
  }

  def startConnections(topicInfos: Iterable[PartitionTopicInfo], cluster: Cluster): Unit = {
    leaderFinderThread = new LeaderFinderThread(consumerIdString + "-leader-finder-thread")
    leaderFinderThread.start()

    inLock(lock) {
      partitionMap = topicInfos.map(tpi => (TopicAndPartition(tpi.topic, tpi.partitionId), tpi)).toMap
      this.cluster = cluster
      noLeaderPartitionSet ++= topicInfos.map(tpi => TopicAndPartition(tpi.topic, tpi.partitionId))
      cond.signalAll()
    }
  }

  def stopConnections(): Unit = {
    /*
     * Stop the leader finder thread first before stopping fetchers. Otherwise, if there are more partitions without
     * leader, then the leader finder thread will process these partitions (before shutting down) and add fetchers for
     * these partitions.
     */
    info("Stopping leader finder thread")
    if (leaderFinderThread != null) {
      leaderFinderThread.shutdown()
      leaderFinderThread = null
    }

    info("Stopping all fetchers")
    closeAllFetchers()

    // no need to hold the lock for the following since leaderFindThread and all fetchers have been stopped
    partitionMap = null
    noLeaderPartitionSet.clear()

    info("All connections stopped")
  }

  def addPartitionsWithError(partitionList: Iterable[TopicAndPartition]): Unit = {
    debug("adding partitions with error %s".format(partitionList))
    inLock(lock) {
      if (partitionMap != null) {
        noLeaderPartitionSet ++= partitionList
        cond.signalAll()
      }
    }
  }
}