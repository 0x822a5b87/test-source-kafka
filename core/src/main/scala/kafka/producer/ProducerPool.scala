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
import kafka.cluster.Broker
import kafka.common.UnavailableProducerException
import kafka.utils.Logging

import java.util.Properties
import scala.collection.mutable


object ProducerPool {
  /**
   * 用来创建连接broker的[[SyncProducer]]
   */
  def createSyncProducer(config: ProducerConfig, broker: Broker): SyncProducer = {
    val props = new Properties()
    props.put("host", broker.host)
    props.put("port", broker.port.toString)
    props.putAll(config.props.props)
    new SyncProducer(new SyncProducerConfig(props))
  }
}

class ProducerPool(val config: ProducerConfig) extends Logging {
  /**
   * 保存了所有到broker的[[SyncProducer]]
   */
  private val syncProducers = new mutable.HashMap[Int, SyncProducer]
  private val lock = new Object()

  /**
   * 基于[[TopicMetadata]]更新broker的连接
   * @param topicMetadata [[kafka.api.TopicMetadataResponse.topicsMetadata]]
   */
  def updateProducer(topicMetadata: Seq[TopicMetadata]): Unit = {
    val newBrokers = new collection.mutable.HashSet[Broker]
    topicMetadata.foreach(tmd => {
      tmd.partitionsMetadata.foreach(pmd => {
        /**
         * 1. 可以看到，所有的[[SyncProducer]]都是连接到leader的
         * 2. 对于每一个存在的PartitionMetadata都会建立建立一个连接，但是由于是HashSet[Broker]，所以对于一台机器上
         *    存在多个Partition，只会建立一个连接，这个可以有效的降低连接数。
         */
        if (pmd.leader.isDefined)
          newBrokers += pmd.leader.get
      })
    })
    lock synchronized {
      newBrokers.foreach(b => {
        /**
         * 更新[[syncProducers]]中的[[SyncProducer]]
         */
        if(syncProducers.contains(b.id)){
          syncProducers(b.id).close()
          syncProducers.put(b.id, ProducerPool.createSyncProducer(config, b))
        } else
          syncProducers.put(b.id, ProducerPool.createSyncProducer(config, b))
      })
    }
  }

  def getProducer(brokerId: Int) : SyncProducer = {
    lock.synchronized {
      val producer = syncProducers.get(brokerId)
      producer match {
        case Some(p) => p
        case None => throw new UnavailableProducerException("Sync producer for broker id %d does not exist".format(brokerId))
      }
    }
  }

  /**
   * Closes all the producers in the pool
   */
  def close(): Unit = {
    lock.synchronized {
      info("Closing all sync producers")
      val iter = syncProducers.values.iterator
      while(iter.hasNext)
        iter.next.close()
    }
  }
}
