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

import org.I0Itec.zkclient.ZkClient
import kafka.common.TopicAndPartition
import kafka.utils.{Utils, ZkUtils, Logging}

trait PartitionAssignor {

  /**
   * Assigns partitions to consumer instances in a group.
   * @return An assignment map of partition to consumer thread. This only includes assignments for threads that belong
   *         to the given assignment-context's consumer.
   */
  def assign(ctx: AssignmentContext): scala.collection.Map[TopicAndPartition, ConsumerThreadId]

}

object PartitionAssignor {
  def createInstance(assignmentStrategy: String) = assignmentStrategy match {
    case "roundrobin" => new RoundRobinAssignor()
    case _ => new RangeAssignor()
  }
}

class AssignmentContext(group: String, val consumerId: String, excludeInternalTopics: Boolean, zkClient: ZkClient) {
  val myTopicThreadIds: collection.Map[String, collection.Set[ConsumerThreadId]] = {
    val myTopicCount = TopicCount.constructTopicCount(group, consumerId, zkClient, excludeInternalTopics)
    myTopicCount.getConsumerThreadIdsPerTopic
  }

  val partitionsForTopic: collection.Map[String, Seq[Int]] =
    ZkUtils.getPartitionsForTopics(zkClient, myTopicThreadIds.keySet.toSeq)

  val consumersForTopic: collection.Map[String, List[ConsumerThreadId]] =
    ZkUtils.getConsumersPerTopic(zkClient, group, excludeInternalTopics)

  val consumers: Seq[String] = ZkUtils.getConsumersInGroup(zkClient, group).sorted
}

/**
 * The round-robin partition assignor lays out all the available partitions and all the available consumer threads. It
 * then proceeds to do a round-robin assignment from partition to consumer thread. If the subscriptions of all consumer
 * instances are identical, then the partitions will be uniformly distributed. (i.e., the partition ownership counts
 * will be within a delta of exactly one across all consumer threads.)
 *
 * (For simplicity of implementation) the assignor is allowed to assign a given topic-partition to any consumer instance
 * and thread-id within that instance. Therefore, round-robin assignment is allowed only if:
 * a) Every topic has the same number of streams within a consumer instance
 * b) The set of subscribed topics is identical for every consumer instance within the group.
 */

class RoundRobinAssignor() extends PartitionAssignor with Logging {

  def assign(ctx: AssignmentContext): scala.collection.Map[TopicAndPartition, ConsumerThreadId] = {
    val partitionOwnershipDecision = collection.mutable.Map[TopicAndPartition, ConsumerThreadId]()

    //检查条件是否满足
    //取出第一个topic的Set<ConsumerThreadId>
    val (headTopic, headThreadIdSet) = (ctx.consumersForTopic.head._1, ctx.consumersForTopic.head._2.toSet)
    ctx.consumersForTopic.foreach { case (topic, threadIds) =>
      //使用订阅的所有的topic对应的Set<ConsumerThreadId>，和第一个比较，必须完全相同。
      val threadIdSet = threadIds.toSet
      require(threadIdSet == headThreadIdSet,
              "Round-robin assignment is allowed only if all consumers in the group subscribe to the same topics, " +
              "AND if the stream counts across topics are identical for a given consumer instance.\n" +
              "Topic %s has the following available consumer streams: %s\n".format(topic, threadIdSet) +
              "Topic %s has the following available consumer streams: %s\n".format(headTopic, headThreadIdSet))
    }

    //为所有的ConsumerThreadId创建一个循环迭代器
    val threadAssignor = Utils.circularIterator(headThreadIdSet.toSeq.sorted)

    info("Starting round-robin assignment with consumers " + ctx.consumers)
    val allTopicPartitions = ctx.partitionsForTopic.flatMap { case(topic, partitions) =>
      //将{topic} -> {partitions}的映射修改为TopicAndPartition列表
      info("Consumer %s rebalancing the following partitions for topic %s: %s"
           .format(ctx.consumerId, topic, partitions))
      partitions.map(partition => {
        TopicAndPartition(topic, partition)
      })
    }.toSeq.sortWith((topicPartition1, topicPartition2) => {
      //将TopicAndPartition列表按照hashCode随机排序，以减少某个topic的全部分区落在一个consumer的可能性
      topicPartition1.toString.hashCode < topicPartition2.toString.hashCode
    })

    //将TopicAndPartition列表分配到不同的ConsumerThread
    allTopicPartitions.foreach(topicPartition => {
      val threadId = threadAssignor.next()
      if (threadId.consumer == ctx.consumerId)
        partitionOwnershipDecision += (topicPartition -> threadId)
    })

    partitionOwnershipDecision
  }
}

/**
 * Range partitioning works on a per-topic basis. For each topic, we lay out the available partitions in numeric order
 * and the consumer threads in lexicographic order. We then divide the number of partitions by the total number of
 * consumer streams (threads) to determine the number of partitions to assign to each consumer. If it does not evenly
 * divide, then the first few consumers will have one extra partition. For example, suppose there are two consumers C1
 * and C2 with two streams each, and there are five available partitions (p0, p1, p2, p3, p4). So each consumer thread
 * will get at least one partition and the first consumer thread will get one extra partition. So the assignment will be:
 * p0 -> C1-0, p1 -> C1-0, p2 -> C1-1, p3 -> C2-0, p4 -> C2-1
 */
class RangeAssignor() extends PartitionAssignor with Logging {

  def assign(ctx: AssignmentContext):scala.collection.Map[TopicAndPartition, ConsumerThreadId]= {
    val partitionOwnershipDecision = collection.mutable.Map[TopicAndPartition, ConsumerThreadId]()

    for ((topic, consumerThreadIdSet) <- ctx.myTopicThreadIds) {
      //获取当前的消费者ID
      val curConsumers = ctx.consumersForTopic(topic)
      //获取目标partition
      val curPartitions: Seq[Int] = ctx.partitionsForTopic(topic)

      //每个ConsumerThread应该消费多少分区
      val nPartsPerConsumer = curPartitions.size / curConsumers.size
      //多出来的分区
      val nConsumersWithExtraPart = curPartitions.size % curConsumers.size

      info("Consumer " + ctx.consumerId + " rebalancing the following partitions: " + curPartitions +
        " for topic " + topic + " with consumers: " + curConsumers)

      for (consumerThreadId <- consumerThreadIdSet) {
        val myConsumerPosition = curConsumers.indexOf(consumerThreadId)
        assert(myConsumerPosition >= 0)
        /**
         * 假设有6个partition，2个consumer，每个consumer包含2个thread
         * 1. myConsumerPosition = 0, startPart = 0, nParts = 2
         * 2. myConsumerPosition = 1, startPart = 2, nParts = 2
         * 3. myConsumerPosition = 2, startPart = 4, nParts = 1
         * 4. myConsumerPosition = 3, startPart = 5, nParts = 1
         *
         * val startPart = myConsumerPosition + myConsumerPosition.min(2)
         * val nParts = 1 + (if (myConsumerPosition + 1 > 2) 0 else 1)
         *
         * 这个算法主要是计算两个值，{开始索引}和分到的partition数量。
         * 我们可以这样理解这个算法：
         * 1. nPartsPerConsumer 代表了每个区域最少负责的partition数量
         * 2. nConsumersWithExtraPart 表示分配完之后多出来的partition数量，而这个partition数量是一定小于我们的thread的数量的。
         *    那么，我们从索引 [0, 1, ...] 开始，每个thread分配一个剩余的partition。
         *
         * 所以，startPart = nPartsPerConsumer * myConsumerPosition + 步骤2所分配的partition数量。
         * 而步骤2所分配的partition数量就是当前索引和nConsumersWithExtraPart的最小值。
         * - 当myConsumerPosition < nConsumersWithExtraParts 时，我们还在继续分配，所以索引的偏移量和index有关；
         * - 当myConsumerPosition >= nConsumersWithExtraParts时，分配已经结束，后续的都加上这个值即可；
         *
         * 而nParts在nConsumersWithExtraParts时没有分配完时，需要分配 nPartsPerConsumer + 1，分配完之后就是 nPartsPerConsumer
         */
        //计算开始索引
        val startPart = nPartsPerConsumer * myConsumerPosition + myConsumerPosition.min(nConsumersWithExtraPart)
        //计算自己分配了几个partition
        val nParts = nPartsPerConsumer + (if (myConsumerPosition >= nConsumersWithExtraPart) 0 else 1)

        /**
         *   Range-partition the sorted partitions to consumers for better locality.
         *  The first few consumers pick up an extra partition, if any.
         */
        if (nParts <= 0)
          warn("No broker partitions consumed by consumer thread " + consumerThreadId + " for topic " + topic)
        else {
          for (i <- startPart until startPart + nParts) {
            val partition = curPartitions(i)
            info(consumerThreadId + " attempting to claim partition " + partition)
            // record the partition ownership decision
            partitionOwnershipDecision += (TopicAndPartition(topic, partition) -> consumerThreadId)
          }
        }
      }
    }

    partitionOwnershipDecision
  }
}
