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
package kafka.controller

import kafka.admin.AdminUtils
import kafka.api.LeaderAndIsr
import kafka.log.LogConfig
import kafka.utils.Logging
import kafka.common.{LeaderElectionNotNeededException, TopicAndPartition, StateChangeFailedException, NoReplicaOnlineException}
import kafka.server.KafkaConfig

trait PartitionLeaderSelector {

  /**
   * @param topicAndPartition          The topic and partition whose leader needs to be elected
   * @param currentLeaderAndIsr        The current leader and isr of input partition read from zookeeper
   * @throws NoReplicaOnlineException If no replica in the assigned replicas list is alive
   * @return The leader and isr request, with the newly selected leader and isr, and the set of replicas to receive
   * the LeaderAndIsrRequest.
   */
  def selectLeader(topicAndPartition: TopicAndPartition, currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int])

}

/**
 * Select the new leader, new isr and receiving replicas (for the LeaderAndIsrRequest):
 * 1. If at least one broker from the isr is alive, it picks a broker from the live isr as the new leader and the live
 *    isr as the new isr.
 * 2. Else, if unclean leader election for the topic is disabled, it throws a NoReplicaOnlineException.
 * 3. Else, it picks some alive broker from the assigned replica list as the new leader and the new isr.
 * 4. If no broker in the assigned replica list is alive, it throws a NoReplicaOnlineException
 * Replicas to receive LeaderAndIsr request = live assigned replicas
 * Once the leader is successfully registered in zookeeper, it updates the allLeaders cache
 */
class OfflinePartitionLeaderSelector(controllerContext: ControllerContext, config: KafkaConfig)
  extends PartitionLeaderSelector with Logging {
  this.logIdent = "[OfflinePartitionLeaderSelector]: "

  def selectLeader(topicAndPartition: TopicAndPartition, currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int]) = {
    controllerContext.partitionReplicaAssignment.get(topicAndPartition) match {
      case Some(assignedReplicas) =>
        // live AR
        val liveAssignedReplicas = assignedReplicas.filter(r => controllerContext.liveBrokerIds.contains(r))
        // live ISR
        val liveBrokersInIsr = currentLeaderAndIsr.isr.filter(r => controllerContext.liveBrokerIds.contains(r))
        val currentLeaderEpoch = currentLeaderAndIsr.leaderEpoch
        val currentLeaderIsrZkPathVersion = currentLeaderAndIsr.zkVersion
        val newLeaderAndIsr = liveBrokersInIsr.isEmpty match {
          // live ISR 为空
          case true =>
            // 如果不允许从 AR 中选举那么直接抛出异常
            if (!LogConfig.fromProps(config.props.props, AdminUtils.fetchTopicConfig(controllerContext.zkClient,
              topicAndPartition.topic)).uncleanLeaderElectionEnable) {
              throw new NoReplicaOnlineException(("No broker in ISR for partition " +
                "%s is alive. Live brokers are: [%s],".format(topicAndPartition, controllerContext.liveBrokerIds)) +
                " ISR brokers are: [%s]".format(currentLeaderAndIsr.isr.mkString(",")))
            }

            debug("No broker in ISR is alive for %s. Pick the leader from the alive assigned replicas: %s"
              .format(topicAndPartition, liveAssignedReplicas.mkString(",")))

            liveAssignedReplicas.isEmpty match {
              case true =>
                // AR 也为空则抛出异常
                throw new NoReplicaOnlineException(("No replica for partition " +
                  "%s is alive. Live brokers are: [%s],".format(topicAndPartition, controllerContext.liveBrokerIds)) +
                  " Assigned replicas are: [%s]".format(assignedReplicas))
              case false =>
                // 从 AR 中选择第一个作为新的 leader、ISR
                // 注意，我们这里返回的 ISR 里面只有 AR 的第一个
                ControllerStats.uncleanLeaderElectionRate.mark()
                val newLeader = liveAssignedReplicas.head
                warn("No broker in ISR is alive for %s. Elect leader %d from live brokers %s. There's potential data loss."
                  .format(topicAndPartition, newLeader, liveAssignedReplicas.mkString(",")))
                new LeaderAndIsr(newLeader, currentLeaderEpoch + 1, List(newLeader), currentLeaderIsrZkPathVersion + 1)
            }

          // live ISR 不为空，
          case false =>
            val liveReplicasInIsr = liveAssignedReplicas.filter(r => liveBrokersInIsr.contains(r))
            val newLeader = liveReplicasInIsr.head
            debug("Some broker in ISR is alive for %s. Select %d from ISR %s to be the leader."
              .format(topicAndPartition, newLeader, liveBrokersInIsr.mkString(",")))
            // 得到选举结果
            new LeaderAndIsr(newLeader, currentLeaderEpoch + 1, liveBrokersInIsr.toList, currentLeaderIsrZkPathVersion + 1)
        }
        info("Selected new leader and ISR %s for offline partition %s".format(newLeaderAndIsr.toString(), topicAndPartition))
        (newLeaderAndIsr, liveAssignedReplicas)
      case None =>
        throw new NoReplicaOnlineException("Partition %s doesn't have replicas assigned to it".format(topicAndPartition))
    }
  }
}

/**
 * New leader = a live in-sync reassigned replica
 * New isr = current isr
 * Replicas to receive LeaderAndIsr request = reassigned replicas
 */
class ReassignedPartitionLeaderSelector(controllerContext: ControllerContext) extends PartitionLeaderSelector with Logging {
  this.logIdent = "[ReassignedPartitionLeaderSelector]: "

  /**
   * The reassigned replicas are already in the ISR when selectLeader is called.
   */
  def selectLeader(topicAndPartition: TopicAndPartition, currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int]) = {
    // 获取指定AR列表
    val reassignedInSyncReplicas = controllerContext.partitionsBeingReassigned(topicAndPartition).newReplicas
    val currentLeaderEpoch = currentLeaderAndIsr.leaderEpoch
    val currentLeaderIsrZkPathVersion = currentLeaderAndIsr.zkVersion
    // 在指定AR列表中获取满足 live 和 ISR 的列表
    val aliveReassignedInSyncReplicas = reassignedInSyncReplicas.filter(r => controllerContext.liveBrokerIds.contains(r) &&
                                                                             currentLeaderAndIsr.isr.contains(r))
    val newLeaderOpt = aliveReassignedInSyncReplicas.headOption
    newLeaderOpt match {
      // 如果列表不为空使用第一个作为新的leader
      case Some(newLeader) => (new LeaderAndIsr(newLeader, currentLeaderEpoch + 1, currentLeaderAndIsr.isr,
        currentLeaderIsrZkPathVersion + 1), reassignedInSyncReplicas)
      case None =>
        reassignedInSyncReplicas.size match {
          case 0 =>
            throw new NoReplicaOnlineException("List of reassigned replicas for partition " +
              " %s is empty. Current leader and ISR: [%s]".format(topicAndPartition, currentLeaderAndIsr))
          case _ =>
            throw new NoReplicaOnlineException("None of the reassigned replicas for partition " +
              "%s are in-sync with the leader. Current leader and ISR: [%s]".format(topicAndPartition, currentLeaderAndIsr))
        }
    }
  }
}

/**
 * New leader = preferred (first assigned) replica (if in isr and alive);
 * New isr = current isr;
 * Replicas to receive LeaderAndIsr request = assigned replicas
 */
class PreferredReplicaPartitionLeaderSelector(controllerContext: ControllerContext) extends PartitionLeaderSelector
with Logging {
  this.logIdent = "[PreferredReplicaPartitionLeaderSelector]: "

  def selectLeader(topicAndPartition: TopicAndPartition, currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int]) = {
    // 获取AR
    val assignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)
    // 从AR中选择第一个
    val preferredReplica = assignedReplicas.head
    // 检查 preferred replica 是否是当前的 leader
    val currentLeader = controllerContext.partitionLeadershipInfo(topicAndPartition).leaderAndIsr.leader
    // 如果 preferred replica 是当前 leader 则抛出异常，这个异常会在外部被 swallow 无需处理
    if (currentLeader == preferredReplica) {
      throw new LeaderElectionNotNeededException("Preferred replica %d is already the current leader for partition %s"
                                                   .format(preferredReplica, topicAndPartition))
    } else {
      info("Current leader %d for partition %s is not the preferred replica.".format(currentLeader, topicAndPartition) +
        " Triggering preferred replica leader election")
      // 如果 preferred replica 满足 alive 以及是一个 ISR，则被选举为新的leader
      if (controllerContext.liveBrokerIds.contains(preferredReplica) && currentLeaderAndIsr.isr.contains(preferredReplica)) {
        (new LeaderAndIsr(preferredReplica, currentLeaderAndIsr.leaderEpoch + 1, currentLeaderAndIsr.isr,
          currentLeaderAndIsr.zkVersion + 1), assignedReplicas)
      } else {
        throw new StateChangeFailedException("Preferred replica %d for partition ".format(preferredReplica) +
          "%s is either not alive or not in the isr. Current leader and ISR: [%s]".format(topicAndPartition, currentLeaderAndIsr))
      }
    }
  }
}

/**
 * New leader = replica in isr that's not being shutdown;
 * New isr = current isr - shutdown replica;
 * Replicas to receive LeaderAndIsr request = live assigned replicas
 */
class ControlledShutdownLeaderSelector(controllerContext: ControllerContext)
        extends PartitionLeaderSelector
        with Logging {

  this.logIdent = "[ControlledShutdownLeaderSelector]: "

  def selectLeader(topicAndPartition: TopicAndPartition, currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int]) = {
    val currentLeaderEpoch = currentLeaderAndIsr.leaderEpoch
    val currentLeaderIsrZkPathVersion = currentLeaderAndIsr.zkVersion

    val currentLeader = currentLeaderAndIsr.leader

    // 获取AR
    val assignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)
    // 获取存活的 brokerIds
    val liveOrShuttingDownBrokerIds = controllerContext.liveOrShuttingDownBrokerIds
    // 获取 live AR
    val liveAssignedReplicas = assignedReplicas.filter(r => liveOrShuttingDownBrokerIds.contains(r))

    // 只保留在live broker上的isr
    val newIsr = currentLeaderAndIsr.isr.filter(brokerId => !controllerContext.shuttingDownBrokerIds.contains(brokerId))
    // 选择live isr的第一条
    val newLeaderOpt = newIsr.headOption
    newLeaderOpt match {
      case Some(newLeader) =>
        debug("Partition %s : current leader = %d, new leader = %d"
              .format(topicAndPartition, currentLeader, newLeader))
        (LeaderAndIsr(newLeader, currentLeaderEpoch + 1, newIsr, currentLeaderIsrZkPathVersion + 1),
         liveAssignedReplicas)
      case None =>
        throw new StateChangeFailedException(("No other replicas in ISR %s for %s besides" +
          " shutting down brokers %s").format(currentLeaderAndIsr.isr.mkString(","), topicAndPartition, controllerContext.shuttingDownBrokerIds.mkString(",")))
    }
  }
}

/**
 * Essentially does nothing. Returns the current leader and ISR, and the current
 * set of replicas assigned to a given topic/partition.
 */
class NoOpLeaderSelector(controllerContext: ControllerContext) extends PartitionLeaderSelector with Logging {

  this.logIdent = "[NoOpLeaderSelector]: "

  def selectLeader(topicAndPartition: TopicAndPartition, currentLeaderAndIsr: LeaderAndIsr): (LeaderAndIsr, Seq[Int]) = {
    warn("I should never have been asked to perform leader election, returning the current LeaderAndIsr and replica assignment.")
    (currentLeaderAndIsr, controllerContext.partitionReplicaAssignment(topicAndPartition))
  }
}
