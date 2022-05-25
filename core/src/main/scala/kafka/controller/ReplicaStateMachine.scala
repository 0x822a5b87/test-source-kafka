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

import collection._
import collection.JavaConversions._
import java.util.concurrent.atomic.AtomicBoolean
import kafka.common.{TopicAndPartition, StateChangeFailedException}
import kafka.utils.{ZkUtils, ReplicationUtils, Logging}
import org.I0Itec.zkclient.IZkChildListener
import org.apache.log4j.Logger
import kafka.controller.Callbacks._
import kafka.utils.Utils._

/**
 * This class represents the state machine for replicas. It defines the states that a replica can be in, and
 * transitions to move the replica to another legal state. The different states that a replica can be in are -
 * 1. NewReplica        : The controller can create new replicas during partition reassignment. In this state, a
 *                        replica can only get become follower state change request.  Valid previous
 *                        state is NonExistentReplica
 * 2. OnlineReplica     : Once a replica is started and part of the assigned replicas for its partition, it is in this
 *                        state. In this state, it can get either become leader or become follower state change requests.
 *                        Valid previous state are NewReplica, OnlineReplica or OfflineReplica
 * 3. OfflineReplica    : If a replica dies, it moves to this state. This happens when the broker hosting the replica
 *                        is down. Valid previous state are NewReplica, OnlineReplica
 * 4. ReplicaDeletionStarted: If replica deletion starts, it is moved to this state. Valid previous state is OfflineReplica
 * 5. ReplicaDeletionSuccessful: If replica responds with no error code in response to a delete replica request, it is
 *                        moved to this state. Valid previous state is ReplicaDeletionStarted
 * 6. ReplicaDeletionIneligible: If replica deletion fails, it is moved to this state. Valid previous state is ReplicaDeletionStarted
 * 7. NonExistentReplica: If a replica is deleted successfully, it is moved to this state. Valid previous state is
 *                        ReplicaDeletionSuccessful
 */
class ReplicaStateMachine(controller: KafkaController) extends Logging {
  private val controllerContext = controller.controllerContext
  private val controllerId = controller.config.brokerId
  private val zkClient = controllerContext.zkClient
  private val replicaState: mutable.Map[PartitionAndReplica, ReplicaState] = mutable.Map.empty
  private val brokerChangeListener = new BrokerChangeListener()
  private val brokerRequestBatch = new ControllerBrokerRequestBatch(controller)
  private val hasStarted = new AtomicBoolean(false)
  private val stateChangeLogger = KafkaController.stateChangeLogger

  this.logIdent = "[Replica state machine on controller " + controller.config.brokerId + "]: "


  /**
   * Invoked on successful controller election. First registers a broker change listener since that triggers all
   * state transitions for replicas. Initializes the state of replicas for all partitions by reading from zookeeper.
   * Then triggers the OnlineReplica state change for all replicas.
   */
  def startup(): Unit = {
    // initialize replica state
    initializeReplicaState()
    // set started flag
    hasStarted.set(true)
    // move all Online replicas to Online
    handleStateChanges(controllerContext.allLiveReplicas(), OnlineReplica)

    info("Started replica state machine with initial state -> " + replicaState.toString())
  }

  // register ZK listeners of the replica state machine
  def registerListeners(): Unit = {
    // register broker change listener
    registerBrokerChangeListener()
  }

  // de-register ZK listeners of the replica state machine
  def deregisterListeners(): Unit = {
    // de-register broker change listener
    deregisterBrokerChangeListener()
  }

  /**
   * Invoked on controller shutdown.
   */
  def shutdown() {
    // reset started flag
    hasStarted.set(false)
    // reset replica state
    replicaState.clear()
    // de-register all ZK listeners
    deregisterListeners()

    info("Stopped replica state machine")
  }

  /**
   * This API is invoked by the broker change controller callbacks and the startup API of the state machine
   * @param replicas     The list of replicas (brokers) that need to be transitioned to the target state
   * @param targetState  The state that the replicas should be moved to
   * The controller's allLeaders cache should have been updated before this
   */
  def handleStateChanges(replicas: Set[PartitionAndReplica], targetState: ReplicaState,
                         callbacks: Callbacks = (new CallbackBuilder).build): Unit = {
    if(replicas.nonEmpty) {
      info("Invoking state change to %s for replicas %s".format(targetState, replicas.mkString(",")))
      try {
        brokerRequestBatch.newBatch()
        // 将所有的 replica 修改到 targetState
        replicas.foreach(r => handleStateChange(r, targetState, callbacks))
        // 发送 LeaderAndIsrRequest、UpdateMetadataRequest、StopReplicaRequest 到 broker
        brokerRequestBatch.sendRequestsToBrokers(controller.epoch, controllerContext.correlationId.getAndIncrement)
      }catch {
        case e: Throwable => error("Error while moving some replicas to %s state".format(targetState), e)
      }
    }
  }

  /**
   * This API exercises the replica's state machine. It ensures that every state transition happens from a legal
   * previous state to the target state. Valid state transitions are:
   * NonExistentReplica --> NewReplica
   * --send LeaderAndIsr request with current leader and isr to the new replica and UpdateMetadata request for the
   *   partition to every live broker
   *
   * NewReplica -> OnlineReplica
   * --add the new replica to the assigned replica list if needed
   *
   * OnlineReplica,OfflineReplica -> OnlineReplica
   * --send LeaderAndIsr request with current leader and isr to the new replica and UpdateMetadata request for the
   *   partition to every live broker
   *
   * NewReplica,OnlineReplica,OfflineReplica,ReplicaDeletionIneligible -> OfflineReplica
   * --send StopReplicaRequest to the replica (w/o deletion)
   * --remove this replica from the isr and send LeaderAndIsr request (with new isr) to the leader replica and
   *   UpdateMetadata request for the partition to every live broker.
   *
   * OfflineReplica -> ReplicaDeletionStarted
   * --send StopReplicaRequest to the replica (with deletion)
   *
   * ReplicaDeletionStarted -> ReplicaDeletionSuccessful
   * -- mark the state of the replica in the state machine
   *
   * ReplicaDeletionStarted -> ReplicaDeletionIneligible
   * -- mark the state of the replica in the state machine
   *
   * ReplicaDeletionSuccessful -> NonExistentReplica
   * -- remove the replica from the in memory partition replica assignment cache


   * @param partitionAndReplica The replica for which the state transition is invoked
   * @param targetState The end state that the replica should be moved to
   */
  def handleStateChange(partitionAndReplica: PartitionAndReplica, targetState: ReplicaState,
                        callbacks: Callbacks): Unit = {
    val topic = partitionAndReplica.topic
    val partition = partitionAndReplica.partition
    val replicaId = partitionAndReplica.replica
    val topicAndPartition = TopicAndPartition(topic, partition)
    if (!hasStarted.get)
      throw new StateChangeFailedException(("Controller %d epoch %d initiated state change of replica %d for partition %s " +
                                            "to %s failed because replica state machine has not started")
                                              .format(controllerId, controller.epoch, replicaId, topicAndPartition, targetState))
    // 如果当前replica没有创建则创建一个 NonExistentReplica
    val currState = replicaState.getOrElseUpdate(partitionAndReplica, NonExistentReplica)
    try {
      val replicaAssignment = controllerContext.partitionReplicaAssignment(topicAndPartition)
      targetState match {
        case NewReplica => {
          assertValidPreviousStates(partitionAndReplica, List(NonExistentReplica), targetState)
          // 启动replica作为它分区的当前leader的follower
          val leaderIsrAndControllerEpochOpt = ReplicationUtils.getLeaderIsrAndEpochForPartition(zkClient, topic, partition)
          leaderIsrAndControllerEpochOpt match {
            // 由于是从 NonExistentReplica 变为 NewReplica，所以此时应该没有 leader
            // 如果当前 replica 是 leader 则直接抛出异常，否则发送 LeaderAndIsrRequest
            case Some(leaderIsrAndControllerEpoch) =>
              if (leaderIsrAndControllerEpoch.leaderAndIsr.leader == replicaId)
                throw new StateChangeFailedException("Replica %d for partition %s cannot be moved to NewReplica"
                  .format(replicaId, topicAndPartition) + "state as it is being requested to become leader")
              brokerRequestBatch.addLeaderAndIsrRequestForBrokers(List(replicaId),
                topic, partition, leaderIsrAndControllerEpoch,
                replicaAssignment)
            case None => // new leader request will be sent to this replica when one gets elected
          }
          replicaState.put(partitionAndReplica, NewReplica)
          stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s"
            .format(controllerId, controller.epoch, replicaId, topicAndPartition, currState,
              targetState))
        }
        case ReplicaDeletionStarted => {
          // 标记 OfflineReplica 为 ReplicaDeletionStarted 并发送 StopReplicaRequest 到 broker
          assertValidPreviousStates(partitionAndReplica, List(OfflineReplica), targetState)
          replicaState.put(partitionAndReplica, ReplicaDeletionStarted)
          brokerRequestBatch.addStopReplicaRequestForBrokers(List(replicaId), topic, partition, deletePartition = true,
            callbacks.stopReplicaResponseCallback)
          stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s"
            .format(controllerId, controller.epoch, replicaId, topicAndPartition, currState, targetState))
        }
        case ReplicaDeletionIneligible => {
          // 仅仅标记replica为ReplicaDeletionIneligible
          assertValidPreviousStates(partitionAndReplica, List(ReplicaDeletionStarted), targetState)
          replicaState.put(partitionAndReplica, ReplicaDeletionIneligible)
          stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s"
            .format(controllerId, controller.epoch, replicaId, topicAndPartition, currState, targetState))
        }
        case ReplicaDeletionSuccessful => {
          // 仅仅标记replica为ReplicaDeletionSuccessful
          assertValidPreviousStates(partitionAndReplica, List(ReplicaDeletionStarted), targetState)
          replicaState.put(partitionAndReplica, ReplicaDeletionSuccessful)
          stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s"
            .format(controllerId, controller.epoch, replicaId, topicAndPartition, currState, targetState))
        }
        case NonExistentReplica => {
          assertValidPreviousStates(partitionAndReplica, List(ReplicaDeletionSuccessful), targetState)
          // 删除 replica 在 AR 的信息
          val currentAssignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)
          controllerContext.partitionReplicaAssignment.put(topicAndPartition, currentAssignedReplicas.filterNot(_ == replicaId))
          // 删除 replica 的 replicaState 信息
          replicaState.remove(partitionAndReplica)
          stateChangeLogger.trace("Controller %d epoch %d changed state of replica %d for partition %s from %s to %s"
            .format(controllerId, controller.epoch, replicaId, topicAndPartition, currState, targetState))
        }
        case OnlineReplica => {
          // 检验 PrevStatus
          assertValidPreviousStates(partitionAndReplica,
            List(NewReplica, OnlineReplica, OfflineReplica, ReplicaDeletionIneligible), targetState)
          replicaState(partitionAndReplica) match {
            case NewReplica =>
              // 从 NewReplica -> OnlineReplica 只需要将replicaId添加到ControllerContext即可
              // <TopicAndPartition> -> Seq[Int] -> replicaId
              val currentAssignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)
              if (!currentAssignedReplicas.contains(replicaId))
                controllerContext.partitionReplicaAssignment.put(topicAndPartition, currentAssignedReplicas :+ replicaId)
            case _ =>
              // OnlineReplica, OfflineReplica, ReplicaDeletionIneligible -> OnlineReplica
              controllerContext.partitionLeadershipInfo.get(topicAndPartition) match {
                case Some(leaderIsrAndControllerEpoch) =>
                  // 如果leader存在，向replicaId对应的broker发送LeaderAndIsrRequest
                  brokerRequestBatch.addLeaderAndIsrRequestForBrokers(List(replicaId), topic, partition, leaderIsrAndControllerEpoch,
                    replicaAssignment)
                  // 修改replica状态
                  replicaState.put(partitionAndReplica, OnlineReplica)
                case None =>
                // 如果没有leader，说明partition从来没有进入过OnlinePartition，
                // 也意味着broker从来没有为partition启动一个log，也没有拥有过HighWatermark
              }
          }
          replicaState.put(partitionAndReplica, OnlineReplica)
        }
        case OfflineReplica => {
          // 检验PrevState
          assertValidPreviousStates(partitionAndReplica,
            List(NewReplica, OnlineReplica, OfflineReplica, ReplicaDeletionIneligible), targetState)
          // 向replicaId对应的broker发送StopReplicaRequest以便于停止replica向leader fetch数据
          brokerRequestBatch.addStopReplicaRequestForBrokers(List(replicaId), topic, partition, deletePartition = false)
          // 作为一个优化，controller从ISR中删除了死亡的replicas
          // 1. 不存在leader，则 leaderAndIsrIsEmpty = true
          // 2. 存在leader，但是replica是leader，或者说删除了leader之后没有足够的ISR，则 leaderAndIsrIsEmpty = true
          val leaderAndIsrIsEmpty: Boolean =
            controllerContext.partitionLeadershipInfo.get(topicAndPartition) match {
              // 如果replica存在leader
              case Some(_) => {
                // Removes a given partition replica from the ISR;
                // if it is not the current leader and there are sufficient remaining replicas in ISR.
                // 将当前replica从ISR中删除
                controller.removeReplicaFromIsr(topic, partition, replicaId) match {
                  case Some(updatedLeaderIsrAndControllerEpoch) =>
                    // send the shrunk ISR state change request to all the remaining alive replicas of the partition.
                    val currentAssignedReplicas = controllerContext.partitionReplicaAssignment(topicAndPartition)
                    if (!controller.deleteTopicManager.isPartitionToBeDeleted(topicAndPartition)) {
                      brokerRequestBatch.addLeaderAndIsrRequestForBrokers(currentAssignedReplicas.filterNot(_ == replicaId),
                        topic, partition, updatedLeaderIsrAndControllerEpoch, replicaAssignment)
                    }
                    replicaState.put(partitionAndReplica, OfflineReplica)
                    false
                  case None =>
                    true
                }
              }
              case None =>
                true
            }
          if (leaderAndIsrIsEmpty)
            throw new StateChangeFailedException(
              "Failed to change state of replica %d for partition %s since the leader and isr path in zookeeper is empty"
                .format(replicaId, topicAndPartition))
        }
      }
    }
    catch {
      case t: Throwable =>
        stateChangeLogger.error("Controller %d epoch %d initiated state change of replica %d for partition [%s,%d] from %s to %s failed"
                                  .format(controllerId, controller.epoch, replicaId, topic, partition, currState, targetState), t)
    }
  }

  def areAllReplicasForTopicDeleted(topic: String): Boolean = {
    val replicasForTopic = controller.controllerContext.replicasForTopic(topic)
    val replicaStatesForTopic = replicasForTopic.map(r => (r, replicaState(r))).toMap
    debug("Are all replicas for topic %s deleted %s".format(topic, replicaStatesForTopic))
    replicaStatesForTopic.foldLeft(true)((deletionState, r) => deletionState && r._2 == ReplicaDeletionSuccessful)
  }

  def isAtLeastOneReplicaInDeletionStartedState(topic: String): Boolean = {
    val replicasForTopic = controller.controllerContext.replicasForTopic(topic)
    val replicaStatesForTopic = replicasForTopic.map(r => (r, replicaState(r))).toMap
    replicaStatesForTopic.foldLeft(false)((deletionState, r) => deletionState || r._2 == ReplicaDeletionStarted)
  }

  def replicasInState(topic: String, state: ReplicaState): Set[PartitionAndReplica] = {
    replicaState.filter(r => r._1.topic.equals(topic) && r._2 == state).keySet
  }

  def isAnyReplicaInState(topic: String, state: ReplicaState): Boolean = {
    replicaState.exists(r => r._1.topic.equals(topic) && r._2 == state)
  }

  def replicasInDeletionStates(topic: String): Set[PartitionAndReplica] = {
    val deletionStates = Set(ReplicaDeletionStarted, ReplicaDeletionSuccessful, ReplicaDeletionIneligible)
    replicaState.filter(r => r._1.topic.equals(topic) && deletionStates.contains(r._2)).keySet
  }

  private def assertValidPreviousStates(partitionAndReplica: PartitionAndReplica, fromStates: Seq[ReplicaState],
                                        targetState: ReplicaState): Unit = {
    assert(fromStates.contains(replicaState(partitionAndReplica)),
      "Replica %s should be in the %s states before moving to %s state"
        .format(partitionAndReplica, fromStates.mkString(","), targetState) +
        ". Instead it is in %s state".format(replicaState(partitionAndReplica)))
  }

  private def registerBrokerChangeListener() = {
    zkClient.subscribeChildChanges(ZkUtils.BrokerIdsPath, brokerChangeListener)
  }

  private def deregisterBrokerChangeListener() = {
    zkClient.unsubscribeChildChanges(ZkUtils.BrokerIdsPath, brokerChangeListener)
  }

  /**
   * Invoked on startup of the replica's state machine to set the initial state for replicas of all existing partitions
   * in zookeeper
   */
  private def initializeReplicaState(): Unit = {
    // <TopicAndPartition> -> Seq[Int]，保存了 <TopicAndPartition> 对应的所有AR
    for((topicPartition, assignedReplicas) <- controllerContext.partitionReplicaAssignment) {
      val topic = topicPartition.topic
      val partition = topicPartition.partition
      // replicaId 是某个 <TopicAndPartition> 的 replica 的 id
      assignedReplicas.foreach { replicaId =>
        val partitionAndReplica = PartitionAndReplica(topic, partition, replicaId)
        // 如果某一个 PartitionAndReplica 所在的 broker 是存活的则初始化为 OnlineReplica，否则初始化为 ReplicaDeletionIneligible
        if (controllerContext.liveBrokerIds.contains(replicaId)) {
          replicaState.put(partitionAndReplica, OnlineReplica)
        } else {
          replicaState.put(partitionAndReplica, ReplicaDeletionIneligible)
        }
      }
    }
  }

  def partitionsAssignedToBroker(topics: Seq[String], brokerId: Int):Seq[TopicAndPartition] = {
    controllerContext.partitionReplicaAssignment.filter(_._2.contains(brokerId)).keySet.toSeq
  }

  /**
   * This is the zookeeper listener that triggers all the state transitions for a replica
   */
  class BrokerChangeListener() extends IZkChildListener with Logging {
    this.logIdent = "[BrokerChangeListener on Controller " + controller.config.brokerId + "]: "
    def handleChildChange(parentPath : String, currentBrokerList : java.util.List[String]) {
      info("Broker change listener fired for path %s with children %s".format(parentPath, currentBrokerList.mkString(",")))
      inLock(controllerContext.controllerLock) {
        if (hasStarted.get) {
          ControllerStats.leaderElectionTimer.time {
            try {
              val curBrokerIds = currentBrokerList.map(_.toInt).toSet
              val newBrokerIds = curBrokerIds -- controllerContext.liveOrShuttingDownBrokerIds
              val newBrokerInfo = newBrokerIds.map(ZkUtils.getBrokerInfo(zkClient, _))
              val newBrokers = newBrokerInfo.filter(_.isDefined).map(_.get)
              val deadBrokerIds = controllerContext.liveOrShuttingDownBrokerIds -- curBrokerIds
              controllerContext.liveBrokers = curBrokerIds.map(ZkUtils.getBrokerInfo(zkClient, _)).filter(_.isDefined).map(_.get)
              info("Newly added brokers: %s, deleted brokers: %s, all live brokers: %s"
                .format(newBrokerIds.mkString(","), deadBrokerIds.mkString(","), controllerContext.liveBrokerIds.mkString(",")))
              newBrokers.foreach(controllerContext.controllerChannelManager.addBroker(_))
              deadBrokerIds.foreach(controllerContext.controllerChannelManager.removeBroker(_))
              if(newBrokerIds.size > 0)
                controller.onBrokerStartup(newBrokerIds.toSeq)
              if(deadBrokerIds.size > 0)
                controller.onBrokerFailure(deadBrokerIds.toSeq)
            } catch {
              case e: Throwable => error("Error while handling broker changes", e)
            }
          }
        }
      }
    }
  }
}

sealed trait ReplicaState { def state: Byte }
case object NewReplica extends ReplicaState { val state: Byte = 1 }
case object OnlineReplica extends ReplicaState { val state: Byte = 2 }
case object OfflineReplica extends ReplicaState { val state: Byte = 3 }
case object ReplicaDeletionStarted extends ReplicaState { val state: Byte = 4}
case object ReplicaDeletionSuccessful extends ReplicaState { val state: Byte = 5}
case object ReplicaDeletionIneligible extends ReplicaState { val state: Byte = 6}
case object NonExistentReplica extends ReplicaState { val state: Byte = 7 }
