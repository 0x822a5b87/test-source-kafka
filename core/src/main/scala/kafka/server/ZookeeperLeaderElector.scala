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

import kafka.utils.ZkUtils._
import kafka.utils.Utils._
import kafka.utils.{Json, SystemTime, Logging}
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import org.I0Itec.zkclient.IZkDataListener
import kafka.controller.ControllerContext
import kafka.controller.KafkaController

/**
 * This class handles zookeeper based leader election based on an ephemeral path. The election module does not handle
 * session expiration, instead it assumes the caller will handle it by probably try to re-elect again. If the existing
 * leader is dead, this class will handle automatic re-election and if it succeeds, it invokes the leader state change
 * callback
 */
class ZookeeperLeaderElector(controllerContext: ControllerContext,
                             electionPath: String,
                             onBecomingLeader: () => Unit,
                             onResigningAsLeader: () => Unit,
                             brokerId: Int)
  extends LeaderElector with Logging {
  // 初始化 leaderId
  var leaderId: Int = -1
  // create the election path in ZK, if one does not exist
  val index: Int = electionPath.lastIndexOf("/")
  if (index > 0)
    makeSurePersistentPathExists(controllerContext.zkClient, electionPath.substring(0, index))
  // leader 变化时的回调函数，包含了两个回调函数
  // handleDataChange  : 当 electionPath 路径上的节点数据被修改时，修改 leaderId；
  // handleDataDeleted : 当 electionPath 路径上的节点数据被删除（说明leader失效），进入leader选举流程。
  val leaderChangeListener = new LeaderChangeListener

  /**
   * startup 在 KafkaController#startup 中调用：
   * 1. 首先在 electionPath 上注册回调函数；
   * 2. 随后进入选举流程。
   */
  def startup: Unit = {
    inLock(controllerContext.controllerLock) {
      controllerContext.zkClient.subscribeDataChanges(electionPath, leaderChangeListener)
      elect
    }
  }

  /**
   * 从electionPath读取leader相关信息，如果已经有broker注册为leader返回leaderId，否则返回 -1
   * @return
   */
  private def getControllerID: Int = {
    readDataMaybeNull(controllerContext.zkClient, electionPath)._1 match {
       case Some(controller) => KafkaController.parseControllerId(controller)
       case None => -1
    }
  }

  /**
   * controller 选举流程
   * @return 成为leader返回true，否则返回false
   */
  def elect: Boolean = {
    val timestamp = SystemTime.milliseconds.toString
    val electString = Json.encode(Map("version" -> 1, "brokerId" -> brokerId, "timestamp" -> timestamp))
   
   leaderId = getControllerID
    /* 
     * 我们可能在startup或者handleDeleted ZK回调函数中参与选举，由于潜在的竞争条件，当我们到达这里时，controller 选举可能已经结束了。
     * 如果此代理已经是控制器，则此检查将防止以下 createEphemeralPath 方法进入无限循环。
     */
    if(leaderId != -1) {
       debug("Broker %d has been elected as leader, so stopping the election process.".format(leaderId))
       return amILeader
    }

    // 尝试将自身信息注册到 electionPath 节点，如果成功则当选leader，失败则进入leader竞选失败流程，作为follower初始化leader相关信息。
    try {
      // 在 electionPath 上尝试选举leader（创建 ephemeral node 并且写入数据 electString）
      // 如果当前 electionPath 对应的值为 brokerId 的话会重试
      createEphemeralPathExpectConflictHandleZKBug(controllerContext.zkClient, electionPath, electString, brokerId,
        (controllerString : String, leaderId : Any) => KafkaController.parseControllerId(controllerString) == leaderId.asInstanceOf[Int],
        controllerContext.zkSessionTimeout)
      info(brokerId + " successfully elected as leader")
      // 修改 leaderId
      leaderId = brokerId
      // 进入回调函数
      onBecomingLeader()
    } catch {
      case e: ZkNodeExistsException =>
        // If someone else has written the path, then
        leaderId = getControllerID 

        if (leaderId != -1)
          debug("Broker %d was elected as leader instead of broker %d".format(leaderId, brokerId))
        else
          warn("A leader has been elected but just resigned, this will result in another round of election")

      case e2: Throwable =>
        error("Error while electing or becoming leader on broker %d".format(brokerId), e2)
        // 如果在选举中碰到其他异常，需要 resign
        resign()
    }
    amILeader
  }

  def close: Unit = {
    leaderId = -1
  }

  def amILeader : Boolean = leaderId == brokerId

  def resign(): Boolean = {
    leaderId = -1
    deletePath(controllerContext.zkClient, electionPath)
  }

  /**
   * We do not have session expiration listen in the ZkElection, but assuming the caller who uses this module will
   * have its own session expiration listener and handler
   */
  class LeaderChangeListener extends IZkDataListener with Logging {
    /**
     * Called when the leader information stored in zookeeper has changed. Record the new leader in memory
     * @throws Exception On any error.
     */
    @throws(classOf[Exception])
    def handleDataChange(dataPath: String, data: Object): Unit = {
      inLock(controllerContext.controllerLock) {
        leaderId = KafkaController.parseControllerId(data.toString)
        info("New leader is %d".format(leaderId))
      }
    }

    /**
     * Called when the leader information stored in zookeeper has been delete. Try to elect as the leader
     * @throws Exception
     *             On any error.
     */
    @throws(classOf[Exception])
    def handleDataDeleted(dataPath: String): Unit = {
      inLock(controllerContext.controllerLock) {
        debug("%s leader change listener fired for path %s to handle data deleted: trying to elect as a leader"
          .format(brokerId, dataPath))
        if(amILeader)
          onResigningAsLeader()
        elect
      }
    }
  }
}
