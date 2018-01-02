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

import kafka.utils.CoreUtils._
import kafka.utils.{Json, Logging, ZKCheckedEphemeral}
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import org.I0Itec.zkclient.IZkDataListener
import kafka.controller.ControllerContext
import kafka.controller.KafkaController
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.utils.Time

/**
 * This class handles zookeeper based leader election based on an ephemeral path. The election module does not handle
 * session expiration, instead it assumes the caller will handle it by probably try to re-elect again. If the existing
 * leader is dead, this class will handle automatic re-election and if it succeeds, it invokes the leader state change
 * callback
 */
// ZookeeperLeaderElector 类基于 zookeeper 临时节点的抢占式选主策略, 多个备选者都去 zookeeper 上注册同一个临时节点
// 但 zookeeper 保证同时只有一个备选者注册成功, 此备选者即成为leader.
// 然后大家都 watch 这个临时节点, 一旦此临时节点消失, watcher被触发, 各备选者又一次开始抢占选主
// 简单来说就是哪台 broker 先启动它就会成为 leader, 这时候如果其他的 broker 启动完成后会读取 /controller 节点的数据更新其各自的内存数据
class ZookeeperLeaderElector(controllerContext: ControllerContext,
                             electionPath: String,
                             onBecomingLeader: () => Unit,     // 被选举为 leader 的回调函数
                             onResigningAsLeader: () => Unit,  // leader 变更的回调函数
                             brokerId: Int,
                             time: Time)
  extends LeaderElector with Logging {
  var leaderId = -1
  // create the election path in ZK, if one does not exist
  val index = electionPath.lastIndexOf("/")
  if (index > 0)
    controllerContext.zkUtils.makeSurePersistentPathExists(electionPath.substring(0, index))
  val leaderChangeListener = new LeaderChangeListener

  // 启动 elector, 先 watch 这个 zookeeper 节点并注册回调函数, 然后调用 elect() 进行选举
  def startup {
    inLock(controllerContext.controllerLock) {
      // watch 这个 zookeeper 结点, 注册回调函数 LeaderChangeListener
      controllerContext.zkUtils.zkClient.subscribeDataChanges(electionPath, leaderChangeListener)
      elect
    }
  }

  def getControllerID(): Int = {
    controllerContext.zkUtils.readDataMaybeNull(electionPath)._1 match {
       case Some(controller) => KafkaController.parseControllerId(controller)
       case None => -1
    }
  }

  // 核心函数, 选举 leader
  def elect: Boolean = {
    val timestamp = time.milliseconds.toString
    val electString = Json.encode(Map("version" -> 1, "brokerid" -> brokerId, "timestamp" -> timestamp))

    // 读取选举路径的 leader 数据
    leaderId = getControllerID
    /* 
     * We can get here during the initial startup and the handleDeleted ZK callback. Because of the potential race condition, 
     * it's possible that the controller has already been elected when we get here. This check will prevent the following 
     * createEphemeralPath method from getting into an infinite loop if this broker is already the controller.
     */
    // 如果读取 leader 数据成功, 那么集群已经存在了一个 leader, 则终止选举过程, 返回 amILeader()
    if(leaderId != -1) {
       debug("Broker %d has been elected as leader, so stopping the election process.".format(leaderId))
       return amILeader
    }

    // 如果读取 leader 数据失败, 则当前没有 leader, 进行选举: 创建 zookeeper 临时节点并存放数据
    try {
      val zkCheckedEphemeral = new ZKCheckedEphemeral(electionPath,
                                                      electString,
                                                      controllerContext.zkUtils.zkConnection.getZookeeper,
                                                      JaasUtils.isZkSecurityEnabled())
      zkCheckedEphemeral.create()
      info(brokerId + " successfully elected as leader")
      leaderId = brokerId
      onBecomingLeader()
    } catch {
      case _: ZkNodeExistsException =>
        // If someone else has written the path, then
        leaderId = getControllerID 

        if (leaderId != -1)
          debug("Broker %d was elected as leader instead of broker %d".format(leaderId, brokerId))
        else
          warn("A leader has been elected but just resigned, this will result in another round of election")

      // 当在发送数据到zookeeper过程中出现Throwable异常时，会调用resign()方法
      case e2: Throwable =>
        error("Error while electing or becoming leader on broker %d".format(brokerId), e2)
        resign()
    }
    amILeader
  }

  def close = {
    leaderId = -1
  }

  def amILeader : Boolean = leaderId == brokerId

  // 重新选举, 实际是只是删除 zookeeper 的 leader 信息
  // 会触发其他所有的 broker 上 LeaderChangeListener 的 handleDataDeleted(), 从而发生重新选举
  def resign() = {
    leaderId = -1
    controllerContext.zkUtils.deletePath(electionPath)
  }

  /**
   * We do not have session expiration listen in the ZkElection, but assuming the caller who uses this module will
   * have its own session expiration listener and handler
   */
  // IZkDataListener 是 zookeeper 提供的接口.
  // 当数据发生变化时会调用该 listener 的 handleDataChange() 和 handleDataDeleted() 方法
  class LeaderChangeListener extends IZkDataListener with Logging {
    /**
     * Called when the leader information stored in zookeeper has changed. Record the new leader in memory
     * @throws Exception On any error.
     */
    @throws[Exception]
    def handleDataChange(dataPath: String, data: Object) {
      val shouldResign = inLock(controllerContext.controllerLock) {
        val amILeaderBeforeDataChange = amILeader
        leaderId = KafkaController.parseControllerId(data.toString)
        info("New leader is %d".format(leaderId))
        // The old leader needs to resign leadership if it is no longer the leader
        amILeaderBeforeDataChange && !amILeader
      }

      if (shouldResign)
        onResigningAsLeader()
    }

    /**
     * Called when the leader information stored in zookeeper has been delete. Try to elect as the leader
     * @throws Exception
     *             On any error.
     */
    // 当 leader 信息被删除的时候, listener 的 handleDataDeleted() 方法被自动调用
    // 这时会重新去选举 leader
    @throws[Exception]
    def handleDataDeleted(dataPath: String) { 
      val shouldResign = inLock(controllerContext.controllerLock) {
        debug("%s leader change listener fired for path %s to handle data deleted: trying to elect as a leader"
          .format(brokerId, dataPath))
        amILeader
      }

      if (shouldResign)
        onResigningAsLeader()

      inLock(controllerContext.controllerLock) {
        elect
      }
    }
  }
}
