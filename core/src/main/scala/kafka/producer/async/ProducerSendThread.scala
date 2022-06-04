/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.producer.async

import kafka.utils.{SystemTime, Logging}
import java.util.concurrent.{TimeUnit, CountDownLatch, BlockingQueue}
import collection.mutable.ArrayBuffer
import kafka.producer.KeyedMessage
import kafka.metrics.KafkaMetricsGroup
import com.yammer.metrics.core.Gauge

/**
 * 当producer.type为async时，Producer客户端会启动ProducerSendThread线程，该线程从queue中收集信息并在合适的时机发送数据
 *
 * @param threadName 线程名，"ProducerSendThread-" + config.clientId
 * @param queue      Producer内部的阻塞队列
 * @param handler    处理队列中的batch数据的handler
 * @param queueTime  [[AsyncProducerConfig# queueBufferingMaxMs]]，队列缓存数据的最大时间，默认5秒
 * @param batchSize  [[AsyncProducerConfig# batchNumMessages]]，每次发送的batch数量，默认200
 * @param clientId   clientId
 * @tparam K key
 * @tparam V value
 */
class ProducerSendThread[K, V](val threadName: String,
                               val queue: BlockingQueue[KeyedMessage[K, V]],
                               val handler: EventHandler[K, V],
                               val queueTime: Long,
                               val batchSize: Int,
                               val clientId: String) extends Thread(threadName) with Logging with KafkaMetricsGroup {

  /**
   * shutdownCommand在shutdown()方法被调用时被放到queue中，随后进入[[shutdownLatch.await()]]并等待[[processEvents()]]清空缓存
   * [[processEvents]]方法消费到shutdownCommand会导致[[Stream.continually]]退出，并进入[[tryToHandle()]]方法清空缓存
   * 随后[[processEvents()]]方法退出，进入[[shutdownLatch.countDown()]]
   */
  private val shutdownLatch = new CountDownLatch(1)
  private val shutdownCommand = new KeyedMessage[K, V]("shutdown", null.asInstanceOf[K], null.asInstanceOf[V])

  // ProducerQueueSize metrics
  newGauge("ProducerQueueSize",
    new Gauge[Int] {
      def value: Int = queue.size
    },
    Map("clientId" -> clientId))

  override def run(): Unit = {
    try {
      processEvents()
    } catch {
      case e: Throwable => error("Error in sending events: ", e)
    } finally {
      shutdownLatch.countDown()
    }
  }

  def shutdown(): Unit = {
    info("Begin shutting down ProducerSendThread")
    queue.put(shutdownCommand)
    shutdownLatch.await()
    info("Shutdown ProducerSendThread complete")
  }

  private def processEvents(): Unit = {
    var lastSend = SystemTime.milliseconds
    //queue是调用send时外部感知到的阻塞队列，超时时发送的是events内部的数据
    var events = new ArrayBuffer[KeyedMessage[K, V]]
    var full: Boolean = false

    // drain the queue until you get a shutdown command
    Stream.continually(queue.poll(scala.math.max(0, (lastSend + queueTime) - SystemTime.milliseconds), TimeUnit.MILLISECONDS))
      .takeWhile(item => if (item != null) item ne shutdownCommand else true).foreach {
      currentQueueItem =>
        val elapsed = (SystemTime.milliseconds - lastSend)
        // 如果<currentQueueItem> == null，说明现在是queue.poll超时返回了null
        val expired = currentQueueItem == null
        if (currentQueueItem != null) {
          trace("Dequeued item for topic %s, partition key: %s, data: %s"
            .format(currentQueueItem.topic, currentQueueItem.key, currentQueueItem.message))
          events += currentQueueItem
        }

        // check if the batch size is reached
        full = events.size >= batchSize

        if (full || expired) {
          if (expired)
            debug(elapsed + " ms elapsed. Queue time reached. Sending..")
          if (full)
            debug("Batch full. Sending..")
          // 发送缓存的数据
          tryToHandle(events)
          lastSend = SystemTime.milliseconds
          events = new ArrayBuffer[KeyedMessage[K, V]]
        }
    }
    //收到了<shutdownCommand>，发送剩余的数据
    tryToHandle(events)
    if (queue.size > 0)
      throw new IllegalQueueStateException("Invalid queue state! After queue shutdown, %d remaining items in the queue"
        .format(queue.size))
  }

  /**
   * 发送数据
   * @param events 等待发送的数据
   */
  def tryToHandle(events: Seq[KeyedMessage[K, V]]): Unit = {
    val size = events.size
    try {
      debug("Handling " + size + " events")
      if (size > 0)
        handler.handle(events)
    } catch {
      case e: Throwable => error("Error in handling batch of " + size + " events", e)
    }
  }

}
