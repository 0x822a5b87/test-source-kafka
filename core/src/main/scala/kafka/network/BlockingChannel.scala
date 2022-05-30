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

package kafka.network

import java.net.InetSocketAddress
import java.nio.channels._
import kafka.utils.{nonthreadsafe, Logging}
import kafka.api.RequestOrResponse


object BlockingChannel{
  val UseDefaultBufferSize: Int = -1
}

/**
 *  A simple blocking channel with timeouts correctly enabled.
 *
 */
@nonthreadsafe
class BlockingChannel( val host: String, 
                       val port: Int, 
                       val readBufferSize: Int, 
                       val writeBufferSize: Int, 
                       val readTimeoutMs: Int ) extends Logging {
  // 是否连接
  private var connected = false
  // socket 客户端
  private var channel: SocketChannel = _
  // 读通道
  private var readChannel: ReadableByteChannel = _
  // 写通道
  private var writeChannel: GatheringByteChannel = _
  private val lock = new Object()
  private val connectTimeoutMs = readTimeoutMs

  /**
   * 连接到一台指定的broker的方法，会在以下几种情况下调用：
   * 1. 客户端消费；
   * 2. SimpleConsumer 消费；
   * 3. SyncProducer 生产；
   * 4. RequestSendThread 初始化；
   * 5. RequestSendThread 工作时传输数据出现异常；
   * 6. KafkaApis 触发 controlledShutdown。
   */
  def connect(): Unit = lock synchronized  {
    if(!connected) {
      try {
        // 打开并配置socket连接
        channel = SocketChannel.open()
        if(readBufferSize > 0)
          channel.socket.setReceiveBufferSize(readBufferSize)
        if(writeBufferSize > 0)
          channel.socket.setSendBufferSize(writeBufferSize)
        channel.configureBlocking(true)
        channel.socket.setSoTimeout(readTimeoutMs)
        channel.socket.setKeepAlive(true)
        channel.socket.setTcpNoDelay(true)
        // 连接socket
        channel.socket.connect(new InetSocketAddress(host, port), connectTimeoutMs)

        writeChannel = channel
        readChannel = Channels.newChannel(channel.socket().getInputStream)
        connected = true
        // settings may not match what we requested above
        val msg = "Created socket with SO_TIMEOUT = %d (requested %d), SO_RCVBUF = %d (requested %d), SO_SNDBUF = %d (requested %d), connectTimeoutMs = %d."
        debug(msg.format(channel.socket.getSoTimeout,
                         readTimeoutMs,
                         channel.socket.getReceiveBufferSize, 
                         readBufferSize,
                         channel.socket.getSendBufferSize,
                         writeBufferSize,
                         connectTimeoutMs))

      } catch {
        case e: Throwable => disconnect()
      }
    }
  }
  
  def disconnect(): Unit = lock synchronized {
    if(channel != null) {
      swallow(channel.close())
      swallow(channel.socket.close())
      channel = null
      writeChannel = null
    }
    // closing the main socket channel *should* close the read channel
    // but let's do it to be sure.
    if(readChannel != null) {
      swallow(readChannel.close())
      readChannel = null
    }
    connected = false
  }

  def isConnected: Boolean = connected

  def send(request: RequestOrResponse):Int = {
    if(!connected)
      throw new ClosedChannelException()

    val send = new BoundedByteBufferSend(request)
    send.writeCompletely(writeChannel)
  }
  
  def receive(): Receive = {
    if(!connected)
      throw new ClosedChannelException()

    val response = new BoundedByteBufferReceive()
    response.readCompletely(readChannel)

    response
  }

}
