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

import java.io.IOException
import java.net._
import java.nio.channels._
import java.nio.channels.{Selector => NSelector}
import java.util
import java.util.concurrent._
import java.util.concurrent.atomic._

import com.yammer.metrics.core.Gauge
import kafka.cluster.{BrokerEndPoint, EndPoint}
import kafka.common.KafkaException
import kafka.metrics.KafkaMetricsGroup
import kafka.security.CredentialProvider
import kafka.server.KafkaConfig
import kafka.utils._
import org.apache.kafka.common.errors.InvalidRequestException
import org.apache.kafka.common.metrics._
import org.apache.kafka.common.network.{ChannelBuilders, KafkaChannel, ListenerName, LoginType, Mode, Selectable, Selector => KSelector}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.common.protocol.types.SchemaException
import org.apache.kafka.common.utils.{Time, Utils}

import scala.collection._
import JavaConverters._
import scala.util.control.{ControlThrowable, NonFatal}

/**
 * An NIO socket server. The threading model is
 *   1 Acceptor thread that handles new connections
 *   Acceptor has N Processor threads that each have their own selector and read requests from sockets
 *   M Handler threads that handle requests and produce responses back to the processor threads for writing.
 */
// SocketServer 是对一个 broker 的相关 ServerSocket 的抽象, 用于管理这个 broker 的底层 socket 连接
// Kafka SocketServer 是基于Java NIO来开发的, 采用了Reactor的模式, 其中包含了1个 Acceptor 负责接受客户端请求,
// N个 Processor 负责读写数据, M个 Handler 来处理业务逻辑.
// 每个 Acceptor 对象拥有一个 NSelector 对绑定的 ServerSocketChannel 监听 OP_ACCEPT 消息
// 每个 Processor 都有一个 KSelector, 用来监听多个客户端 SocketChannel, 因此可以非阻塞地处理多个客户端的读写请求.
// Acceptor 和 Processor 有 newConnections (每个 Processor 有一个队列) 队列来缓存客户端连接,
// Processor 与 Handler 之间有 RequestChannel 的 requestQueue (共用一个) 与 responseQueues (每个 Processor 单独有一个队列)
// 两个队列来缓存 request 与 response.
// Kafka 的 SocketServer 是一个典型的 SEDA (Staged Event-Driven Architecture) 架构.
class SocketServer(val config: KafkaConfig, val metrics: Metrics, val time: Time, val credentialProvider: CredentialProvider) extends Logging with KafkaMetricsGroup {

  private val endpoints = config.listeners.map(l => l.listenerName -> l).toMap
  // Acceptor (每个 endpoint 都有一个 Acceptor 对象) 可以创建的 processor 线程的个数
  private val numProcessorThreads = config.numNetworkThreads
  private val maxQueuedRequests = config.queuedMaxRequests
  // 总共创建的 processor 线程数量 (每个 Acceptor 线程会创建 numProcessorThreads 个线程)
  private val totalProcessorThreads = numProcessorThreads * endpoints.size

  private val maxConnectionsPerIp = config.maxConnectionsPerIp
  private val maxConnectionsPerIpOverrides = config.maxConnectionsPerIpOverrides

  this.logIdent = "[Socket Server on Broker " + config.brokerId + "], "

  // 一个 broker 拥有一个 RequestChannel 对象, 其中为每一个 processor 线程创建一个 requestQueue 与一个 responseQueues
  val requestChannel = new RequestChannel(totalProcessorThreads, maxQueuedRequests)
  // processors 是一个数组, 用于对所有的 processor 的索引
  private val processors = new Array[Processor](totalProcessorThreads)

  // acceptors 是一个 Map[EndPoint, Acceptor]
  private[network] val acceptors = mutable.Map[EndPoint, Acceptor]()
  private var connectionQuotas: ConnectionQuotas = _

  private val allMetricNames = (0 until totalProcessorThreads).map { i =>
    val tags = new util.HashMap[String, String]()
    tags.put("networkProcessor", i.toString)
    metrics.metricName("io-wait-ratio", "socket-server-metrics", tags)
  }

  /**
   * Start the socket server
   */
  // kafka 启动时会通过 KafkaServer 对象调用 startup() 方法
  def startup() {
    this.synchronized {

      connectionQuotas = new ConnectionQuotas(maxConnectionsPerIp, maxConnectionsPerIpOverrides)

      // 发送数据的 buffer 大小
      val sendBufferSize = config.socketSendBufferBytes
      // 接收数据的 buffer 大小
      val recvBufferSize = config.socketReceiveBufferBytes
      // 本机的 brokerId
      val brokerId = config.brokerId

      var processorBeginIndex = 0
      config.listeners.foreach { endpoint =>
        val listenerName = endpoint.listenerName
        val securityProtocol = endpoint.securityProtocol
        val processorEndIndex = processorBeginIndex + numProcessorThreads

        // 对于每个 endpoint, 根据 numProcessorThreads 创建多个 processor
        for (i <- processorBeginIndex until processorEndIndex)
          processors(i) = newProcessor(i, connectionQuotas, listenerName, securityProtocol)

        // 对于这个 endpoint 创建其对应的 Acceptor (关联其对应的 processor 线程)
        val acceptor = new Acceptor(endpoint, sendBufferSize, recvBufferSize, brokerId,
          processors.slice(processorBeginIndex, processorEndIndex), connectionQuotas)
        acceptors.put(endpoint, acceptor)
        // 创建并启动这个 Acceptor 线程
        Utils.newThread(s"kafka-socket-acceptor-$listenerName-$securityProtocol-${endpoint.port}", acceptor, false).start()
        // 通过 Acceptor 中的 latch 阻塞住 startup() 线程, 直到 Acceptor 完全启动
        acceptor.awaitStartup()

        processorBeginIndex = processorEndIndex
      }
    }

    newGauge("NetworkProcessorAvgIdlePercent",
      new Gauge[Double] {
        def value = allMetricNames.map { metricName =>
          Option(metrics.metric(metricName)).fold(0.0)(_.value)
        }.sum / totalProcessorThreads
      }
    )

    info("Started " + acceptors.size + " acceptor threads")
  }

  // register the processor threads for notification of responses
  requestChannel.addResponseListener(id => processors(id).wakeup())

  /**
   * Shutdown the socket server
   */
  def shutdown() = {
    info("Shutting down")
    this.synchronized {
      acceptors.values.foreach(_.shutdown)
      processors.foreach(_.shutdown)
    }
    info("Shutdown completed")
  }

  def boundPort(listenerName: ListenerName): Int = {
    try {
      acceptors(endpoints(listenerName)).serverChannel.socket.getLocalPort
    } catch {
      case e: Exception => throw new KafkaException("Tried to check server's port before server was started or checked for port of non-existing protocol", e)
    }
  }

  /* `protected` for test usage */
  protected[network] def newProcessor(id: Int, connectionQuotas: ConnectionQuotas, listenerName: ListenerName,
                                      securityProtocol: SecurityProtocol): Processor = {
    new Processor(id,
      time,
      config.socketRequestMaxBytes,
      requestChannel,
      connectionQuotas,
      config.connectionsMaxIdleMs,
      listenerName,
      securityProtocol,
      config.values,
      metrics,
      credentialProvider
    )
  }

  /* For test usage */
  private[network] def connectionCount(address: InetAddress): Int =
    Option(connectionQuotas).fold(0)(_.get(address))

  /* For test usage */
  private[network] def processor(index: Int): Processor = processors(index)

}

/**
 * A base class with some helper variables and methods
 */
private[kafka] abstract class AbstractServerThread(connectionQuotas: ConnectionQuotas) extends Runnable with Logging {

  private val startupLatch = new CountDownLatch(1)

  // `shutdown()` is invoked before `startupComplete` and `shutdownComplete` if an exception is thrown in the constructor
  // (e.g. if the address is already in use). We want `shutdown` to proceed in such cases, so we first assign an open
  // latch and then replace it in `startupComplete()`.
  @volatile private var shutdownLatch = new CountDownLatch(0)

  private val alive = new AtomicBoolean(true)

  def wakeup(): Unit

  /**
   * Initiates a graceful shutdown by signaling to stop and waiting for the shutdown to complete
   */
  def shutdown(): Unit = {
    alive.set(false)
    wakeup()
    shutdownLatch.await()
  }

  /**
   * Wait for the thread to completely start up
   */
  def awaitStartup(): Unit = startupLatch.await

  /**
   * Record that the thread startup is complete
   */
  protected def startupComplete(): Unit = {
    // Replace the open latch with a closed one
    shutdownLatch = new CountDownLatch(1)
    startupLatch.countDown()
  }

  /**
   * Record that the thread shutdown is complete
   */
  protected def shutdownComplete(): Unit = shutdownLatch.countDown()

  /**
   * Is the server still running?
   */
  protected def isRunning: Boolean = alive.get

  /**
   * Close the connection identified by `connectionId` and decrement the connection count.
   */
  def close(selector: KSelector, connectionId: String): Unit = {
    val channel = selector.channel(connectionId)
    if (channel != null) {
      debug(s"Closing selector connection $connectionId")
      val address = channel.socketAddress
      if (address != null)
        connectionQuotas.dec(address)
      selector.close(connectionId)
    }
  }

  /**
   * Close `channel` and decrement the connection count.
   */
  def close(channel: SocketChannel): Unit = {
    if (channel != null) {
      debug("Closing connection from " + channel.socket.getRemoteSocketAddress())
      connectionQuotas.dec(channel.socket.getInetAddress)
      swallowError(channel.socket().close())
      swallowError(channel.close())
    }
  }
}

/**
 * Thread that accepts and configures new connections. There is one of these per endpoint.
 */
// Acceptor 线程用于 accept 新的连接, 每个 endpoint 都会有一个自己的 Acceptor
// Acceptor 作两件事: 1)创建一堆 processor 线程;  2) 接受新连接, 将新的socket指派给某个 processor 线程
// 每个 Acceptor 对象拥有一个 NSelector 对绑定的 ServerSocketChannel 监听 OP_ACCEPT 消息
private[kafka] class Acceptor(val endPoint: EndPoint,
                              val sendBufferSize: Int,
                              val recvBufferSize: Int,
                              brokerId: Int,
                              processors: Array[Processor],
                              connectionQuotas: ConnectionQuotas) extends AbstractServerThread(connectionQuotas) with KafkaMetricsGroup {

  // 每个 Acceptor 拥有一个 NIO Selector 对象, 对应一个 ServerSocketChannel 对象
  private val nioSelector = NSelector.open()
  val serverChannel = openServerSocket(endPoint.host, endPoint.port)

  // 启动这个 Acceptor 管理的所有 processor 线程
  this.synchronized {
    processors.foreach { processor =>
      Utils.newThread(s"kafka-network-thread-$brokerId-${endPoint.listenerName}-${endPoint.securityProtocol}-${processor.id}",
        processor, false).start()
    }
  }

  /**
   * Accept loop that checks for new connection attempts
   */
  // Acceptor 接收 request 的核心方法. 利用 NIO 的 selector 来接收网络连接.
  def run() {
    // 在 selector 注册 SelectionKey.OP_ACCEPT 事件, SelectionKey 是表示一个 Channel 和 Selector 的注册关系。
    // 在 Acceptor 中的 selector, 只有监听客户端连接请求的 ServerSocketChannel 的 OP_ACCEPT 事件注册在上面。
    // 当 selector 的 select 方法返回时, 则表示注册在它上面的 Channel 发生了对应的事件。
    // 在 Acceptor 中, 这个事件就是 OP_ACCEPT, 表示这个 ServerSocketChannel 的 OP_ACCEPT 事件发生了.
    // 四种事件 1) OP_ACCEPT, 2) OP_CONNECT, 3) OP_READ, 4) OP_WRITE
    serverChannel.register(nioSelector, SelectionKey.OP_ACCEPT)
    startupComplete()
    try {
      var currentProcessor = 0
      while (isRunning) {
        try {
          // 开始等待客户端的连接请求
          // 在调用 select() 并返回了有 channel 就绪之后，可以通过选中的 key 集合来获取 channel.
          // 当 selector 的 select 方法返回时, 则表示注册在它上面的 Channel 发生了对应的事件.
          val ready = nioSelector.select(500)
          if (ready > 0) {
            // 可以通过选中的 key 集合来获取 channel
            val keys = nioSelector.selectedKeys()
            val iter = keys.iterator()
            while (iter.hasNext && isRunning) {
              try {
                val key = iter.next
                iter.remove()
                // a connection was accepted by a ServerSocketChannel
                if (key.isAcceptable)
                  // 接受一个新的网络连接, 通过 currentProcessor 索引到一个 processor 并由它处理
                  accept(key, processors(currentProcessor))
                else
                  throw new IllegalStateException("Unrecognized key state for acceptor thread.")

                // round robin to the next processor thread
                // 通过 currentProcessor 索引切换到下一个 processor
                currentProcessor = (currentProcessor + 1) % processors.length
              } catch {
                case e: Throwable => error("Error while accepting connection", e)
              }
            }
          }
        }
        catch {
          // We catch all the throwables to prevent the acceptor thread from exiting on exceptions due
          // to a select operation on a specific channel or a bad request. We don't want
          // the broker to stop responding to requests from other clients in these scenarios.
          case e: ControlThrowable => throw e
          case e: Throwable => error("Error occurred", e)
        }
      }
    } finally {
      debug("Closing server socket and selector.")
      swallowError(serverChannel.close())
      swallowError(nioSelector.close())
      shutdownComplete()
    }
  }

  /*
   * Create a server socket to listen for connections on.
   */
  // 创建 ServerSocket 并监听连接
  private def openServerSocket(host: String, port: Int): ServerSocketChannel = {
    val socketAddress =
      if(host == null || host.trim.isEmpty)
        new InetSocketAddress(port)
      else
        new InetSocketAddress(host, port)
    // 创建一个 ServerSocketChannel 对象
    val serverChannel = ServerSocketChannel.open()
    // 将这个 ServerSocketChannel 配置成非阻塞模式
    serverChannel.configureBlocking(false)
    // 配置接受 buffer 大小
    if (recvBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE)
      serverChannel.socket().setReceiveBufferSize(recvBufferSize)

    try {
      // 将这个 ServerSocketChannel 绑定到指定的 socket address
      serverChannel.socket.bind(socketAddress)
      info("Awaiting socket connections on %s:%d.".format(socketAddress.getHostString, serverChannel.socket.getLocalPort))
    } catch {
      case e: SocketException =>
        throw new KafkaException("Socket server failed to bind to %s:%d: %s.".format(socketAddress.getHostString, port, e.getMessage), e)
    }
    serverChannel
  }

  /*
   * Accept a new connection
   */
  // 接受一个网络连接, 并交给 processor 处理
  // Acceptor 的 accept 方法的处理逻辑为:
  // 1) 首先通过 SelectionKey 来拿到对应的 ServerSocketChannel
  // 2）调用 ServerSocketChannel 的 accept 方法来建立和客户端的连接
  // 3) 然后拿到对应的 SocketChannel 并交给了 processor 处理
  // 4) 到这里 Acceptor 的任务就完成了, 开始去处理下一个客户端的连接请求.
  def accept(key: SelectionKey, processor: Processor) {
    // 获取 ServerSocketChannel 对象
    val serverSocketChannel = key.channel().asInstanceOf[ServerSocketChannel]
    // 从 ServerSocketChannel 对象获得客户端的 SocketChannel 对象
    val socketChannel = serverSocketChannel.accept()
    try {
      // 配置客户端 SocketChannel 的连接
      connectionQuotas.inc(socketChannel.socket().getInetAddress)
      // 设置为非阻塞模式
      socketChannel.configureBlocking(false)
      socketChannel.socket().setTcpNoDelay(true)
      // 设置为长连接
      socketChannel.socket().setKeepAlive(true)
      if (sendBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE)
        socketChannel.socket().setSendBufferSize(sendBufferSize)

      debug("Accepted connection from %s on %s and assigned it to processor %d, sendBufferSize [actual|requested]: [%d|%d] recvBufferSize [actual|requested]: [%d|%d]"
            .format(socketChannel.socket.getRemoteSocketAddress, socketChannel.socket.getLocalSocketAddress, processor.id,
                  socketChannel.socket.getSendBufferSize, sendBufferSize,
                  socketChannel.socket.getReceiveBufferSize, recvBufferSize))

      // 将客户端的 SocketChannel 交给 processor 处理
      processor.accept(socketChannel)
    } catch {
      case e: TooManyConnectionsException =>
        info("Rejected connection from %s, address already has the configured maximum of %d connections.".format(e.ip, e.count))
        close(socketChannel)
    }
  }

  /**
   * Wakeup the thread for selection.
   */
  @Override
  def wakeup = nioSelector.wakeup()

}

/**
 * Thread that processes all requests from a single connection. There are N of these running in parallel
 * each of which has its own selector
 */
// 从单个连接进来的 request 都由 processor 线程处理.
// 它的主要职责是负责从客户端读取数据和将响应返回给客户端, 它本身不处理具体的业务逻辑, 也就是说它并不认识它从客户端读取回来的数据.
// 每个 Processor 都有一个 KSelector, 用来监听多个客户端 SocketChannel, 因此可以非阻塞地处理多个客户端的读写请求.
private[kafka] class Processor(val id: Int,
                               time: Time,
                               maxRequestSize: Int,
                               requestChannel: RequestChannel,
                               connectionQuotas: ConnectionQuotas,
                               connectionsMaxIdleMs: Long,
                               listenerName: ListenerName,
                               securityProtocol: SecurityProtocol,
                               channelConfigs: java.util.Map[String, _],
                               metrics: Metrics,
                               credentialProvider: CredentialProvider) extends AbstractServerThread(connectionQuotas) with KafkaMetricsGroup {

  private object ConnectionId {
    def fromString(s: String): Option[ConnectionId] = s.split("-") match {
      case Array(local, remote) => BrokerEndPoint.parseHostPort(local).flatMap { case (localHost, localPort) =>
        BrokerEndPoint.parseHostPort(remote).map { case (remoteHost, remotePort) =>
          ConnectionId(localHost, localPort, remoteHost, remotePort)
        }
      }
      case _ => None
    }
  }

  private case class ConnectionId(localHost: String, localPort: Int, remoteHost: String, remotePort: Int) {
    override def toString: String = s"$localHost:$localPort-$remoteHost:$remotePort"
  }

  private val newConnections = new ConcurrentLinkedQueue[SocketChannel]()
  private val inflightResponses = mutable.Map[String, RequestChannel.Response]()
  private val metricTags = Map("networkProcessor" -> id.toString).asJava

  newGauge("IdlePercent",
    new Gauge[Double] {
      def value = {
        Option(metrics.metric(metrics.metricName("io-wait-ratio", "socket-server-metrics", metricTags))).fold(0.0)(_.value)
      }
    },
    metricTags.asScala
  )

  private val selector = new KSelector(
    maxRequestSize,
    connectionsMaxIdleMs,
    metrics,
    time,
    "socket-server",
    metricTags,
    false,
    ChannelBuilders.serverChannelBuilder(securityProtocol, channelConfigs, credentialProvider.credentialCache))

  // processor 处理 request 的核心方法。 --> 非常重要 <--
  override def run() {
    startupComplete()
    while (isRunning) {
      try {
        // setup any new connections that have been queued up
        // 从并发队列里取出客户端 SocketChannel, 添加到自身的 KSelector中, 监听该 channel 的 OP_READ 事件
        configureNewConnections()
        // register any new responses for writing
        // 处理当前所有处理完成的 request 相应的response, 这些 response 都是从 RequestChannel 获得 (requestChannel.receiveResponse)
        // 根据 request 的类型来决定从当前连接的 nio selector 中暂时删除读事件监听/添加写事件/关闭当前连接
        processNewResponses()
        poll()
        processCompletedReceives()
        processCompletedSends()
        processDisconnected()
      } catch {
        // We catch all the throwables here to prevent the processor thread from exiting. We do this because
        // letting a processor exit might cause a bigger impact on the broker. Usually the exceptions thrown would
        // be either associated with a specific socket channel or a bad request. We just ignore the bad socket channel
        // or request. This behavior might need to be reviewed if we see an exception that need the entire broker to stop.
        case e: ControlThrowable => throw e
        case e: Throwable =>
          error("Processor got uncaught exception.", e)
      }
    }

    debug("Closing selector - processor " + id)
    swallowError(closeAll())
    shutdownComplete()
  }

  private def processNewResponses() {
    var curr = requestChannel.receiveResponse(id)
    while (curr != null) {
      try {
        curr.responseAction match {
          case RequestChannel.NoOpAction =>
            // There is no response to send to the client, we need to read more pipelined requests
            // that are sitting in the server's socket buffer
            curr.request.updateRequestMetrics
            trace("Socket server received empty response to send, registering for read: " + curr)
            val channelId = curr.request.connectionId
            if (selector.channel(channelId) != null || selector.closingChannel(channelId) != null)
                selector.unmute(channelId)
          case RequestChannel.SendAction =>
            sendResponse(curr)
          case RequestChannel.CloseConnectionAction =>
            curr.request.updateRequestMetrics
            trace("Closing socket connection actively according to the response code.")
            close(selector, curr.request.connectionId)
        }
      } finally {
        curr = requestChannel.receiveResponse(id)
      }
    }
  }

  /* `protected` for test usage */
  protected[network] def sendResponse(response: RequestChannel.Response) {
    trace(s"Socket server received response to send, registering for write and sending data: $response")
    val channel = selector.channel(response.responseSend.destination)
    // `channel` can be null if the selector closed the connection because it was idle for too long
    if (channel == null) {
      warn(s"Attempting to send response via channel for which there is no open connection, connection id $id")
      response.request.updateRequestMetrics()
    }
    else {
      selector.send(response.responseSend)
      inflightResponses += (response.request.connectionId -> response)
    }
  }

  private def poll() {
    try selector.poll(300)
    catch {
      case e @ (_: IllegalStateException | _: IOException) =>
        error(s"Closing processor $id due to illegal state or IO exception")
        swallow(closeAll())
        shutdownComplete()
        throw e
    }
  }

  private def processCompletedReceives() {
    selector.completedReceives.asScala.foreach { receive =>
      try {
        val openChannel = selector.channel(receive.source)
        val session = {
          // Only methods that are safe to call on a disconnected channel should be invoked on 'channel'.
          val channel = if (openChannel != null) openChannel else selector.closingChannel(receive.source)
          RequestChannel.Session(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, channel.principal.getName), channel.socketAddress)
        }
        // 这一步很重要, 构造 Kafka 内部网络层通用的 RequestChannel.Request 对象
        val req = RequestChannel.Request(processor = id, connectionId = receive.source, session = session,
          buffer = receive.payload, startTimeMs = time.milliseconds, listenerName = listenerName,
          securityProtocol = securityProtocol)
        // Processor 将 request 交给 requestChannel 的 requestQueue, 之后由 KafkaRequestHandler 与 KafkaApis 处理
        requestChannel.sendRequest(req)
        // todo 为什么这里使用 mute()?
        selector.mute(receive.source)
      } catch {
        case e @ (_: InvalidRequestException | _: SchemaException) =>
          // note that even though we got an exception, we can assume that receive.source is valid. Issues with constructing a valid receive object were handled earlier
          error(s"Closing socket for ${receive.source} because of error", e)
          close(selector, receive.source)
      }
    }
  }

  private def processCompletedSends() {
    selector.completedSends.asScala.foreach { send =>
      val resp = inflightResponses.remove(send.destination).getOrElse {
        throw new IllegalStateException(s"Send for ${send.destination} completed, but not in `inflightResponses`")
      }
      resp.request.updateRequestMetrics()
      selector.unmute(send.destination)
    }
  }

  private def processDisconnected() {
    selector.disconnected.asScala.foreach { connectionId =>
      val remoteHost = ConnectionId.fromString(connectionId).getOrElse {
        throw new IllegalStateException(s"connectionId has unexpected format: $connectionId")
      }.remoteHost
      inflightResponses.remove(connectionId).foreach(_.request.updateRequestMetrics())
      // the channel has been closed by the selector but the quotas still need to be updated
      connectionQuotas.dec(InetAddress.getByName(remoteHost))
    }
  }

  /**
   * Queue up a new connection for reading
   */
  // 将新连接的 SocketChannel 保存到并发队列中
  // 由于只是简单的将 SocketChannel 对象保存到队列中, 所以每个 Processor 都会处理多个客户端的请求
  def accept(socketChannel: SocketChannel) {
    newConnections.add(socketChannel)
    // 唤醒 Processor 的 selector
    wakeup()
  }

  /**
   * Register any new connections that have been queued up
   */
  // 如果有队列中有新的 SocketChannel, 则它首先将其 OP_READ 事情注册到该 Processor 的 KSelector上面, 监听读事件
  private def configureNewConnections() {
    while (!newConnections.isEmpty) {
      // 取得新连接的客户端 SocketChannel 对象
      val channel = newConnections.poll()
      try {
        debug(s"Processor $id listening to new connection from ${channel.socket.getRemoteSocketAddress}")
        val localHost = channel.socket().getLocalAddress.getHostAddress
        val localPort = channel.socket().getLocalPort
        val remoteHost = channel.socket().getInetAddress.getHostAddress
        val remotePort = channel.socket().getPort
        // [localHost: String, localPort: Int, remoteHost: String, remotePort: Int] 四元组组成 ConnectionId
        val connectionId = ConnectionId(localHost, localPort, remoteHost, remotePort).toString
        // 注册这个 connection 到 KSelector
        // 将这个客户端的 SocketChannel 的 OP_READ 事情注册到该 Processor 的 KSelector上面, 监听该 channel 的读事件
        selector.register(connectionId, channel)
      } catch {
        // We explicitly catch all non fatal exceptions and close the socket to avoid a socket leak. The other
        // throwables will be caught in processor and logged as uncaught exceptions.
        case NonFatal(e) =>
          val remoteAddress = channel.getRemoteAddress
          // need to close the channel here to avoid a socket leak.
          close(channel)
          error(s"Processor $id closed connection from $remoteAddress", e)
      }
    }
  }

  /**
   * Close the selector and all open connections
   */
  private def closeAll() {
    selector.channels.asScala.foreach { channel =>
      close(selector, channel.id)
    }
    selector.close()
  }

  /* For test usage */
  private[network] def channel(connectionId: String): Option[KafkaChannel] =
    Option(selector.channel(connectionId))

  /**
   * Wakeup the thread for selection.
   */
  @Override
  def wakeup = selector.wakeup()

}

class ConnectionQuotas(val defaultMax: Int, overrideQuotas: Map[String, Int]) {

  private val overrides = overrideQuotas.map { case (host, count) => (InetAddress.getByName(host), count) }
  private val counts = mutable.Map[InetAddress, Int]()

  def inc(address: InetAddress) {
    counts.synchronized {
      val count = counts.getOrElseUpdate(address, 0)
      counts.put(address, count + 1)
      val max = overrides.getOrElse(address, defaultMax)
      if (count >= max)
        throw new TooManyConnectionsException(address, max)
    }
  }

  def dec(address: InetAddress) {
    counts.synchronized {
      val count = counts.getOrElse(address,
        throw new IllegalArgumentException(s"Attempted to decrease connection count for address with no connections, address: $address"))
      if (count == 1)
        counts.remove(address)
      else
        counts.put(address, count - 1)
    }
  }

  def get(address: InetAddress): Int = counts.synchronized {
    counts.getOrElse(address, 0)
  }

}

class TooManyConnectionsException(val ip: InetAddress, val count: Int) extends KafkaException("Too many connections from %s (maximum = %d)".format(ip, count))
