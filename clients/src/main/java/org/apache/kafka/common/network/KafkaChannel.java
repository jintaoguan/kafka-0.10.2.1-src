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

package org.apache.kafka.common.network;


import java.io.IOException;

import java.net.InetAddress;
import java.net.Socket;
import java.nio.channels.SelectionKey;

import java.security.Principal;

import org.apache.kafka.common.utils.Utils;

public class KafkaChannel {
    private final String id;
    private final TransportLayer transportLayer;
    private final Authenticator authenticator;
    private final int maxReceiveSize;
    private NetworkReceive receive;
    private Send send;
    // Track connection and mute state of channels to enable outstanding requests on channels to be
    // processed after the channel is disconnected.
    private boolean disconnected;
    private boolean muted;

    public KafkaChannel(String id, TransportLayer transportLayer, Authenticator authenticator, int maxReceiveSize) throws IOException {
        this.id = id;
        this.transportLayer = transportLayer;
        this.authenticator = authenticator;
        this.maxReceiveSize = maxReceiveSize;
        this.disconnected = false;
        this.muted = false;
    }

    public void close() throws IOException {
        this.disconnected = true;
        Utils.closeAll(transportLayer, authenticator);
    }

    /**
     * Returns the principal returned by `authenticator.principal()`.
     */
    public Principal principal() throws IOException {
        return authenticator.principal();
    }

    /**
     * Does handshake of transportLayer and authentication using configured authenticator
     */
    public void prepare() throws IOException {
        if (!transportLayer.ready())
            transportLayer.handshake();
        if (transportLayer.ready() && !authenticator.complete())
            authenticator.authenticate();
    }

    public void disconnect() {
        disconnected = true;
        transportLayer.disconnect();
    }


    public boolean finishConnect() throws IOException {
        return transportLayer.finishConnect();
    }

    public boolean isConnected() {
        return transportLayer.isConnected();
    }

    public String id() {
        return id;
    }

    public void mute() {
        if (!disconnected)
            transportLayer.removeInterestOps(SelectionKey.OP_READ);
        muted = true;
    }

    public void unmute() {
        if (!disconnected)
            transportLayer.addInterestOps(SelectionKey.OP_READ);
        muted = false;
    }

    /**
     * Returns true if this channel has been explicitly muted using {@link KafkaChannel#mute()}
     */
    public boolean isMute() {
        return muted;
    }

    public boolean ready() {
        return transportLayer.ready() && authenticator.complete();
    }

    public boolean hasSend() {
        return send != null;
    }

    /**
     * Returns the address to which this channel's socket is connected or `null` if the socket has never been connected.
     *
     * If the socket was connected prior to being closed, then this method will continue to return the
     * connected address after the socket is closed.
     */
    public InetAddress socketAddress() {
        return transportLayer.socketChannel().socket().getInetAddress();
    }

    // 从 KafkaChannel 中取得 socket 的地址信息并返回
    public String socketDescription() {
        Socket socket = transportLayer.socketChannel().socket();
        if (socket.getInetAddress() == null)
            return socket.getLocalAddress().toString();
        return socket.getInetAddress().toString();
    }

    // 设置这个 KafkaChannel 的 send 字段并关注 OP_WRITE 事件
    // 这样当 SocketChannel 的 OP_WRITE 事件发生时就发送 Send
    public void setSend(Send send) {
        // 如果之前还有 send 没有发送完则抛出异常
        if (this.send != null)
            throw new IllegalStateException("Attempt to begin a send operation with prior send operation still in progress.");
        this.send = send;
        this.transportLayer.addInterestOps(SelectionKey.OP_WRITE);
    }

    // 读取 NetworkReceive 的核心方法, 从 socketChannel 读取一个 NetworkReceive 对象并返回
    public NetworkReceive read() throws IOException {
        NetworkReceive result = null;

        // 先看上一次 read() 时产生的 receive 是否为 null, 如果不是 null, 说明上一次并未读取一个完整 NetworkReceive 对象
        if (receive == null) {
            receive = new NetworkReceive(maxReceiveSize, id);
        }

        // 核心函数, 从 TransportLayer(Kafka 对 SocketChannel的封装) 中读取一个 NetworkReceive 对象
        receive(receive);
        // 如果读取完成则将读取到的 NetworkReceive 对象作为 result 返回
        // 如果读取未完成则直接返回 null, 下一次触发 OP_READ 事件时继续填充这个 NetworkReceive 对象并返回
        if (receive.complete()) {
            receive.payload().rewind();
            result = receive;
            receive = null;
        }
        return result;
    }

    // 发送 Send 的核心方法, 如果没有完全发送完则返回 null, 发送完成则返回 Send 对象
    public Send write() throws IOException {
        Send result = null;
        if (send != null && send(send)) {
            result = send;
            send = null;
        }
        return result;
    }

    // 从 transportLayer(socketChannel) 读取数据到 NetworkReceive 对象中去
    private long receive(NetworkReceive receive) throws IOException {
        return receive.readFrom(transportLayer);
    }

    // 发送 send 的核心方法
    private boolean send(Send send) throws IOException {
        // 如果 send 在一次 write() 调用时没有发送完, SelectionKey 的 OP_WRITE 事件没有取消
        // 就会继续监听此 Channel 的 OP_WRITE 事件直到整个 send 发送完成
        send.writeTo(transportLayer);
        // 如果 send 发送完成则不再关注这个 SocketChannel 的 OP_WRITE 事件
        // 判断发送是否完成是通过查看 ByteBuffer 中是否还有剩余字节来判断的
        if (send.completed())
            transportLayer.removeInterestOps(SelectionKey.OP_WRITE);

        return send.completed();
    }

}
