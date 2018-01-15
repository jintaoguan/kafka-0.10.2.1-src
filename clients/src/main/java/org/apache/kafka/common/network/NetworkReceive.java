/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.network;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.ScatteringByteChannel;

/**
 * A size delimited Receive that consists of a 4 byte network-ordered size N followed by N bytes of content
 */
// NetworkReceive 是对一个网络接收到的请求的描述, 有三部分组成
// size 是一个固定 4 字节的 ByteBuffer, 表示一个网络请求的具体内容的长度, 可以通过 size.getInt() 取得
// buffer 是一个长度不固定的 ByteBuffer, 初始化时由 size.getInt() 作为参数申请空间, 里面存放了请求的具体内容
// source 表示从哪里接收到的请求
public class NetworkReceive implements Receive {

    public final static String UNKNOWN_SOURCE = "";
    public final static int UNLIMITED = -1;

    private final String source;
    private final ByteBuffer size;
    private final int maxSize;
    private ByteBuffer buffer;


    public NetworkReceive(String source, ByteBuffer buffer) {
        this.source = source;
        this.buffer = buffer;
        this.size = null;
        this.maxSize = UNLIMITED;
    }

    public NetworkReceive(String source) {
        this.source = source;
        this.size = ByteBuffer.allocate(4);
        this.buffer = null;
        this.maxSize = UNLIMITED;
    }

    public NetworkReceive(int maxSize, String source) {
        this.source = source;
        this.size = ByteBuffer.allocate(4);
        this.buffer = null;
        this.maxSize = maxSize;
    }

    public NetworkReceive() {
        this(UNKNOWN_SOURCE);
    }

    @Override
    public String source() {
        return source;
    }

    @Override
    public boolean complete() {
        return !size.hasRemaining() && !buffer.hasRemaining();
    }

    // 从 channel 中读取 NetworkReceive 对象
    public long readFrom(ScatteringByteChannel channel) throws IOException {
        return readFromReadableChannel(channel);
    }

    // Need a method to read from ReadableByteChannel because BlockingChannel requires read with timeout
    // See: http://stackoverflow.com/questions/2866557/timeout-for-socketchannel-doesnt-work
    // This can go away after we get rid of BlockingChannel
    // 从 Channel 中读取 NetworkReceive 的过程是
    // 1. 先从 Channel 读取 4 字节放入 size 中, size.getInt() 得到具体内容的长度 L
    // 2. 在申请一个长度为 L 字节的 ByteBuffer 为 buffer
    // 3. 从 Channel 读取 L 字节的内容放入 buffer, NetworkReceive 对象就被填充好了
    @Deprecated
    public long readFromReadableChannel(ReadableByteChannel channel) throws IOException {
        int read = 0;
        // 这里的条件 size.hasRemaining() 用于这次调用是读取一个新的 NetworkReceive 还是继续上一次未完成的读取
        if (size.hasRemaining()) {
            // 从 channel 读取 size 数据, size 这个 ByteBuffer 已经强制为 4 bytes 大小
            int bytesRead = channel.read(size);
            if (bytesRead < 0)
                throw new EOFException();
            read += bytesRead;
            if (!size.hasRemaining()) {
                size.rewind();
                // 从 size ByteBuffer 中读取整数, 即为后面具体内容的长度
                int receiveSize = size.getInt();
                if (receiveSize < 0)
                    throw new InvalidReceiveException("Invalid receive (size = " + receiveSize + ")");
                // 如果从 size 读取出来的长度大于最大值则抛出异常
                if (maxSize != UNLIMITED && receiveSize > maxSize)
                    throw new InvalidReceiveException("Invalid receive (size = " + receiveSize + " larger than " + maxSize + ")");

                // 根据 size 得到的大小申请相应大小的  ByteBuffer 并继续从 channel 中读取具体内容
                this.buffer = ByteBuffer.allocate(receiveSize);
            }
        }
        // 继续从 channel 中读取内容放入 buffer 中
        if (buffer != null) {
            int bytesRead = channel.read(buffer);
            if (bytesRead < 0)
                throw new EOFException();
            read += bytesRead;
        }

        return read;
    }

    // payload() 就是返回存放具体内容的 buffer
    public ByteBuffer payload() {
        return this.buffer;
    }

}
