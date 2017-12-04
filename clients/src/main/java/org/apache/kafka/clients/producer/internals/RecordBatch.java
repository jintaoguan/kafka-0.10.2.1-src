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
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A batch of records that is or will be sent.
 * 
 * This class is not thread safe and external synchronization must be used when modifying it
 */
public final class RecordBatch {

    private static final Logger log = LoggerFactory.getLogger(RecordBatch.class);

    final long createdMs;
    final TopicPartition topicPartition;
    final ProduceRequestResult produceFuture;

    private final List<Thunk> thunks = new ArrayList<>();
    private final MemoryRecordsBuilder recordsBuilder;

    // 尝试发送当前 RecordBatch 的次数
    volatile int attempts;
    // 记录了保存的 record 的个数
    int recordCount;
    // 最大 record 的字节数
    int maxRecordSize;
    long drainedMs;
    long lastAttemptMs;
    long lastAppendTime;
    private String expiryErrorMessage;
    private AtomicBoolean completed;
    private boolean retry;

    public RecordBatch(TopicPartition tp, MemoryRecordsBuilder recordsBuilder, long now) {
        this.createdMs = now;
        this.lastAttemptMs = now;
        this.recordsBuilder = recordsBuilder;
        this.topicPartition = tp;
        this.lastAppendTime = createdMs;
        this.produceFuture = new ProduceRequestResult(topicPartition);
        this.completed = new AtomicBoolean();
    }

    /**
     * Append the record to the current record set and return the relative offset within that record set
     * 
     * @return The RecordSend corresponding to this record or null if there isn't sufficient room.
     */
    // 将序列化后的 record 放入 RecordBatch 中
    // 如果该 RecordBatch 剩余空间不足, 返回 null 表示失败, 否则返回一个 FutureRecordMetadata 对象.
    // FutureRecordMetadata 中包含了 ProduceRequestResult 对象, 记录了 TopicPartition 以及 offset.
    public FutureRecordMetadata tryAppend(long timestamp, byte[] key, byte[] value, Callback callback, long now) {
        // 估计剩余空间不足, 这里不是一个准确值.
        if (!recordsBuilder.hasRoomFor(key, value)) {
            return null;
        } else {
            // 向 MemoryRecords 中添加数据
            long checksum = this.recordsBuilder.append(timestamp, key, value);
            // 更新最大 record 的字节数
            this.maxRecordSize = Math.max(this.maxRecordSize, Record.recordSize(key, value));
            this.lastAppendTime = now;
            // 创建 FutureRecordMetadata 对象
            FutureRecordMetadata future = new FutureRecordMetadata(this.produceFuture, this.recordCount,
                                                                   timestamp, checksum,
                                                                   key == null ? -1 : key.length,
                                                                   value == null ? -1 : value.length);
            // 将用户自定义的 Callback 与 FutureRecordMetadata 一起封装到 Thunk.
            // 每条消息都封装一个 Thunk 对象, 当消息数据发送给服务器成功后, 会进入 RecordBatch.done() 方法.
            // 在 RecordBatch.done() 方法中, 每个 Thunk 会执行一次, 就是对每个消息执行一次自定义 Callback.
            if (callback != null)
                thunks.add(new Thunk(callback, future));
            this.recordCount++;
            return future;
        }
    }

    /**
     * Complete the request.
     * 
     * @param baseOffset The base offset of the messages assigned by the server
     * @param logAppendTime The log append time or -1 if CreateTime is being used
     * @param exception The exception that occurred (or null if the request was successful)
     */
    // 当消息数据发送给服务器成功后,  每个 Thunk 会执行一次, 就是对每个消息执行一次自定义 Callback.
    public void done(long baseOffset, long logAppendTime, RuntimeException exception) {
        log.trace("Produced messages to topic-partition {} with base offset offset {} and error: {}.",
                  topicPartition, baseOffset, exception);

        if (completed.getAndSet(true))
            throw new IllegalStateException("Batch has already been completed");

        // Set the future before invoking the callbacks as we rely on its state for the `onCompletion` call
        produceFuture.set(baseOffset, logAppendTime, exception);

        // execute callbacks
        for (Thunk thunk : thunks) {
            try {
                if (exception == null) {
                    RecordMetadata metadata = thunk.future.value();
                    thunk.callback.onCompletion(metadata, null);
                } else {
                    thunk.callback.onCompletion(null, exception);
                }
            } catch (Exception e) {
                log.error("Error executing user-provided callback on message for topic-partition '{}'", topicPartition, e);
            }
        }

        produceFuture.done();
    }

    /**
     * A callback and the associated FutureRecordMetadata argument to pass to it.
     */
    // 用于封装用户自定义 Callback 与该消息 RecordMetadata 的内部类
    final private static class Thunk {
        final Callback callback;
        final FutureRecordMetadata future;

        public Thunk(Callback callback, FutureRecordMetadata future) {
            this.callback = callback;
            this.future = future;
        }
    }

    @Override
    public String toString() {
        return "RecordBatch(topicPartition=" + topicPartition + ", recordCount=" + recordCount + ")";
    }

    /**
     * A batch whose metadata is not available should be expired if one of the following is true:
     * <ol>
     *     <li> the batch is not in retry AND request timeout has elapsed after it is ready (full or linger.ms has reached).
     *     <li> the batch is in retry AND request timeout has elapsed after the backoff period ended.
     * </ol>
     * This methods closes this batch and sets {@code expiryErrorMessage} if the batch has timed out.
     * {@link #expirationDone()} must be invoked to complete the produce future and invoke callbacks.
     */
    public boolean maybeExpire(int requestTimeoutMs, long retryBackoffMs, long now, long lingerMs, boolean isFull) {

        if (!this.inRetry() && isFull && requestTimeoutMs < (now - this.lastAppendTime))
            expiryErrorMessage = (now - this.lastAppendTime) + " ms has passed since last append";
        else if (!this.inRetry() && requestTimeoutMs < (now - (this.createdMs + lingerMs)))
            expiryErrorMessage = (now - (this.createdMs + lingerMs)) + " ms has passed since batch creation plus linger time";
        else if (this.inRetry() && requestTimeoutMs < (now - (this.lastAttemptMs + retryBackoffMs)))
            expiryErrorMessage = (now - (this.lastAttemptMs + retryBackoffMs)) + " ms has passed since last attempt plus backoff time";

        boolean expired = expiryErrorMessage != null;
        if (expired)
            close();
        return expired;
    }

    /**
     * Completes the produce future with timeout exception and invokes callbacks.
     * This method should be invoked only if {@link #maybeExpire(int, long, long, long, boolean)}
     * returned true.
     */
    void expirationDone() {
        if (expiryErrorMessage == null)
            throw new IllegalStateException("Batch has not expired");
        this.done(-1L, Record.NO_TIMESTAMP,
                  new TimeoutException("Expiring " + recordCount + " record(s) for " + topicPartition + ": " + expiryErrorMessage));
    }

    /**
     * Returns if the batch is been retried for sending to kafka
     */
    private boolean inRetry() {
        return this.retry;
    }

    /**
     * Set retry to true if the batch is being retried (for send)
     */
    public void setRetry() {
        this.retry = true;
    }

    public MemoryRecords records() {
        return recordsBuilder.build();
    }

    public int sizeInBytes() {
        return recordsBuilder.sizeInBytes();
    }

    public double compressionRate() {
        return recordsBuilder.compressionRate();
    }

    public boolean isFull() {
        return recordsBuilder.isFull();
    }

    public void close() {
        recordsBuilder.close();
    }

    public ByteBuffer buffer() {
        return recordsBuilder.buffer();
    }

    public int initialCapacity() {
        return recordsBuilder.initialCapacity();
    }

    public boolean isWritable() {
        return !recordsBuilder.isClosed();
    }

}
