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

import java.util.Iterator;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.CopyOnWriteMap;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class acts as a queue that accumulates records into {@link MemoryRecords}
 * instances to be sent to the server.
 * <p>
 * The accumulator uses a bounded amount of memory and append calls will block when that memory is exhausted, unless
 * this behavior is explicitly disabled.
 */
/**
 * RecordAccumulator 对象是 KafkaProducer 对象的buffer区域的管理者.
 * 追加 record 的基本流程:
 * 1. 获取该 topic-partition 对应的 queue, 没有的话会创建一个空的 queue:
 * 2. 向 queue 中追加数据, 先获取 queue 中最新加入的那个 RecordBatch,
 *    如果不存在或者存在但剩余空余不足以添加本条 record 则返回 null, 成功写入的话直接返回结果, 写入成功.
 * 3. 创建一个新的 RecordBatch, 初始化内存大小根据 max(batch.size, Records.LOG_OVERHEAD + Record.recordSize(key, value))
 *    来确定 防止单条 record 过大); 向新建的 RecordBatch 写入 record, 并将 RecordBatch 添加到 queue 中, 返回结果, 写入成功.
 */
public final class RecordAccumulator {

    private static final Logger log = LoggerFactory.getLogger(RecordAccumulator.class);

    private volatile boolean closed;
    private final AtomicInteger flushesInProgress;
    // 记录正在追加数据的计算器
    private final AtomicInteger appendsInProgress;
    // batchSize 是用于确定每个 RecordBatch 的大小
    private final int batchSize;
    // CompressionType 支持4种压缩方式 (NONE, GZIP, SNAPPY, LZ4)
    private final CompressionType compression;
    private final long lingerMs;
    private final long retryBackoffMs;
    // free 是一个 Kafka 自己管理的 BufferPool, 用于管理空闲的 buffer 资源, 非常重要.
    private final BufferPool free;
    private final Time time;
    // batches 是一个 ConcurrentMap<TopicPartition, ArrayDeque<RecordBatch>>, 保证其线程安全和多线程效率
    // 根据 TopicPartition 组织 RecordBatch 队列, 用来存放序列化后的 record
    private final ConcurrentMap<TopicPartition, Deque<RecordBatch>> batches;
    // IncompleteRecordBatches 是用于存放还未写满的 RecordBatch 对象的集合, 其只有一个成员 Set<RecordBatch>
    private final IncompleteRecordBatches incomplete;
    // The following variables are only accessed by the sender thread, so we don't need to protect them.
    private final Set<TopicPartition> muted;
    private int drainIndex;

    /**
     * Create a new record accumulator
     * 
     * @param batchSize The size to use when allocating {@link MemoryRecords} instances
     * @param totalSize The maximum memory the record accumulator can use.
     * @param compression The compression codec for the records
     * @param lingerMs An artificial delay time to add before declaring a records instance that isn't full ready for
     *        sending. This allows time for more records to arrive. Setting a non-zero lingerMs will trade off some
     *        latency for potentially better throughput due to more batching (and hence fewer, larger requests).
     * @param retryBackoffMs An artificial delay time to retry the produce request upon receiving an error. This avoids
     *        exhausting all retries in a short period of time.
     * @param metrics The metrics
     * @param time The time instance to use
     */
    public RecordAccumulator(int batchSize,
                             long totalSize,
                             CompressionType compression,
                             long lingerMs,
                             long retryBackoffMs,
                             Metrics metrics,
                             Time time) {
        this.drainIndex = 0;
        this.closed = false;
        this.flushesInProgress = new AtomicInteger(0);
        this.appendsInProgress = new AtomicInteger(0);
        this.batchSize = batchSize;
        this.compression = compression;
        this.lingerMs = lingerMs;
        this.retryBackoffMs = retryBackoffMs;
        this.batches = new CopyOnWriteMap<>();
        String metricGrpName = "producer-metrics";
        this.free = new BufferPool(totalSize, batchSize, metrics, time, metricGrpName);
        this.incomplete = new IncompleteRecordBatches();
        this.muted = new HashSet<>();
        this.time = time;
        registerMetrics(metrics, metricGrpName);
    }

    private void registerMetrics(Metrics metrics, String metricGrpName) {
        MetricName metricName = metrics.metricName("waiting-threads", metricGrpName, "The number of user threads blocked waiting for buffer memory to enqueue their records");
        Measurable waitingThreads = new Measurable() {
            public double measure(MetricConfig config, long now) {
                return free.queued();
            }
        };
        metrics.addMetric(metricName, waitingThreads);

        metricName = metrics.metricName("buffer-total-bytes", metricGrpName, "The maximum amount of buffer memory the client can use (whether or not it is currently used).");
        Measurable totalBytes = new Measurable() {
            public double measure(MetricConfig config, long now) {
                return free.totalMemory();
            }
        };
        metrics.addMetric(metricName, totalBytes);

        metricName = metrics.metricName("buffer-available-bytes", metricGrpName, "The total amount of buffer memory that is not being used (either unallocated or in the free list).");
        Measurable availableBytes = new Measurable() {
            public double measure(MetricConfig config, long now) {
                return free.availableMemory();
            }
        };
        metrics.addMetric(metricName, availableBytes);

        Sensor bufferExhaustedRecordSensor = metrics.sensor("buffer-exhausted-records");
        metricName = metrics.metricName("buffer-exhausted-rate", metricGrpName, "The average per-second number of record sends that are dropped due to buffer exhaustion");
        bufferExhaustedRecordSensor.add(metricName, new Rate());
    }

    /**
     * Add a record to the accumulator, return the append result
     * <p>
     * The append result will contain the future metadata, and flag for whether the appended batch is full or a new batch is created
     * <p>
     *
     * @param tp The topic/partition to which this record is being sent
     * @param timestamp The timestamp of the record
     * @param key The key for the record
     * @param value The value for the record
     * @param callback The user-supplied callback to execute when the request is complete
     * @param maxTimeToBlock The maximum time in milliseconds to block for buffer memory to be available
     */
    public RecordAppendResult append(TopicPartition tp,
                                     long timestamp,
                                     byte[] key,
                                     byte[] value,
                                     Callback callback,
                                     long maxTimeToBlock) throws InterruptedException {
        // We keep track of the number of appending thread to make sure we do not miss batches in
        // abortIncompleteBatches().
        // 这里是对正在进行追加的record进行计数, 当追加完成时(函数最后的finally), 我们看到计数器自减了.
        appendsInProgress.incrementAndGet();
        try {
            // check if we have an in-progress batch
            // 根据record的TopicPartition(该对象只保存了topic与partition), 确定该record应该存入的batch队列
            // 1. 如果该 TopicPartition 对应的队列为空，则为这个 TopicPartition 新建一个 batch 队列并返回该队列
            // 2. 如果该 TopicPartition 已存在一个 batch 队列，则返回队列的最后一个 batch 队列
            Deque<RecordBatch> dq = getOrCreateDeque(tp);
            // 这里的 batches 是一个 ConcurrentMap<TopicPartition, Deque<RecordBatch>>, 通过putIfAbsent()可以保证其线程安全.
            // Deque<RecordBatch> 不是线程安全的对象, 所以追加record的时候需要使用synchronize关键字保证其线程安全.
            synchronized (dq) {
                // 这里有可能是因为, 该 KafkaProducer 对象此时已经被其他线程所关闭，所以需要抛出异常.
                if (closed)
                    throw new IllegalStateException("Cannot send after the producer is closed.");
                // 追加 record(这里已经是序列化之后的 key/value pair) 到 Deque<RecordBatch> 中去
                // 如果返回值 RecordAppendResult 对象不是 null 则说明追加操作成功(成功将 record 追加到了队列的最后一个batch中)
                // 如果返回值是 null, 则说明这是一个新创建的batch队列, 目前还没有可用的 batch, 我们将为其创建新的 batch 并放入队列.
                RecordAppendResult appendResult = tryAppend(timestamp, key, value, callback, dq);
                if (appendResult != null)
                    return appendResult;
            }

            // we don't have an in-progress record batch try to allocate a new batch
            // 这里我们需要为该 batch 队列创建新的 RecordBatch 并放入队列.
            // 需要初始化相应的 RecordBatch, 要为其分配的大小是: max（batch.size, 包括头文件后的本条消息的大小）
            int size = Math.max(this.batchSize, Records.LOG_OVERHEAD + Record.recordSize(key, value));
            log.trace("Allocating a new {} byte message buffer for topic {} partition {}", size, tp.topic(), tp.partition());
            // 为该 TopicPartition 对应的 RecordBatch 队列 创建一个 ByteBuffer 对象.
            ByteBuffer buffer = free.allocate(size, maxTimeToBlock);
            // 同样地, 为了防止其他线程同样为其创建 ByteBuffer, 在这里对这个队列加锁.
            synchronized (dq) {
                // Need to check if producer is closed again after grabbing the dequeue lock.
                // 再次查看是否 KafkaProducer 已经被关闭.
                if (closed)
                    throw new IllegalStateException("Cannot send after the producer is closed.");

                RecordAppendResult appendResult = tryAppend(timestamp, key, value, callback, dq);
                if (appendResult != null) {
                    // Somebody else found us a batch, return the one we waited for! Hopefully this doesn't happen often...
                    // 如果突然发现这个 queue 已经存在, 那么就释放这个已经分配的空间.
                    // 这里应该同样是多线程导致的问题, 假设线程A与线程B同时为一个 TopicPartition 追加数据,
                    // 而且该 TopicPartition 是一个新的 TopicPartition, 其队列中没有 RecordBatch 对象,
                    // 那么线程A与线程B都会创建 ByteBuffer 并且试图去获得锁,
                    // 假设线程A获得锁(线程B在这里阻塞), 并且成功追加数据, 将该 RecordBatch 加入队列, 退出函数, 释放该锁.
                    // 之后线程B获得锁, 也会将自己的 RecordBatch 加入队列, 但是之前线程A加入的 RecordBatch 并未写满, 造成资源浪费
                    free.deallocate(buffer);
                    return appendResult;
                }

                // 将新创建的 ByteBuffer 对象包装成 MemoryRecords 对象,
                // 然后再包装成 RecordBatch 对象, 并加入这个 TopicPartition 所对应的队列.
                MemoryRecordsBuilder recordsBuilder = MemoryRecords.builder(buffer, compression, TimestampType.CREATE_TIME, this.batchSize);
                RecordBatch batch = new RecordBatch(tp, recordsBuilder, time.milliseconds());
                // 在新创建的 RecordBatch 中追加 record, 并将这个 RecordBatch 对象添加到 batches 集合中.
                // 创建一个 RecordMetadata 的 Future, 用于非阻塞的函数调用.
                // 这里的 FutureRecordMetadata 就是 Future<RecordMetadata> 的一个增强的实现.
                FutureRecordMetadata future = Utils.notNull(batch.tryAppend(timestamp, key, value, callback, time.milliseconds()));

                // 加入这个 TopicPartition 所对应的队列.
                dq.addLast(batch);
                // 该 RecordBatch 还未写满, 放入 IncompleteRecordBatches 集合.
                incomplete.add(batch);
                // 如果（dp.size() > 1), 就证明这个 queue 有一个 batch 是可以发送了
                return new RecordAppendResult(future, dq.size() > 1 || batch.isFull(), true);
            }
        } finally {
            appendsInProgress.decrementAndGet();
        }
    }

    /**
     * If `RecordBatch.tryAppend` fails (i.e. the record batch is full), close its memory records to release temporary
     * resources (like compression streams buffers).
     */
    // 追加数据, 将 record 放入其 TopicPartition 的 RecordBatch 队列的最后一个 RecordBatch 中.
    private RecordAppendResult tryAppend(long timestamp, byte[] key, byte[] value, Callback callback, Deque<RecordBatch> deque) {
        RecordBatch last = deque.peekLast();
        if (last != null) {
            FutureRecordMetadata future = last.tryAppend(timestamp, key, value, callback, time.milliseconds());
            if (future == null)
                last.close();
            else
                return new RecordAppendResult(future, deque.size() > 1 || last.isFull(), false);
        }
        return null;
    }

    /**
     * Abort the batches that have been sitting in RecordAccumulator for more than the configured requestTimeout
     * due to metadata being unavailable
     */
    public List<RecordBatch> abortExpiredBatches(int requestTimeout, long now) {
        List<RecordBatch> expiredBatches = new ArrayList<>();
        int count = 0;
        for (Map.Entry<TopicPartition, Deque<RecordBatch>> entry : this.batches.entrySet()) {
            Deque<RecordBatch> dq = entry.getValue();
            TopicPartition tp = entry.getKey();
            // We only check if the batch should be expired if the partition does not have a batch in flight.
            // This is to prevent later batches from being expired while an earlier batch is still in progress.
            // Note that `muted` is only ever populated if `max.in.flight.request.per.connection=1` so this protection
            // is only active in this case. Otherwise the expiration order is not guaranteed.
            if (!muted.contains(tp)) {
                synchronized (dq) {
                    // iterate over the batches and expire them if they have been in the accumulator for more than requestTimeOut
                    RecordBatch lastBatch = dq.peekLast();
                    Iterator<RecordBatch> batchIterator = dq.iterator();
                    while (batchIterator.hasNext()) {
                        RecordBatch batch = batchIterator.next();
                        boolean isFull = batch != lastBatch || batch.isFull();
                        // Check if the batch has expired. Expired batches are closed by maybeExpire, but callbacks
                        // are invoked after completing the iterations, since sends invoked from callbacks
                        // may append more batches to the deque being iterated. The batch is deallocated after
                        // callbacks are invoked.
                        if (batch.maybeExpire(requestTimeout, retryBackoffMs, now, this.lingerMs, isFull)) {
                            expiredBatches.add(batch);
                            count++;
                            batchIterator.remove();
                        } else {
                            // Stop at the first batch that has not expired.
                            break;
                        }
                    }
                }
            }
        }
        if (!expiredBatches.isEmpty()) {
            log.trace("Expired {} batches in accumulator", count);
            for (RecordBatch batch : expiredBatches) {
                batch.expirationDone();
                deallocate(batch);
            }
        }

        return expiredBatches;
    }

    /**
     * Re-enqueue the given record batch in the accumulator to retry
     */
    public void reenqueue(RecordBatch batch, long now) {
        batch.attempts++;
        batch.lastAttemptMs = now;
        batch.lastAppendTime = now;
        batch.setRetry();
        Deque<RecordBatch> deque = getOrCreateDeque(batch.topicPartition);
        synchronized (deque) {
            deque.addFirst(batch);
        }
    }

    /**
     * Get a list of nodes whose partitions are ready to be sent, and the earliest time at which any non-sendable
     * partition will be ready; Also return the flag for whether there are any unknown leaders for the accumulated
     * partition batches.
     * <p>
     * A destination node is ready to send data if:
     * <ol>
     * <li>There is at least one partition that is not backing off its send
     * <li><b>and</b> those partitions are not muted (to prevent reordering if
     *   {@value org.apache.kafka.clients.producer.ProducerConfig#MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION}
     *   is set to one)</li>
     * <li><b>and <i>any</i></b> of the following are true</li>
     * <ul>
     *     <li>The record set is full</li>
     *     <li>The record set has sat in the accumulator for at least lingerMs milliseconds</li>
     *     <li>The accumulator is out of memory and threads are blocking waiting for data (in this case all partitions
     *     are immediately considered ready).</li>
     *     <li>The accumulator has been closed</li>
     * </ul>
     * </ol>
     */
    /**
     * 获取那些已经可以发送的 RecordBatch 对应的 nodes, 条件如下:
     * (1). 队列中有多个 RecordBatche 或者第一个 RecordBatch 已满
     * (2). 是否超时
     * (3). 是否有其他线程在等待 BufferPool 释放空间, 即 BufferPool 的空间耗尽了
     * (4). 是否有线程在等待 flush 操作完成
     * (5). Sender 线程准备关闭
     *
     * 它会遍历 batches 中的每个 TopicPartition, 首先查找当前分区 leader 副本所在的 Node,
     * 如果满足上面5个条件就把这个 Node 的信息加入 readyNodes 集合.
     * 遍历完成后返回 ReadyCheckResult 对象
     */
    public ReadyCheckResult ready(Cluster cluster, long nowMs) {
        Set<Node> readyNodes = new HashSet<>();
        long nextReadyCheckDelayMs = Long.MAX_VALUE;
        Set<String> unknownLeaderTopics = new HashSet<>();

        boolean exhausted = this.free.queued() > 0;
        for (Map.Entry<TopicPartition, Deque<RecordBatch>> entry : this.batches.entrySet()) {
            TopicPartition part = entry.getKey();
            Deque<RecordBatch> deque = entry.getValue();

            // 找到该 partition 的 leader replica 所在的 Node
            // 因为消息实际上是发送给 leader replica, 在通过同步复制发送到其他 replica 中的
            Node leader = cluster.leaderFor(part);
            synchronized (deque) {
                if (leader == null && !deque.isEmpty()) {
                    // This is a partition for which leader is not known, but messages are available to send.
                    // Note that entries are currently not removed from batches when deque is empty.
                    // 这个 partition 未知, 记录下这个 partition 的信息
                    unknownLeaderTopics.add(part.topic());
                } else if (!readyNodes.contains(leader) && !muted.contains(part)) {
                    // 判断这个节点是否还未加入 readyNodes 集合,
                    // 因为有可能多个 partition 的 leader replica 位于同一个 Node 上.
                    // 取这个 TopicPartition 队列中的第一个 RecordBatch
                    RecordBatch batch = deque.peekFirst();
                    if (batch != null) {
                        boolean backingOff = batch.attempts > 0 && batch.lastAttemptMs + retryBackoffMs > nowMs;
                        long waitedTimeMs = nowMs - batch.lastAttemptMs;
                        long timeToWaitMs = backingOff ? retryBackoffMs : lingerMs;
                        long timeLeftMs = Math.max(timeToWaitMs - waitedTimeMs, 0);
                        // 条件(1)
                        boolean full = deque.size() > 1 || batch.isFull();
                        // 条件(2)
                        boolean expired = waitedTimeMs >= timeToWaitMs;
                        // 条件 (3),(4),(5)
                        boolean sendable = full || expired || exhausted || closed || flushInProgress();
                        if (sendable && !backingOff) {
                            readyNodes.add(leader);
                        } else {
                            // Note that this results in a conservative estimate since an un-sendable partition may have
                            // a leader that will later be found to have sendable data. However, this is good enough
                            // since we'll just wake up and then sleep again for the remaining time.
                            nextReadyCheckDelayMs = Math.min(timeLeftMs, nextReadyCheckDelayMs);
                        }
                    }
                }
            }
        }

        return new ReadyCheckResult(readyNodes, nextReadyCheckDelayMs, unknownLeaderTopics);
    }

    /**
     * @return Whether there is any unsent record in the accumulator.
     */
    public boolean hasUnsent() {
        for (Map.Entry<TopicPartition, Deque<RecordBatch>> entry : this.batches.entrySet()) {
            Deque<RecordBatch> deque = entry.getValue();
            synchronized (deque) {
                if (!deque.isEmpty())
                    return true;
            }
        }
        return false;
    }

    /**
     * Drain all the data for the given nodes and collate them into a list of batches that will fit within the specified
     * size on a per-node basis. This method attempts to avoid choosing the same topic-node over and over.
     * 
     * @param cluster The current cluster metadata
     * @param nodes The list of node to drain
     * @param maxSize The maximum number of bytes to drain
     * @param now The current unix time in milliseconds
     * @return A list of {@link RecordBatch} for each node specified with total size less than the requested maxSize.
     */
    /**
     * drain() 方法会根据 ready() 发放返回的 Node 集合获取要发送的数据, 返回 Map<Integer, List<RecordBatch>>,
     * 该 Map 的 key 是 NodeId, value 是发送给这个 Node 的 RecordBatch 集合.
     * drain() 方法由 Sender 线程调用.
     * drain() 发放的核心逻辑是将一个 TopicPartition -> RecordBatch 的映射转化成 NodeId -> RecordBatch 的映射.
     * 因为底层网络 I/O 只关心消息发往哪些 node, 而高层 API 关系的消息发往哪些 TopicPartition.
     */
    public Map<Integer, List<RecordBatch>> drain(Cluster cluster,
                                                 Set<Node> nodes,
                                                 int maxSize,
                                                 long now) {
        if (nodes.isEmpty())
            return Collections.emptyMap();

        Map<Integer, List<RecordBatch>> batches = new HashMap<>();
        for (Node node : nodes) {
            int size = 0;
            // 获取该 node 上的 partition 集合
            List<PartitionInfo> parts = cluster.partitionsForNode(node.id());
            List<RecordBatch> ready = new ArrayList<>();
            /* to make starvation less likely this loop doesn't start at 0 */
            int start = drainIndex = drainIndex % parts.size();
            do {
                // 获取分区详细情况
                PartitionInfo part = parts.get(drainIndex);
                TopicPartition tp = new TopicPartition(part.topic(), part.partition());
                // Only proceed if the partition has no in-flight batches.
                if (!muted.contains(tp)) {
                    // 获取该 partition 对应的 RecordBatch 队列
                    Deque<RecordBatch> deque = getDeque(new TopicPartition(part.topic(), part.partition()));
                    if (deque != null) {
                        synchronized (deque) {
                            // 获取队列第一个 RecordBatch
                            RecordBatch first = deque.peekFirst();
                            if (first != null) {
                                boolean backoff = first.attempts > 0 && first.lastAttemptMs + retryBackoffMs > now;
                                // Only drain the batch if it is not during backoff period.
                                if (!backoff) {
                                    if (size + first.sizeInBytes() > maxSize && !ready.isEmpty()) {
                                        // there is a rare case that a single batch size is larger than the request size due
                                        // to compression; in this case we will still eventually send this batch in a single
                                        // request
                                        // 数据量已满, 结束循环
                                        break;
                                    } else {
                                        // 从队列中获取一个 RecordBatch 并将其加入 ready 集合
                                        // 每个 TopicPartition 只取一个 RecordBatch
                                        RecordBatch batch = deque.pollFirst();
                                        batch.close();
                                        size += batch.sizeInBytes();
                                        ready.add(batch);
                                        batch.drainedMs = now;
                                    }
                                }
                            }
                        }
                    }
                }
                this.drainIndex = (this.drainIndex + 1) % parts.size();
            } while (start != drainIndex);
            batches.put(node.id(), ready);
        }
        return batches;
    }

    private Deque<RecordBatch> getDeque(TopicPartition tp) {
        return batches.get(tp);
    }

    /**
     * Get the deque for the given topic-partition, creating it if necessary.
     */
    // 1. 如果该TopicPartition对应的队列为空，则为这个TopicPartition新建一个batch队列,
    // 2. 如果该TopicPartition已存在一个batch队列，则把这个record追加到这个batch队列的最后一个batch当中.
    // 其中 batches 是一个 ConcurrentMap<TopicPartition, Deque<RecordBatch>> 对象，通过putIfAbsent()可以保证其线程安全.
    // 但是 Deque<RecordBatch> 不是线程安全的对象, 所以追加record的时候需要使用synchronize关键字保证其线程安全.
    private Deque<RecordBatch> getOrCreateDeque(TopicPartition tp) {
        Deque<RecordBatch> d = this.batches.get(tp);
        if (d != null)
            return d;
        d = new ArrayDeque<>();
        Deque<RecordBatch> previous = this.batches.putIfAbsent(tp, d);
        if (previous == null)
            return d;
        else
            return previous;
    }

    /**
     * Deallocate the record batch
     */
    public void deallocate(RecordBatch batch) {
        incomplete.remove(batch);
        free.deallocate(batch.buffer(), batch.initialCapacity());
    }
    
    /**
     * Are there any threads currently waiting on a flush?
     *
     * package private for test
     */
    boolean flushInProgress() {
        return flushesInProgress.get() > 0;
    }

    /* Visible for testing */
    Map<TopicPartition, Deque<RecordBatch>> batches() {
        return Collections.unmodifiableMap(batches);
    }
    
    /**
     * Initiate the flushing of data from the accumulator...this makes all requests immediately ready
     */
    public void beginFlush() {
        this.flushesInProgress.getAndIncrement();
    }

    /**
     * Are there any threads currently appending messages?
     */
    private boolean appendsInProgress() {
        return appendsInProgress.get() > 0;
    }

    /**
     * Mark all partitions as ready to send and block until the send is complete
     */
    public void awaitFlushCompletion() throws InterruptedException {
        try {
            for (RecordBatch batch : this.incomplete.all())
                batch.produceFuture.await();
        } finally {
            this.flushesInProgress.decrementAndGet();
        }
    }

    /**
     * This function is only called when sender is closed forcefully. It will fail all the
     * incomplete batches and return.
     */
    public void abortIncompleteBatches() {
        // We need to keep aborting the incomplete batch until no thread is trying to append to
        // 1. Avoid losing batches.
        // 2. Free up memory in case appending threads are blocked on buffer full.
        // This is a tight loop but should be able to get through very quickly.
        do {
            abortBatches();
        } while (appendsInProgress());
        // After this point, no thread will append any messages because they will see the close
        // flag set. We need to do the last abort after no thread was appending in case there was a new
        // batch appended by the last appending thread.
        abortBatches();
        this.batches.clear();
    }

    /**
     * Go through incomplete batches and abort them.
     */
    private void abortBatches() {
        for (RecordBatch batch : incomplete.all()) {
            Deque<RecordBatch> dq = getDeque(batch.topicPartition);
            // Close the batch before aborting
            synchronized (dq) {
                batch.close();
                dq.remove(batch);
            }
            batch.done(-1L, Record.NO_TIMESTAMP, new IllegalStateException("Producer is closed forcefully."));
            deallocate(batch);
        }
    }

    public void mutePartition(TopicPartition tp) {
        muted.add(tp);
    }

    public void unmutePartition(TopicPartition tp) {
        muted.remove(tp);
    }

    /**
     * Close this accumulator and force all the record buffers to be drained
     */
    public void close() {
        this.closed = true;
    }

    /*
     * Metadata about a record just appended to the record accumulator
     */
    public final static class RecordAppendResult {
        public final FutureRecordMetadata future;
        public final boolean batchIsFull;
        public final boolean newBatchCreated;

        public RecordAppendResult(FutureRecordMetadata future, boolean batchIsFull, boolean newBatchCreated) {
            this.future = future;
            this.batchIsFull = batchIsFull;
            this.newBatchCreated = newBatchCreated;
        }
    }

    /*
     * The set of nodes that have at least one complete record batch in the accumulator
     */
    public final static class ReadyCheckResult {
        public final Set<Node> readyNodes;
        public final long nextReadyCheckDelayMs;
        public final Set<String> unknownLeaderTopics;

        public ReadyCheckResult(Set<Node> readyNodes, long nextReadyCheckDelayMs, Set<String> unknownLeaderTopics) {
            this.readyNodes = readyNodes;
            this.nextReadyCheckDelayMs = nextReadyCheckDelayMs;
            this.unknownLeaderTopics = unknownLeaderTopics;
        }
    }
    
    /*
     * A threadsafe helper class to hold RecordBatches that haven't been ack'd yet
     */
    private final static class IncompleteRecordBatches {
        private final Set<RecordBatch> incomplete;

        public IncompleteRecordBatches() {
            this.incomplete = new HashSet<RecordBatch>();
        }
        
        public void add(RecordBatch batch) {
            synchronized (incomplete) {
                this.incomplete.add(batch);
            }
        }
        
        public void remove(RecordBatch batch) {
            synchronized (incomplete) {
                boolean removed = this.incomplete.remove(batch);
                if (!removed)
                    throw new IllegalStateException("Remove from the incomplete set failed. This should be impossible.");
            }
        }
        
        public Iterable<RecordBatch> all() {
            synchronized (incomplete) {
                return new ArrayList<>(this.incomplete);
            }
        }
    }

}
