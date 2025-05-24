/*
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

package org.apache.spark.executor

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.shuffle.ShuffleReadMetricsReporter
import org.apache.spark.util.LongAccumulator


/**
 * :: DeveloperApi ::
 * A collection of accumulators that represent metrics about reading shuffle data.
 * Operations are not thread-safe.
 */

/**
 * 1.线程安全说明‌
 *   明确标注"Operations are not thread-safe"：
 *       1)每个task线程持有独立的ShuffleReadMetrics实例
 *       2)依赖上层调用方保证线程隔离
 *       3) 最终通过mergeShuffleReadMetrics安全合并
 * 2.Shuffle优化关键指标‌
 * ‌网络瓶颈检测‌：高_remoteBlocksFetched+长_fetchWaitTime需调大spark.reducer.maxSizeInFlight
 * ‌数据倾斜定位‌：_remoteBytesRead差异过大表明分区不均
 * ‌内存压力识别‌：_remoteBytesReadToDisk > 0时需要增加spark.shuffle.memoryFraction
 *
 * 3.典型使用场景：
 *  Reduce阶段开始时：
      1. 创建ShuffleReadMetrics实例
      2. 每个分区读取线程更新对应指标
      3. 最终合并到TaskMetrics
    调优示例：
      当_remoteBytesReadToDisk持续增长时：
        - 增加spark.shuffle.spill.batchSize
        - 检查spark.shuffle.file.buffer配置
 *
 */
@DeveloperApi
class ShuffleReadMetrics private[spark] () extends Serializable {
  private[executor] val _remoteBlocksFetched = new LongAccumulator    // 从其他节点获取的块数（网络IO）
  private[executor] val _localBlocksFetched = new LongAccumulator    // 本地直接读取的块数（零拷贝优化）
  private[executor] val _remoteBytesRead = new LongAccumulator       // 通过网络读取的字节总数
  private[executor] val _remoteBytesReadToDisk = new LongAccumulator // 溢出到磁盘的远程数据量（内存不足时）
  private[executor] val _localBytesRead = new LongAccumulator  // 本地读取的字节量
  private[executor] val _fetchWaitTime = new LongAccumulator  // 等待块传输的总时间（毫秒）
  private[executor] val _recordsRead = new LongAccumulator    // 处理后输出的记录条数

  /**
   * Number of remote blocks fetched in this shuffle by this task.
   */
  def remoteBlocksFetched: Long = _remoteBlocksFetched.sum

  /**
   * Number of local blocks fetched in this shuffle by this task.
   */
  def localBlocksFetched: Long = _localBlocksFetched.sum

  /**
   * Total number of remote bytes read from the shuffle by this task.
   */
  def remoteBytesRead: Long = _remoteBytesRead.sum

  /**
   * Total number of remotes bytes read to disk from the shuffle by this task.
   */
  def remoteBytesReadToDisk: Long = _remoteBytesReadToDisk.sum

  /**
   * Shuffle data that was read from the local disk (as opposed to from a remote executor).
   */
  def localBytesRead: Long = _localBytesRead.sum

  /**
   * Time the task spent waiting for remote shuffle blocks. This only includes the time
   * blocking on shuffle input data. For instance if block B is being fetched while the task is
   * still not finished processing block A, it is not considered to be blocking on block B.
   */
  def fetchWaitTime: Long = _fetchWaitTime.sum

  /**
   * Total number of records read from the shuffle by this task.
   */
  def recordsRead: Long = _recordsRead.sum

  /**
   * Total bytes fetched in the shuffle by this task (both remote and local).
   */
  def totalBytesRead: Long = remoteBytesRead + localBytesRead

  /**
   * Number of blocks fetched in this shuffle by this task (remote or local).
   */
  def totalBlocksFetched: Long = remoteBlocksFetched + localBlocksFetched

  private[spark] def incRemoteBlocksFetched(v: Long): Unit = _remoteBlocksFetched.add(v)
  private[spark] def incLocalBlocksFetched(v: Long): Unit = _localBlocksFetched.add(v)
  private[spark] def incRemoteBytesRead(v: Long): Unit = _remoteBytesRead.add(v)
  private[spark] def incRemoteBytesReadToDisk(v: Long): Unit = _remoteBytesReadToDisk.add(v)
  private[spark] def incLocalBytesRead(v: Long): Unit = _localBytesRead.add(v)
  private[spark] def incFetchWaitTime(v: Long): Unit = _fetchWaitTime.add(v)
  private[spark] def incRecordsRead(v: Long): Unit = _recordsRead.add(v)

  private[spark] def setRemoteBlocksFetched(v: Int): Unit = _remoteBlocksFetched.setValue(v)
  private[spark] def setLocalBlocksFetched(v: Int): Unit = _localBlocksFetched.setValue(v)
  private[spark] def setRemoteBytesRead(v: Long): Unit = _remoteBytesRead.setValue(v)
  private[spark] def setRemoteBytesReadToDisk(v: Long): Unit = _remoteBytesReadToDisk.setValue(v)
  private[spark] def setLocalBytesRead(v: Long): Unit = _localBytesRead.setValue(v)
  private[spark] def setFetchWaitTime(v: Long): Unit = _fetchWaitTime.setValue(v)
  private[spark] def setRecordsRead(v: Long): Unit = _recordsRead.setValue(v)

  /**
   * Resets the value of the current metrics (`this`) and merges all the independent
   * [[TempShuffleReadMetrics]] into `this`.
   */
  private[spark] def setMergeValues(metrics: Seq[TempShuffleReadMetrics]): Unit = {
    _remoteBlocksFetched.setValue(0)
    _localBlocksFetched.setValue(0)
    _remoteBytesRead.setValue(0)
    _remoteBytesReadToDisk.setValue(0)
    _localBytesRead.setValue(0)
    _fetchWaitTime.setValue(0)
    _recordsRead.setValue(0)
    metrics.foreach { metric =>
      _remoteBlocksFetched.add(metric.remoteBlocksFetched)
      _localBlocksFetched.add(metric.localBlocksFetched)
      _remoteBytesRead.add(metric.remoteBytesRead)
      _remoteBytesReadToDisk.add(metric.remoteBytesReadToDisk)
      _localBytesRead.add(metric.localBytesRead)
      _fetchWaitTime.add(metric.fetchWaitTime)
      _recordsRead.add(metric.recordsRead)
    }
  }
}


/**
 * A temporary shuffle read metrics holder that is used to collect shuffle read metrics for each
 * shuffle dependency, and all temporary metrics will be merged into the [[ShuffleReadMetrics]] at
 * last.
 */
private[spark] class TempShuffleReadMetrics extends ShuffleReadMetricsReporter {
  private[this] var _remoteBlocksFetched = 0L
  private[this] var _localBlocksFetched = 0L
  private[this] var _remoteBytesRead = 0L
  private[this] var _remoteBytesReadToDisk = 0L
  private[this] var _localBytesRead = 0L
  private[this] var _fetchWaitTime = 0L
  private[this] var _recordsRead = 0L

  override def incRemoteBlocksFetched(v: Long): Unit = _remoteBlocksFetched += v
  override def incLocalBlocksFetched(v: Long): Unit = _localBlocksFetched += v
  override def incRemoteBytesRead(v: Long): Unit = _remoteBytesRead += v
  override def incRemoteBytesReadToDisk(v: Long): Unit = _remoteBytesReadToDisk += v
  override def incLocalBytesRead(v: Long): Unit = _localBytesRead += v
  override def incFetchWaitTime(v: Long): Unit = _fetchWaitTime += v
  override def incRecordsRead(v: Long): Unit = _recordsRead += v

  def remoteBlocksFetched: Long = _remoteBlocksFetched
  def localBlocksFetched: Long = _localBlocksFetched
  def remoteBytesRead: Long = _remoteBytesRead
  def remoteBytesReadToDisk: Long = _remoteBytesReadToDisk
  def localBytesRead: Long = _localBytesRead
  def fetchWaitTime: Long = _fetchWaitTime
  def recordsRead: Long = _recordsRead
}
