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
import org.apache.spark.shuffle.ShuffleWriteMetricsReporter
import org.apache.spark.util.LongAccumulator


/**
 * :: DeveloperApi ::
 * A collection of accumulators that represent metrics about writing shuffle data.
 * Operations are not thread-safe.
 */

/**
 * ‌实现注意事项‌
 *   非线程安全设计：每个Task独占实例
 *   精确递减方法：应对Spark推测执行机制（Speculative Execution）
 *   纳秒计时：避免Executor时钟漂移影响统计
 *
 * 与ShuffleReadMetrics的对比‌
 * | 维度 | WriteMetrics | ReadMetrics |
 * |--------------|----------------------------|-----------------------------|
 * | 核心指标 | 写入量/阻塞时间 | 网络传输量/等待时间 |
 * | 关键优化点 | 序列化效率/磁盘溢出 | 网络缓冲/数据本地化 |
 * | 典型问题 | 写入倾斜/磁盘IO瓶颈 | 数据倾斜/网络拥塞 |
 *
 *性能调优关联参数
 *   1.高writeTime时需检查： spark.shuffle.spill.compress=true   spark.shuffle.file.buffer=32KB -> 64KB
 *   2.大bytesWritten但小recordsWritten时： spark.serializer=KryoSerializer  spark.shuffle.compress=true
 * 典型工作流程：
 * Map阶段结束时：
 * 1. 创建ShuffleWriteMetrics实例
 * 2. 写入数据前记录startTime = System.nanoTime()
 * 3. 每完成一个数据块：
 *    - incBytesWritten(blockSize)
 *    - incRecordsWritten(recordCount)
 *    - incWriteTime(System.nanoTime() - startTime)
 * 4. 异常时调用decRecordsWritten方法回滚
 *
 *
 */
@DeveloperApi
class ShuffleWriteMetrics private[spark] () extends ShuffleWriteMetricsReporter with Serializable {
  private[executor] val _bytesWritten = new LongAccumulator            //累计写入字节数（含序列化后大小）
  private[executor] val _recordsWritten = new LongAccumulator         //写入的记录条数（原始数据计数）
  private[executor] val _writeTime = new LongAccumulator              //阻塞写入时间（纳秒级精度）

  /**
   * Number of bytes written for the shuffle by this task.
   */
  def bytesWritten: Long = _bytesWritten.sum

  /**
   * Total number of records written to the shuffle by this task.
   */
  def recordsWritten: Long = _recordsWritten.sum

  /**
   * Time the task spent blocking on writes to disk or buffer cache, in nanoseconds.
   */
  def writeTime: Long = _writeTime.sum

  private[spark] override def incBytesWritten(v: Long): Unit = _bytesWritten.add(v)
  private[spark] override def incRecordsWritten(v: Long): Unit = _recordsWritten.add(v)
  private[spark] override def incWriteTime(v: Long): Unit = _writeTime.add(v)  // 纳秒级时间统计
  private[spark] override def decBytesWritten(v: Long): Unit = {
    _bytesWritten.setValue(bytesWritten - v)
  }
  private[spark] override def decRecordsWritten(v: Long): Unit = {       //递减方法,用于纠正写入统计（如任务失败回滚时）
    _recordsWritten.setValue(recordsWritten - v)
  }
}
