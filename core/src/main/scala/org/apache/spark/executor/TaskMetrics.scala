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

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, LinkedHashMap}

import org.apache.spark._
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.Tests.IS_TESTING
import org.apache.spark.scheduler.AccumulableInfo
import org.apache.spark.storage.{BlockId, BlockStatus}
import org.apache.spark.util._


/**
 * :: DeveloperApi ::
 * Metrics tracked during the execution of a task.
 *
 * This class is wrapper around a collection of internal accumulators that represent metrics
 * associated with a task. The local values of these accumulators are sent from the executor
 * to the driver when the task completes. These values are then merged into the corresponding
 * accumulator previously registered on the driver.
 *
 * The accumulator updates are also sent to the driver periodically (on executor heartbeat)
 * and when the task failed with an exception. The [[TaskMetrics]] object itself should never
 * be sent to the driver.
 */
//TaskMetrics用于精确采集和传输任务执行过程中的性能指标
@DeveloperApi
class TaskMetrics private[spark]() extends Serializable {
  // Each metric is internally represented as an accumulator
  /**
   * ‌时间监控类变量
   */
  private val _executorDeserializeTime = new LongAccumulator //任务反序列化耗时（毫秒级）
  private val _executorDeserializeCpuTime = new LongAccumulator // 反序列化CPU时间（纳秒级）
  private val _executorRunTime = new LongAccumulator // 任务执行总时间（含shuffle读取）
  private val _executorCpuTime = new LongAccumulator // 纯CPU计算时间（排除I/O等待）

  /**
   * 资源消耗类变量
   */
  private val _jvmGCTime = new LongAccumulator // GC停顿时间（用于内存调优）

  private val _memoryBytesSpilled = new LongAccumulator //内存溢出到磁盘的数据量
  private val _diskBytesSpilled = new LongAccumulator //直接写入磁盘的数据量
  private val _peakExecutionMemory = new LongAccumulator //内存使用峰值（单位字节）

  /**
   * 任务结果类变量
   */
  private val _resultSize = new LongAccumulator // 任务返回结果序列化后大小
  private val _resultSerializationTime = new LongAccumulator //任务结果序列化耗时

  /**
   * 特殊状态变量
   */
  private val _updatedBlockStatuses = new CollectionAccumulator[(BlockId, BlockStatus)] // 记录任务修改的块状态（用于故障恢复）

  /**
   * Time taken on the executor to deserialize this task.
   */
  def executorDeserializeTime: Long = _executorDeserializeTime.sum

  /**
   * CPU Time taken on the executor to deserialize this task in nanoseconds.
   */
  def executorDeserializeCpuTime: Long = _executorDeserializeCpuTime.sum

  /**
   * Time the executor spends actually running the task (including fetching shuffle data).
   */
  def executorRunTime: Long = _executorRunTime.sum

  /**
   * CPU Time the executor spends actually running the task
   * (including fetching shuffle data) in nanoseconds.
   */
  def executorCpuTime: Long = _executorCpuTime.sum

  /**
   * The number of bytes this task transmitted back to the driver as the TaskResult.
   */
  def resultSize: Long = _resultSize.sum

  /**
   * Amount of time the JVM spent in garbage collection while executing this task.
   */
  def jvmGCTime: Long = _jvmGCTime.sum

  /**
   * Amount of time spent serializing the task result.
   */
  def resultSerializationTime: Long = _resultSerializationTime.sum

  /**
   * The number of in-memory bytes spilled by this task.
   */
  def memoryBytesSpilled: Long = _memoryBytesSpilled.sum

  /**
   * The number of on-disk bytes spilled by this task.
   */
  def diskBytesSpilled: Long = _diskBytesSpilled.sum

  /**
   * Peak memory used by internal data structures created during shuffles, aggregations and
   * joins. The value of this accumulator should be approximately the sum of the peak sizes
   * across all such data structures created in this task. For SQL jobs, this only tracks all
   * unsafe operators and ExternalSort.
   */
  def peakExecutionMemory: Long = _peakExecutionMemory.sum

  /**
   * Storage statuses of any blocks that have been updated as a result of this task.
   *
   * Tracking the _updatedBlockStatuses can use a lot of memory.
   * It is not used anywhere inside of Spark so we would ideally remove it, but its exposed to
   * the user in SparkListenerTaskEnd so the api is kept for compatibility.
   * Tracking can be turned off to save memory via config
   * TASK_METRICS_TRACK_UPDATED_BLOCK_STATUSES.
   */
  def updatedBlockStatuses: Seq[(BlockId, BlockStatus)] = {
    // This is called on driver. All accumulator updates have a fixed value. So it's safe to use
    // `asScala` which accesses the internal values using `java.util.Iterator`.
    _updatedBlockStatuses.value.asScala.toSeq
  }

  // Setters and increment-ers
  private[spark] def setExecutorDeserializeTime(v: Long): Unit =
    _executorDeserializeTime.setValue(v)

  private[spark] def setExecutorDeserializeCpuTime(v: Long): Unit =
    _executorDeserializeCpuTime.setValue(v)

  private[spark] def setExecutorRunTime(v: Long): Unit = _executorRunTime.setValue(v)

  private[spark] def setExecutorCpuTime(v: Long): Unit = _executorCpuTime.setValue(v)

  private[spark] def setResultSize(v: Long): Unit = _resultSize.setValue(v)

  private[spark] def setJvmGCTime(v: Long): Unit = _jvmGCTime.setValue(v)

  private[spark] def setResultSerializationTime(v: Long): Unit =
    _resultSerializationTime.setValue(v)

  private[spark] def setPeakExecutionMemory(v: Long): Unit = _peakExecutionMemory.setValue(v)

  private[spark] def incMemoryBytesSpilled(v: Long): Unit = _memoryBytesSpilled.add(v)

  private[spark] def incDiskBytesSpilled(v: Long): Unit = _diskBytesSpilled.add(v)

  private[spark] def incPeakExecutionMemory(v: Long): Unit = _peakExecutionMemory.add(v)

  private[spark] def incUpdatedBlockStatuses(v: (BlockId, BlockStatus)): Unit =
    _updatedBlockStatuses.add(v)

  private[spark] def setUpdatedBlockStatuses(v: java.util.List[(BlockId, BlockStatus)]): Unit =
    _updatedBlockStatuses.setValue(v)

  private[spark] def setUpdatedBlockStatuses(v: Seq[(BlockId, BlockStatus)]): Unit =
    _updatedBlockStatuses.setValue(v.asJava)

  /**
   * Metrics related to reading data from a [[org.apache.spark.rdd.HadoopRDD]] or from persisted
   * data, defined only in tasks with input.
   */
  val inputMetrics: InputMetrics = new InputMetrics() // 跟踪HDFS/Hive等数据源的读取情况,包含：字节数、记录数、耗时，注意：在Kafka源中还会跟踪偏移量

  /**
   * Metrics related to writing data externally (e.g. to a distributed filesystem),
   * defined only in tasks with output.
   */
  val outputMetrics: OutputMetrics = new OutputMetrics() // 记录写入外部存储(如HDFS/S3)的指标

  /**
   * Metrics related to shuffle read aggregated across all shuffle dependencies.
   * This is defined only if there are shuffle dependencies in this task.
   */
  val shuffleReadMetrics: ShuffleReadMetrics = new ShuffleReadMetrics() // 聚合所有shuffle依赖的读取指标，Read：远程块数、本地块数、获取数据大小

  /**
   * Metrics related to shuffle write, defined only in shuffle map stages.
   */
  val shuffleWriteMetrics: ShuffleWriteMetrics = new ShuffleWriteMetrics() // 仅shuffle map阶段存在，写入记录数、字节数、溢出文件数

  /**
   * A list of [[TempShuffleReadMetrics]], one per shuffle dependency.
   *
   * A task may have multiple shuffle readers for multiple dependencies. To avoid synchronization
   * issues from readers in different threads, in-progress tasks use a [[TempShuffleReadMetrics]]
   * for each dependency and merge these metrics before reporting them to the driver.
   */

  /**
   * 临时指标优化设计,用于解决多依赖并发问题：每个shuffle依赖独立记录，任务结束时合并到shuffleReadMetrics，
   * 注意：当tempShuffleReadMetrics持续增长时需检查数据倾斜
   *
   * 运行示例：
   *  Map阶段:
   *   inputMetrics记录输入
   *   shuffleWriteMetrics记录输出
   *
   *  Reduce阶段:
   *   shuffleReadMetrics聚合多个map输出
   *   outputMetrics记录最终输出
   *
   */
  @transient private lazy val tempShuffleReadMetrics = new ArrayBuffer[TempShuffleReadMetrics]

  /**
   * Create a [[TempShuffleReadMetrics]] for a particular shuffle dependency.
   *
   * All usages are expected to be followed by a call to [[mergeShuffleReadMetrics]], which
   * merges the temporary values synchronously. Otherwise, all temporary data collected will
   * be lost.
   */
  private[spark] def createTempShuffleReadMetrics(): TempShuffleReadMetrics = synchronized {
    val readMetrics = new TempShuffleReadMetrics
    tempShuffleReadMetrics += readMetrics
    readMetrics
  }

  /**
   * Merge values across all temporary [[ShuffleReadMetrics]] into `_shuffleReadMetrics`.
   * This is expected to be called on executor heartbeat and at the end of a task.
   */

  /**
   * ‌指标合并机制, 触发时机： Executor心跳周期（默认3秒）、 任务结束时刻
   *
   * 合并策略：
   *  1）将多个临时指标聚合成全局shuffleReadMetrics
   *  2）避免driver端合并竞争
   *
   */
  private[spark] def mergeShuffleReadMetrics(): Unit = synchronized {
    if (tempShuffleReadMetrics.nonEmpty) {
      shuffleReadMetrics.setMergeValues(tempShuffleReadMetrics.toSeq)
    }
  }

  // Only used for test
  private[spark] val testAccum = sys.props.get(IS_TESTING.key).map(_ => new LongAccumulator)  //‌测试专用累加器


  import InternalAccumulator._

  @transient private[spark] lazy val nameToAccums = LinkedHashMap(
    EXECUTOR_DESERIALIZE_TIME -> _executorDeserializeTime,
    EXECUTOR_DESERIALIZE_CPU_TIME -> _executorDeserializeCpuTime,
    EXECUTOR_RUN_TIME -> _executorRunTime,
    EXECUTOR_CPU_TIME -> _executorCpuTime,
    RESULT_SIZE -> _resultSize,
    JVM_GC_TIME -> _jvmGCTime,
    RESULT_SERIALIZATION_TIME -> _resultSerializationTime,
    MEMORY_BYTES_SPILLED -> _memoryBytesSpilled,
    DISK_BYTES_SPILLED -> _diskBytesSpilled,
    PEAK_EXECUTION_MEMORY -> _peakExecutionMemory,
    UPDATED_BLOCK_STATUSES -> _updatedBlockStatuses,
    shuffleRead.REMOTE_BLOCKS_FETCHED -> shuffleReadMetrics._remoteBlocksFetched,
    shuffleRead.LOCAL_BLOCKS_FETCHED -> shuffleReadMetrics._localBlocksFetched,
    shuffleRead.REMOTE_BYTES_READ -> shuffleReadMetrics._remoteBytesRead,
    shuffleRead.REMOTE_BYTES_READ_TO_DISK -> shuffleReadMetrics._remoteBytesReadToDisk,
    shuffleRead.LOCAL_BYTES_READ -> shuffleReadMetrics._localBytesRead,
    shuffleRead.FETCH_WAIT_TIME -> shuffleReadMetrics._fetchWaitTime,
    shuffleRead.RECORDS_READ -> shuffleReadMetrics._recordsRead,
    shuffleWrite.BYTES_WRITTEN -> shuffleWriteMetrics._bytesWritten,
    shuffleWrite.RECORDS_WRITTEN -> shuffleWriteMetrics._recordsWritten,
    shuffleWrite.WRITE_TIME -> shuffleWriteMetrics._writeTime,
    input.BYTES_READ -> inputMetrics._bytesRead,
    input.RECORDS_READ -> inputMetrics._recordsRead,
    output.BYTES_WRITTEN -> outputMetrics._bytesWritten,
    output.RECORDS_WRITTEN -> outputMetrics._recordsWritten
  ) ++ testAccum.map(TEST_ACCUM -> _)

  @transient private[spark] lazy val internalAccums: Seq[AccumulatorV2[_, _]] =
    nameToAccums.values.toIndexedSeq

  /* ========================== *
   |        OTHER THINGS        |
   * ========================== */

  private[spark] def register(sc: SparkContext): Unit = {
    nameToAccums.foreach {
      //为每个累加器调用register方法，将其注册到SparkContext中
      case (name, acc) => acc.register(sc, name = Some(name), countFailedValues = true)  //countFailedValues=true表示统计失败任务的值（如推测执行场景）
    }
  }

  /**
   * External accumulators registered with this task.
   */
  @transient private[spark] lazy val externalAccums = new ArrayBuffer[AccumulatorV2[_, _]]

  private[spark] def registerAccumulator(a: AccumulatorV2[_, _]): Unit = {
    externalAccums += a
  }

  private[spark] def accumulators(): Seq[AccumulatorV2[_, _]] = internalAccums ++ externalAccums

  private[spark] def nonZeroInternalAccums(): Seq[AccumulatorV2[_, _]] = {
    // RESULT_SIZE accumulator is always zero at executor, we need to send it back as its
    // value will be updated at driver side.
    internalAccums.filter(a => !a.isZero || a == _resultSize)
  }
}


private[spark] object TaskMetrics extends Logging {

  import InternalAccumulator._

  /**
   * Create an empty task metrics that doesn't register its accumulators.
   */
  def empty: TaskMetrics = {
    val tm = new TaskMetrics
    tm.nameToAccums.foreach { case (name, acc) =>
      acc.metadata = AccumulatorMetadata(AccumulatorContext.newId(), Some(name), true)
    }
    tm
  }

  def registered: TaskMetrics = {
    val tm = empty
    tm.internalAccums.foreach(AccumulatorContext.register)
    tm
  }

  /**
   * Construct a [[TaskMetrics]] object from a list of [[AccumulableInfo]], called on driver only.
   * The returned [[TaskMetrics]] is only used to get some internal metrics, we don't need to take
   * care of external accumulator info passed in.
   */
  def fromAccumulatorInfos(infos: Seq[AccumulableInfo]): TaskMetrics = {
    val tm = new TaskMetrics
    infos.filter(info => info.name.isDefined && info.update.isDefined).foreach { info =>
      val name = info.name.get
      val value = info.update.get
      if (name == UPDATED_BLOCK_STATUSES) {
        tm.setUpdatedBlockStatuses(value.asInstanceOf[java.util.List[(BlockId, BlockStatus)]])
      } else {
        tm.nameToAccums.get(name).foreach(
          _.asInstanceOf[LongAccumulator].setValue(value.asInstanceOf[Long])
        )
      }
    }
    tm
  }

  /**
   * Construct a [[TaskMetrics]] object from a list of accumulator updates, called on driver only.
   */
  def fromAccumulators(accums: Seq[AccumulatorV2[_, _]]): TaskMetrics = {
    val tm = new TaskMetrics
    for (acc <- accums) {
      val name = acc.name
      if (name.isDefined && tm.nameToAccums.contains(name.get)) {
        val tmAcc = tm.nameToAccums(name.get).asInstanceOf[AccumulatorV2[Any, Any]]
        tmAcc.metadata = acc.metadata
        tmAcc.merge(acc.asInstanceOf[AccumulatorV2[Any, Any]])
      } else {
        tm.externalAccums += acc
      }
    }
    tm
  }
}
