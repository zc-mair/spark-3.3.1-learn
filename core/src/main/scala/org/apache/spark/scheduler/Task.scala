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

package org.apache.spark.scheduler

import java.nio.ByteBuffer
import java.util.Properties

import org.apache.spark._
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.config.APP_CALLER_CONTEXT
import org.apache.spark.internal.plugin.PluginContainer
import org.apache.spark.memory.{MemoryMode, TaskMemoryManager}
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.rdd.InputFileBlockHolder
import org.apache.spark.resource.ResourceInformation
import org.apache.spark.util._

/**
 * A unit of execution. We have two kinds of Task's in Spark:
 *
 *  - [[org.apache.spark.scheduler.ShuffleMapTask]]
 *  - [[org.apache.spark.scheduler.ResultTask]]
 *
 * A Spark job consists of one or more stages. The very last stage in a job consists of multiple
 * ResultTasks, while earlier stages consist of ShuffleMapTasks. A ResultTask executes the task
 * and sends the task output back to the driver application. A ShuffleMapTask executes the task
 * and divides the task output to multiple buckets (based on the task's partitioner).
 *
 * @param stageId id of the stage this task belongs to
 * @param stageAttemptId attempt id of the stage this task belongs to
 * @param partitionId index of the number in the RDD
 * @param localProperties copy of thread-local properties set by the user on the driver side.
 * @param serializedTaskMetrics a `TaskMetrics` that is created and serialized on the driver side
 *                              and sent to executor side.
 *
 * The parameters below are optional:
 * @param jobId id of the job this task belongs to
 * @param appId id of the app this task belongs to
 * @param appAttemptId attempt id of the app this task belongs to
 * @param isBarrier whether this task belongs to a barrier stage. Spark must launch all the tasks
 *                  at the same time for a barrier stage.
 */

/**
 * 定义了 Spark 任务的通用执行框架
 *
 * 典型工作流程示例：
 *  1.Executor通过setTaskMemoryManager注入内存管理器
 *  2.TaskRunner线程调用run()方法初始化上下文
 *  3.执行过程中通过taskMemoryManager申请内存
 *  4.若被终止，通过kill()方法级联清理资源
 *  5.最终Executor通过collectAccumulatorUpdates上报指标
 */
private[spark] abstract class Task[T](
    val stageId: Int,          //所属Stage的唯一标识
    val stageAttemptId: Int,   //Stage尝试次数(用于容错)
    val partitionId: Int,      //对应的RDD分区索引
    @transient var localProperties: Properties = new Properties, //不序列化的线程本地属性
    // The default value is only used in tests.
    serializedTaskMetrics: Array[Byte] =
      SparkEnv.get.closureSerializer.newInstance().serialize(TaskMetrics.registered).array(),   //存储在driver端序列化的任务指标数据
    val jobId: Option[Int] = None,        //所属Job的ID(可选)
    val appId: Option[String] = None,     //应用级标识(可选)
    val appAttemptId: Option[String] = None,  //应用尝试标识（可选）
    val isBarrier: Boolean = false) extends Serializable {  //是否屏障任务(需同步执行)

  @transient lazy val metrics: TaskMetrics =
    SparkEnv.get.closureSerializer.newInstance().deserialize(ByteBuffer.wrap(serializedTaskMetrics))  //反序列化得到TaskMetrics对象

  /**
   * Called by [[org.apache.spark.executor.Executor]] to run this task.
   *
   * @param taskAttemptId an identifier for this task attempt that is unique within a SparkContext.
   * @param attemptNumber how many times this task has been attempted (0 for the first attempt)
   * @param resources other host resources (like gpus) that this task attempt can access
   * @return the result of the task along with updates of Accumulators.
   */
  final def run(
      taskAttemptId: Long,
      attemptNumber: Int,
      metricsSystem: MetricsSystem,
      cpus: Int,
      resources: Map[String, ResourceInformation],
      plugins: Option[PluginContainer]): T = {

    //强制要求每个任务至少分配1个CPU核心
    require(cpus > 0, "CPUs per task should be > 0")

    //任务资源注册,在BlockManager注册任务ID，建立shuffle文件写入通道,后续shuffle输出会以shuffle_${shuffleId}_${mapId}_${reduceId}格式存储
    SparkEnv.get.blockManager.registerTask(taskAttemptId)
    // TODO SPARK-24874 Allow create BarrierTaskContext based on partitions, instead of whether
    // the stage is barrier.

    //每个任务都需要构造唯一的上下文
    val taskContext = new TaskContextImpl(
      stageId,
      stageAttemptId, // stageAttemptId and stageAttemptNumber are semantically equal
      partitionId,
      taskAttemptId,
      attemptNumber,
      taskMemoryManager,
      localProperties,
      metricsSystem,
      metrics,
      cpus,
      resources)

    context = if (isBarrier) {
      new BarrierTaskContext(taskContext)  //屏障任务会包装特殊上下文（SPARK-24874待优化点）
    } else {
      taskContext
    }

    //执行环境初始化
    //初始化输入文件块缓存区
    InputFileBlockHolder.initialize()
    //设置线程本地变量保存上下文

    TaskContext.setTaskContext(context)
    //记录执行线程用于后续中断处理
    taskThread = Thread.currentThread()

    //任务终止检查,检查是否被DAGScheduler标记为终止状态,避免启动已被终止的任务
    if (_reasonIfKilled != null) {
      kill(interruptThread = false, _reasonIfKilled)
    }

    /**
     * ‌调用链追踪体系:构建四级调用链（Application → Job → Stage → Task）,用于Spark UI和日志追踪
     *
     * CallerContext与TaskContext职责不同：
     * 1.TaskContext：管理任务执行环境
     *   内存分配（taskMemoryManager）
     *   累加器更新（metricsSystem）
     *   分区信息（partitionId）
     *   任务中断控制（kill机制）
     *
     * 2.CallerContext：建立调用链追踪
     *   关联HDFS/Yarn审计日志（hdfs-audit.log）
     *   构建四级调用标识（App→Job→Stage→Task）
     *   跨系统问题诊断（如NameNode过载溯源）
     *
     * CallerContext与TaskContext设计目标存在差异：
     *   TaskContext是Spark内部执行引擎的核心组件，而CallerContext主要解决与Hadoop生态集成的运维问题
     *
     * 典型使用场景‌
     *  当出现HDFS操作异常时：
     *     通过TaskContext可定位到具体失败的task
     *     通过CallerContext可追溯整个作业链路：YARN审计日志 → HDFS操作日志 → Spark UI
     *
     * 生命周期管理‌
     *   TaskContext随Task启动创建、结束时销毁
     *   CallerContext会持久化到Hadoop系统日志中，CallerContext是轻量级的日志标记机制，通过反射动态加载Hadoop API（见setCurrentContext方法）
     *
     *这种双上下文设计体现了Spark的架构哲学：内部执行与外部集成解耦。正如HDFS-9184提出的，当分布式系统层级增多时，这种跨系统调用链追踪能力对生产环境诊断至关重要。
     */
    new CallerContext(
      "TASK",
      SparkEnv.get.conf.get(APP_CALLER_CONTEXT),
      appId,
      appAttemptId,
      jobId,
      Option(stageId),
      Option(stageAttemptId),
      Option(taskAttemptId),
      Option(attemptNumber)).setCurrentContext()

    //‌插件系统扩展点,触发插件任务启动回调（如监控插件）
    plugins.foreach(_.onTaskStart())

    try {
      runTask(context)  //为抽象方法，由ShuffleMapTask/ResultTask实现，对于ShuffleMapTask来说：该方法执行后生成的shuffle文件会被MapOutputTracker记录
    } catch {
      case e: Throwable =>
        // Catch all errors; run task failure callbacks, and rethrow the exception.
        try {
          context.markTaskFailed(e)  //重算机制，通过markTaskFailed触发下游调度
        } catch {
          case t: Throwable =>
            e.addSuppressed(t)
        }
        context.markTaskCompleted(Some(e))
        throw e
    } finally {
      try {
        // Call the task completion callbacks. If "markTaskCompleted" is called twice, the second
        // one is no-op.
        context.markTaskCompleted(None)  //资源释放三阶段,阶段1：状态标记任务完成
      } finally {
        try {
          Utils.tryLogNonFatalError {
            // Release memory used by this thread for unrolling blocks
            SparkEnv.get.blockManager.memoryStore.releaseUnrollMemoryForThisTask(MemoryMode.ON_HEAP)   // 阶段2：内存释放，同步释放ON_HEAP/OFF_HEAP内存
            SparkEnv.get.blockManager.memoryStore.releaseUnrollMemoryForThisTask(
              MemoryMode.OFF_HEAP)
            // Notify any tasks waiting for execution memory to be freed to wake up and try to
            // acquire memory again. This makes impossible the scenario where a task sleeps forever
            // because there are no other tasks left to notify it. Since this is safe to do but may
            // not be strictly necessary, we should revisit whether we can remove this in the
            // future.
            val memoryManager = SparkEnv.get.memoryManager
            memoryManager.synchronized { memoryManager.notifyAll() }  // 阶段3：唤醒等待线程，通过内存管理器通知其他等待任务
          }
        } finally {
          // Though we unset the ThreadLocal here, the context member variable itself is still
          // queried directly in the TaskRunner to check for FetchFailedExceptions.
          TaskContext.unset()          // 清理线程本地变量
          InputFileBlockHolder.unset()
        }
      }
    }
  }

  /**
   * taskMemoryManager 采用延迟初始化模式，由Executor在任务启动时注入
   * 作用：
   *  1）管理任务堆内/堆外内存分配（通过MemoryConsumer机制）
   *  2）控制shuffle过程中的内存使用（如UnsafeShuffleWriter）
   */
  private var taskMemoryManager: TaskMemoryManager = _

  def setTaskMemoryManager(taskMemoryManager: TaskMemoryManager): Unit = {
    this.taskMemoryManager = taskMemoryManager
  }

  def runTask(context: TaskContext): T

  //位置感知调度
  def preferredLocations: Seq[TaskLocation] = Nil

  // Map output tracker epoch. Will be set by TaskSetManager.
  /**
   *  epoch版本号‌：每个Task持有当前MapOutputTracker的epoch值,用于Shuffle容错设计,与MapOutputTrackerMaster的epoch机制配合,解决Executor宕机导致的shuffle文件失效问题
   *  失效检测‌：当Executor宕机时，Driver端的MapOutputTrackerMaster会增加全局epoch
   *  校验机制‌：ReduceTask获取shuffle数据前会校验epoch是否匹配
   *
   *  故障场景处理示例：
   *   时序说明：
   *   1. Stage1: 在executor1/executor2上运行ShuffleMapTask
   *      - executor1生成epoch=5的map输出
   *      - executor2生成epoch=5的map输出
   *
   *   2. executor2突然宕机
   *     - Driver检测到心跳丢失，调用：
   *         mapOutputTrackerMaster.incrementEpoch() // epoch增至6
   *     - 重新调度Stage1在executor3上运行
   *
   *   3. Stage2的ReduceTask开始执行：
   *      - 从executor1获取epoch=5的数据（有效）
   *      - 尝试从executor2获取数据时发现：
   *        - 原始epoch=5 ≠ 当前tracker epoch=6
   *        - 抛出FetchFailedException触发重试
   *
   */
  var epoch: Long = -1

  // Task context, to be initialized in run().
  @transient var context: TaskContext = _

  // The actual Thread on which the task is running, if any. Initialized in run().

  /**
   * @volatile保证多线程可见性
   */
  @volatile @transient private var taskThread: Thread = _

  // If non-null, this task has been killed and the reason is as specified. This is used in case
  // context is not yet initialized when kill() is invoked.
  @volatile @transient private var _reasonIfKilled: String = null  //记录被杀死的原因

  /**
   * 性能监控,记录：纳秒级反序列化耗时统计
   */
  protected var _executorDeserializeTimeNs: Long = 0

  /**
   * 性能监控,记录：精确测量任务反序列化阶段消耗的CPU时间
   */
  protected var _executorDeserializeCpuTime: Long = 0

  /**
   * If defined, this task has been killed and this option contains the reason.
   */
  def reasonIfKilled: Option[String] = Option(_reasonIfKilled)

  /**
   * Returns the amount of time spent deserializing the RDD and function to be run.
   */
  def executorDeserializeTimeNs: Long = _executorDeserializeTimeNs
  def executorDeserializeCpuTime: Long = _executorDeserializeCpuTime

  /**
   * Collect the latest values of accumulators used in this task. If the task failed,
   * filter out the accumulators whose values should not be included on failures.
   */

  /**
   * 该方法是Spark任务执行过程中‌累加器(Accumulator)结果收集机制‌的核心实现，主要解决任务失败时的指标统计问题
   *
   */
  def collectAccumulatorUpdates(taskFailed: Boolean = false): Seq[AccumulatorV2[_, _]] = {
    if (context != null) { // 有任务上下文时，内部累加器（（如executorRunTime等）） ++ 外部累加器 （如SQLMetrics）
      // Note: internal accumulators representing task metrics always count failed values
      context.taskMetrics.nonZeroInternalAccums() ++
        // zero value external accumulators may still be useful, e.g. SQLMetrics, we should not
        // filter them out.
      context.taskMetrics.externalAccums.filter(a => !taskFailed || a.countFailedValues)
    } else {
      // 无上下文时返回空序列
      Seq.empty
    }
  }

  /**
   * Kills a task by setting the interrupted flag to true. This relies on the upper level Spark
   * code and user code to properly handle the flag. This function should be idempotent so it can
   * be called multiple times.
   * If interruptThread is true, we will also call Thread.interrupt() on the Task's executor thread.
   */

  /**
   * ‌任务生命周期控制
   */
  def kill(interruptThread: Boolean, reason: String): Unit = {
    require(reason != null)
    _reasonIfKilled = reason
    if (context != null) {
      context.markInterrupted(reason)
    }
    if (interruptThread && taskThread != null) {
      taskThread.interrupt()
    }
  }
}
