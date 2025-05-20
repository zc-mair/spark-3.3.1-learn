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

import scala.collection.mutable.HashSet

import org.apache.spark.executor.TaskMetrics
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{DeterministicLevel, RDD}
import org.apache.spark.util.CallSite

/**
 * A stage is a set of parallel tasks all computing the same function that need to run as part
 * of a Spark job, where all the tasks have the same shuffle dependencies. Each DAG of tasks run
 * by the scheduler is split up into stages at the boundaries where shuffle occurs, and then the
 * DAGScheduler runs these stages in topological order.
 *
 * Each Stage can either be a shuffle map stage, in which case its tasks' results are input for
 * other stage(s), or a result stage, in which case its tasks directly compute a Spark action
 * (e.g. count(), save(), etc) by running a function on an RDD. For shuffle map stages, we also
 * track the nodes that each output partition is on.
 *
 * Each Stage also has a firstJobId, identifying the job that first submitted the stage.  When FIFO
 * scheduling is used, this allows Stages from earlier jobs to be computed first or recovered
 * faster on failure.
 *
 * Finally, a single stage can be re-executed in multiple attempts due to fault recovery. In that
 * case, the Stage object will track multiple StageInfo objects to pass to listeners or the web UI.
 * The latest one will be accessible through latestInfo.
 *
 * @param id Unique stage ID
 * @param rdd RDD that this stage runs on: for a shuffle map stage, it's the RDD we run map tasks
 *   on, while for a result stage, it's the target RDD that we ran an action on
 * @param numTasks Total number of tasks in stage; result stages in particular may not need to
 *   compute all partitions, e.g. for first(), lookup(), and take().
 * @param parents List of stages that this stage depends on (through shuffle dependencies).
 * @param firstJobId ID of the first job this stage was part of, for FIFO scheduling.
 * @param callSite Location in the user program associated with this stage: either where the target
 *   RDD was created, for a shuffle map stage, or where the action for a result stage was called.
 */

/**
 * DAGScheduler创建Stage实例关键步骤：
 * 1）调用makeNewStageAttempt初始化运行信息
 * 2）通过findMissingPartitions确定待计算分区
 * 3）任务失败时更新failedAttemptIds
 * 4）成功或重试后通过latestInfo获取最终状态
 *
 *
 * Stage 的不可变性‌：
 * 1.核心元数据（如 id, rdd, parents）一经创建不可修改，确保调度逻辑的一致性。
 * 2.状态与元数据分离‌：
 *  1)不变部分：id, rdd, parents 等
 *  2)可变部分：jobIds, failedAttemptIds, _latestInfo 等
 *  保证并发安全性的同时，支持动态更新。
 * 3.资源感知调度‌：
 * 通过 resourceProfileId 整合动态资源分配（DRAM），为异构计算提供基础。
 *
 * 示例：
 * rdd = sc.textFile(...)
 * .filter(...)          # Narrow 依赖 → 同一 Stage
 * .map(...)
 * .repartition(100)     # Shuffle 依赖 → 新 Stage
 * .count()              # Action → 触发 ResultStage
 *
 * 将生成两个 Stage：
 *
 * 1.ShuffleMapStage‌（ID=0）：包含 repartition -> map->filter->textFile等操作
 *    parents: 空列表
 *    rdd: MapPartitionsRDD（对应 repartition）
 * 2.ResultStage‌（ID=1）：包含 count
 *    parents: [ShuffleMapStage(0)]
 *    rdd: ShuffledRDD（对应 count 的输入）
 */
private[scheduler] abstract class Stage(
    val id: Int,               //stage的全局唯一标识
    val rdd: RDD[_],           //当前Stage对应的 RDD（触发计算的源头），对于 ResultStage，是最后一个 RDD（对应 Action 操作），对于 ShuffleMapStage，是 Shuffle 依赖的父 RD
    val numTasks: Int,         //Stage 的分区数量，由用户指定要计算的分区数量，隶属于自关联 RDD 的分区数,每个分区对应一个 Task（numTasks = numPartitions）
    val parents: List[Stage],  //直接父 Stage 列表（构成 DAG 的关键）
    val firstJobId: Int,       //记录首次提交该 Stage 的 Job ID, 一个 Stage 可能被多个 Job 共享，如缓存 RDD 被复用、 一个sql下的子查询被上层不同Sql反复复用，这个时候需要记录首次创建的JobId,即能进行溯源，也能当此Stage出现数据丢失的时候，知道该怎么处理
    val callSite: CallSite,    //记录用户代码中触发 Stage 的调用位置
    val resourceProfileId: Int) //当前Stage所需的resourceProfile 配置，若未指定，取默认的default配置
  extends Logging {

  val numPartitions = rdd.partitions.length  //自关联 RDD 的分区数, 注意：提交的Stage不一定计算全部分区，故用numTasks记录要计算的分区数

  /** Set of jobs that this stage belongs to. */
  val jobIds = new HashSet[Int]    //存储所有依赖该 Stage 的 Job ID， 动态性‌：随着 Job 提交/完成 动态增减

  /** The ID to use for the next new attempt for this stage. */
  private var nextAttemptId: Int = 0 //生成下一次 Stage 重试的 Attempt ID, 更新时机‌：每次调用 makeNewStageAttempt() 时自增

  val name: String = callSite.shortForm //基于 callSite 生成的人类可读标识,短格式（如 count at MyApp.scala:42）
  val details: String = callSite.longForm // 长格式（包含完整调用栈）

  /**
   * Pointer to the [[StageInfo]] object for the most recent attempt. This needs to be initialized
   * here, before any attempts have actually been created, because the DAGScheduler uses this
   * StageInfo to tell SparkListeners when a job starts (which happens before any stage attempts
   * have been created).
   */

  /**
   *  存储最新的 Stage 运行时信息（供监控与 UI 展示）,包含内容‌：
   *   1）TaskMetrics（任务指标，如数据读取量、Shuffle 数据量）
   *   2）taskLocalityPreferences（任务本地性偏好）
   *   3）numPartitionsToCompute（待计算分区数）
   */
  private var _latestInfo: StageInfo =
    StageInfo.fromStage(this, nextAttemptId, resourceProfileId = resourceProfileId)


  /**
   * Set of stage attempt IDs that have failed. We keep track of these failures in order to avoid
   * endless retries if a stage keeps failing.
   * We keep track of each attempt ID that has failed to avoid recording duplicate failures if
   * multiple tasks from the same stage attempt fail (SPARK-5945).
   */
  val failedAttemptIds = new HashSet[Int]  //记录所有失败的 Attempt ID（防止无限重试）, SPARK-5945 优化‌：避免重复记录同一 Attempt 的多次 Task 失败

  private[scheduler] def clearFailures() : Unit = {
    failedAttemptIds.clear()
  }

  /** Creates a new attempt for this stage by creating a new StageInfo with a new attempt ID. */

  /**
   * 创建新 Stage 尝试（Attempt），尝试有隔离‌性：每次makeNewStageAttempt都会生成新的StageInfo和TaskMetrics
   * 1.生成新的 StageInfo 对象
   * 2.注册新的 TaskMetrics（与 Attempt 绑定）
   * 3.更新 nextAttemptId
   */
  def makeNewStageAttempt(
      numPartitionsToCompute: Int,
      taskLocalityPreferences: Seq[Seq[TaskLocation]] = Seq.empty): Unit = {
    val metrics = new TaskMetrics
    metrics.register(rdd.sparkContext)
    _latestInfo = StageInfo.fromStage(
      this, nextAttemptId, Some(numPartitionsToCompute), metrics, taskLocalityPreferences,
      resourceProfileId = resourceProfileId)
    nextAttemptId += 1
  }

  /** Forward the nextAttemptId if skipped and get visited for the first time. */
  def increaseAttemptIdOnFirstSkip(): Unit = {  //第一此提交被跳过时，ID自增
    if (nextAttemptId == 0) {
      nextAttemptId = 1
    }
  }

  /** Returns the StageInfo for the most recent attempt for this stage. */
  def latestInfo: StageInfo = _latestInfo

  override final def hashCode(): Int = id

  override final def equals(other: Any): Boolean = other match {
    case stage: Stage => stage != null && stage.id == id
    case _ => false
  }

  /** Returns the sequence of partition ids that are missing (i.e. needs to be computed). */

  /**
   * ‌作用‌（需子类实现）：找出未完成计算的分区 ID
   *
   * ‌实现差异‌：
   *  1.ResultStage：直接检查分区是否计算完成
   *  2.ShuffleMapStage：检查 Shuffle 数据是否生成
   */
  def findMissingPartitions(): Seq[Int]  //

  /**
   * ‌确定性判断:用于处理非确定性RDD的容错场景
   */
  def isIndeterminate: Boolean = {
    rdd.outputDeterministicLevel == DeterministicLevel.INDETERMINATE
  }
}
