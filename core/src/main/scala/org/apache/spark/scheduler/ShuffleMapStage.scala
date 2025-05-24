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

import org.apache.spark.{MapOutputTrackerMaster, ShuffleDependency}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.CallSite

/**
 * ShuffleMapStages are intermediate stages in the execution DAG that produce data for a shuffle.
 * They occur right before each shuffle operation, and might contain multiple pipelined operations
 * before that (e.g. map and filter). When executed, they save map output files that can later be
 * fetched by reduce tasks. The `shuffleDep` field describes the shuffle each stage is part of,
 * and variables like `outputLocs` and `numAvailableOutputs` track how many map outputs are ready.
 *
 * ShuffleMapStages can also be submitted independently as jobs with DAGScheduler.submitMapStage.
 * For such stages, the ActiveJobs that submitted them are tracked in `mapStageJobs`. Note that
 * there can be multiple ActiveJobs trying to compute the same shuffle map stage.
 */

/**
 * ShuffleMapStage 用于生成并持久化 Map 端输出文件，供下游 Stage 拉取（通过 BlockManager 存储）
 *
 * 其与ResultStage不同：
 * 1.执行目标不同
 *   1）ResultStage：计算 Action 结果
 *   2）ShuffleMapStage：生成 Shuffle 中间数据
 * 2.‌分区状态追踪
 *   1）ShuffleMapStage 依赖 MapOutputTracker 全局协调，保证数据一致性
 *   2）ResultStage 直接通过 ActiveJob 本地判断
 * 3.容错粒度
 *   1）ShuffleMapStage 需处理 Executor 失效导致的‌输出丢失
 *   2）ResultStage 仅需重新计算未完成的分区
 *
 *Shuffle 数据流协同
 * 1.Map 端输出写入
 *   1）Task 运行时调用 ShuffleWriter（如 SortShuffleWriter）
 *   2）生成 data 和 index 文件（记录分区偏移量）
 * 2.Reduce 端数据获取
 *   1）通过 MapOutputTracker 定位文件位置
 *   2）使用 BlockStoreShuffleReader 拉取数据
 * 3.关键状态同步
 *   // Task 成功后更新状态
 *   mapOutputTrackerMaster.registerMapOutputs( shuffleId, mapStatuses  // 包含每个分块的存储位置 )
 *
 * 具体场景示例：
 * 假设执行 rdd.groupByKey().count()：
 * 1.Stage 划分‌：
 *  1)ShuffleMapStage 0‌：执行 groupByKey 的 Map 端（生成 Hash 分区数据）
 *  2)ResultStage 1‌：执行 count Action
 *2.‌执行流程‌：
 *  1)ShuffleMapStage 0 的 Task 将数据按 Key 哈希写入磁盘
 *  2)MapOutputTracker 记录分区文件位置
 *  3)当 isAvailable == true 时，触发 ResultStage 1 计算
 *
 *  通过这种设计，Spark 实现了高效的 Shuffle 数据生产与调度，同时支持细粒度的容错和资源控制。
 */
private[spark] class ShuffleMapStage(
    id: Int,
    rdd: RDD[_],
    numTasks: Int,
    parents: List[Stage],
    firstJobId: Int,
    callSite: CallSite,
    val shuffleDep: ShuffleDependency[_, _, _],  // 描述 Shuffle 依赖关系（内部携带 Partitioner、Serializer 、keyOrdering、聚合计算等）
    mapOutputTrackerMaster: MapOutputTrackerMaster, //全局管理 Shuffle 输出位置（Driver 端）
    resourceProfileId: Int)  //通过 resourceProfileId 支持 Shuffle 任务的独立资源配置
  extends Stage(id, rdd, numTasks, parents, firstJobId, callSite, resourceProfileId) {

  private[this] var _mapStageJobs: List[ActiveJob] = Nil

  /**
   * Partitions that either haven't yet been computed, or that were computed on an executor
   * that has since been lost, so should be re-computed.  This variable is used by the
   * DAGScheduler to determine when a stage has completed. Task successes in both the active
   * attempt for the stage or in earlier attempts for this stage can cause partition ids to get
   * removed from pendingPartitions. As a result, this variable may be inconsistent with the pending
   * tasks in the TaskSetManager for the active attempt for the stage (the partitions stored here
   * will always be a subset of the partitions that the TaskSetManager thinks are pending).
   */
  val pendingPartitions = new HashSet[Int]  //待计算或需重算的分区 ID（容错关键），用于 ‌跟踪当前 Stage 尝试（attempt）中尚未完成计算的分区‌。

  override def toString: String = "ShuffleMapStage " + id

  /**
   * Returns the list of active jobs,
   * i.e. map-stage jobs that were submitted to execute this stage independently (if any).
   */
  def mapStageJobs: Seq[ActiveJob] = _mapStageJobs  //关联的独立 MapStage 任务列表

  /** Adds the job to the active job list. */
  def addActiveJob(job: ActiveJob): Unit = {
    _mapStageJobs = job :: _mapStageJobs  //// 注册关联 Job,使用‌不可变列表‌（List）实现线程安全的动态维护
  }

  /** Removes the job from the active job list. */
  def removeActiveJob(job: ActiveJob): Unit = {
    _mapStageJobs = _mapStageJobs.filter(_ != job)  //Job 完成/失败时调用 removeActiveJob
  }

  /**
   * Number of partitions that have shuffle outputs.
   * When this reaches [[numPartitions]], this map stage is ready.
   */
  def numAvailableOutputs: Int = mapOutputTrackerMaster.getNumAvailableOutputs(shuffleDep.shuffleId)  //底层通过 MapOutputTrackerMaster 查询 Shuffle 输出状态

  /**
   * Returns true if the map stage is ready, i.e. all partitions have shuffle outputs.
   */
  def isAvailable: Boolean = numAvailableOutputs == numPartitions

  /** Returns the sequence of partition ids that are missing (i.e. needs to be computed). */

  /**
   * 动态确定需重新计算的分区,优先从 MapOutputTracker 获取缺失分区（避免全量扫描）,若MapOutputTracker中未注册（如首次执行），默认全部分区待计算
   * 需要注意的是， ShuffleMapStage如果分区并未注册mapOutputTrackerMaster，那么其下所有的Stage都不会被提交，直到注册为止
   */
  override def findMissingPartitions(): Seq[Int] = {
    mapOutputTrackerMaster
      .findMissingPartitions(shuffleDep.shuffleId)
      .getOrElse(0 until numPartitions)
  }
}
