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

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.util.CallSite

/**
 * ResultStages apply a function on some partitions of an RDD to compute the result of an action.
 * The ResultStage object captures the function to execute, `func`, which will be applied to each
 * partition, and the set of partition IDs, `partitions`. Some stages may not run on all partitions
 * of the RDD, for actions like first() and lookup().
 */

/**
 * ResultStage 核心设计目标:
 * 1.Action 的物理执行单元‌
 *   1)专为触发 Action（如 count(), collect()）而设计，是 DAG 调度的最终阶段
 *   2)与 ShuffleMapStage 形成对比：后者为 Shuffle 写数据，前者为 Action 生成最终结果
 *
 * 2.分区级计算控制‌
 *   1)支持部分分区计算（如 first() 只需计算第一个分区），比如 rdd.take(3) 或 部分失败的分区重试
 *   2)通过 partitions: Array[Int] 指定实际需计算的分区 ID 集合
 *
 * 示例场景:
 * rdd.filter(...).take(5)：
 *
 * 1.首先生成 func为take逻辑的 ResultStage：
 *   1)func: 取前 5 条结果的逻辑
 *   2)partitions: 可能只需计算前 2 个分区（若每个分区数据量足够）
 * 2.执行计算时：
 *   1)findMissingPartitions 返回待计算的分区 ID（如 [0,1]）
 *   2)每个 Task 应用 func 到对应分区数据
 * 通过这种设计，Spark 实现了高效、灵活的 Action 执行机制，同时支持细粒度的容错与资源控制。
 */
private[spark] class ResultStage(
    id: Int,
    rdd: RDD[_],
    val func: (TaskContext, Iterator[_]) => _, //对每个分区数据执行的计算函数（如 count 的累加逻辑）
    val partitions: Array[Int],     //实际需计算的分区ID集合
    parents: List[Stage],
    firstJobId: Int,
    callSite: CallSite,
    resourceProfileId: Int)
  extends Stage(id, rdd, partitions.length, parents, firstJobId, callSite, resourceProfileId) {

  /**
   * The active job for this result stage. Will be empty if the job has already finished
   * (e.g., because the job was cancelled).
   */
  private[this] var _activeJob: Option[ActiveJob] = None  //当前关联的 ActiveJob 引用,（在提交Job时动态绑定），这种设计允许 Stage 被多个 Job 复用

  def activeJob: Option[ActiveJob] = _activeJob

  def setActiveJob(job: ActiveJob): Unit = {
    _activeJob = Option(job)
  }

  def removeActiveJob(): Unit = {
    _activeJob = None
  }

  /**
   * Returns the sequence of partition ids that are missing (i.e. needs to be computed).
   *
   * This can only be called when there is an active job.
   */
  override def findMissingPartitions(): Seq[Int] = {
    val job = activeJob.get
    (0 until job.numPartitions).filter(id => !job.finished(id)) //动态确定待计算的分区,必须在 activeJob 存在时调用, 只需查看activeJob的已完成分区情况就行，失败时仅重新计算缺失分区
  }

  override def toString: String = "ResultStage " + id
}
