package com.twq.spark.atlas.sql

import java.util.UUID

import com.twq.spark.atlas.utils.{AtlasUtils, Logging}
import com.twq.spark.atlas.{AbstractEventProcessor, AtlasClient, AtlasClientConf, AtlasEntityCreationRequestHelper}
import org.apache.spark.sql.execution.command.{DataWritingCommandExec, ExecutedCommandExec, LoadDataCommand}
import org.apache.spark.sql.execution.datasources.v2.WriteToDataSourceV2Exec
import org.apache.spark.sql.execution.{LeafExecNode, QueryExecution, SparkPlan, UnionExec}
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable
import org.apache.spark.sql.streaming.SinkProgress

case class QueryDetail(
                        qe: QueryExecution,
                        executionId: Long,
                        query: Option[String] = None,
                        sink: Option[SinkProgress] = None,
                        queryId: Option[UUID] = None)

object QueryDetail {
  def fromQueryExecutionListener(qe: QueryExecution, durationNs: Long): QueryDetail = {
    QueryDetail(qe, AtlasUtils.issueExecutionId(), Option(SQLQuery.get()))
  }
}

class SparkExecutionPlanProcessor(
                                   private[atlas] val atlasClient: AtlasClient,
                                   val conf: AtlasClientConf)
  extends AbstractEventProcessor[QueryDetail] with Logging {

  val createReqHelper = new AtlasEntityCreationRequestHelper(atlasClient)

  override protected def process(qd: QueryDetail): Unit = {
    val outNodes: Seq[SparkPlan] = qd.qe.sparkPlan.collect {
      case p: UnionExec => p.children
      case p: DataWritingCommandExec => Seq(p)
      case p: WriteToDataSourceV2Exec => Seq(p)
      case p: LeafExecNode => Seq(p)
    }.flatten

    val entities = outNodes.flatMap {
      case r: ExecutedCommandExec =>
        r.cmd match {
          case c: LoadDataCommand =>
            // Case 1. LOAD DATA LOCAL INPATH (from local)
            // Case 2. LOAD DATA INPATH (from HDFS)
            logDebug(s"LOAD DATA [LOCAL] INPATH (${c.path}) ${c.table}")
            CommandsHarvester.LoadDataHarvester.harvest(c, qd)
          case _ =>
            Seq.empty
        }

      case r: DataWritingCommandExec =>
        r.cmd match {
          case c: InsertIntoHiveTable =>
            logDebug(s"INSERT INTO HIVE TABLE query ${qd.qe}")
            CommandsHarvester.InsertIntoHiveTableHarvester.harvest(c, qd)
          case _ =>
            Seq.empty
        }

      case _ =>
        Seq.empty
    }

    createReqHelper.requestCreation(entities, qd.queryId)
  }

}
