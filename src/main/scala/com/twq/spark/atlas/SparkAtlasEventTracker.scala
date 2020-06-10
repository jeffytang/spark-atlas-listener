package com.twq.spark.atlas

import com.twq.spark.atlas.sql.{QueryDetail, SparkCatalogEventProcessor, SparkExecutionPlanProcessor}
import com.twq.spark.atlas.utils.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogEvent
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener

class SparkAtlasEventTracker(atlasClient: AtlasClient, atlasClientConf: AtlasClientConf)
    extends SparkListener with QueryExecutionListener with Logging {
  def this(atlasClientConf: AtlasClientConf) = {
    this(AtlasClient.atalsClient(atlasClientConf), atlasClientConf)
  }

  def this() {
    this(new AtlasClientConf)
  }

  // Processor to handle DDL related events
  private [atlas] val catalogEventTracker =
    new SparkCatalogEventProcessor(atlasClient, atlasClientConf)
  catalogEventTracker.startThread()

  // Processor to handle DML related events
  private val executionPlanTracker =
    new SparkExecutionPlanProcessor(atlasClient, atlasClientConf)
  executionPlanTracker.startThread()

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case e: ExternalCatalogEvent => catalogEventTracker.pushEvent(e)
      case _ => // Ignore other events
    }
  }

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
    if (qe.logical.isStreaming) {
      // TODO
      // streaming query will be tracked via SparkAtlasStreamingQueryEventTracker
      return
    }

    val qd = QueryDetail.fromQueryExecutionListener(qe, durationNs)
    executionPlanTracker.pushEvent(qd)
  }

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {

  }
}
