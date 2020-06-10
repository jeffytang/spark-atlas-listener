package com.twq.spark.atlas.sql

import com.twq.spark.atlas.{AtlasClientConf, SACAtlasReferenceable}
import com.twq.spark.atlas.types.{AtlasEntityUtils, external, internal}
import com.twq.spark.atlas.utils.{Logging, SparkUtils}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, View}
import org.apache.spark.sql.execution.{FileRelation, SparkPlan}
import org.apache.spark.sql.execution.command.LoadDataCommand
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hive.execution.InsertIntoHiveTable

object CommandsHarvester extends AtlasEntityUtils with Logging{
  override val conf: AtlasClientConf = new AtlasClientConf

  object LoadDataHarvester extends Harvester[LoadDataCommand] {
    override def harvest(
                          node: LoadDataCommand,
                          qd: QueryDetail): Seq[SACAtlasReferenceable] = {
      val inputsEntities = Seq(external.pathToEntity(node.path))
      val outputEntities = Seq(prepareEntity(node.table))

      makeProcessEntities(inputsEntities, outputEntities, qd)
    }
  }

  object InsertIntoHiveTableHarvester extends Harvester[InsertIntoHiveTable] {
    override def harvest(node: InsertIntoHiveTable, qd: QueryDetail): Seq[SACAtlasReferenceable] = {
      // source tables entities
      val inputEntities = discoverInputsEntities(node.query, qd.qe.executedPlan)

      // new table entity
      val outputEntities = Seq(tableToEntity(node.table))

      makeProcessEntities(inputEntities, outputEntities, qd)
    }
  }

  private def makeProcessEntities(
                                   inputsEntities: Seq[SACAtlasReferenceable],
                                   outputEntities: Seq[SACAtlasReferenceable],
                                   qd: QueryDetail): Seq[SACAtlasReferenceable] = {
    val logMap = getPlanInfo(qd)

    val cleanedOutput = cleanOutput(inputsEntities, outputEntities)

    Seq(internal.etlProcessToEntity(inputsEntities, cleanedOutput, logMap))
  }

  private def discoverInputsEntities(
                                      plan: LogicalPlan,
                                      executedPlan: SparkPlan): Seq[SACAtlasReferenceable] = {
    val tChildren = plan.collectLeaves()
    tChildren.flatMap {
      case r: HiveTableRelation => Seq(tableToEntity(r.tableMeta))
      case v: View => Seq(tableToEntity(v.desc))
      case LogicalRelation(fileRelation: FileRelation, _, catalogTable, _) =>
        catalogTable.map(tbl => Seq(tableToEntity(tbl))).getOrElse(
          fileRelation.inputFiles.flatMap(file => Seq(external.pathToEntity(file))).toSeq
        )
      case e =>
        logWarn(s"Missing unknown leaf node: $e")
        Seq.empty
    }
  }

  def prepareEntity(tableIdentifier: TableIdentifier): SACAtlasReferenceable = {
    val tableName = SparkUtils.getTableName(tableIdentifier)
    val dbName = SparkUtils.getDatabaseName(tableIdentifier)
    val tableDef = SparkUtils.getExternalCatalog().getTable(dbName, tableName)
    tableToEntity(tableDef)
  }

  private def getPlanInfo(qd: QueryDetail): Map[String, String] = {
    Map("executionId" -> qd.executionId.toString,
      "remoteUser" -> SparkUtils.currSessionUser(qd.qe),
      "details" -> qd.qe.toString(),
      "sparkPlanDescription" -> qd.qe.sparkPlan.toString())
  }


}
