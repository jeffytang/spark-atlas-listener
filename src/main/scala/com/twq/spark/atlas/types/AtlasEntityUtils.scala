package com.twq.spark.atlas.types

import com.twq.spark.atlas.{AtlasClientConf, SACAtlasEntityWithDependencies, SACAtlasReferenceable}
import com.twq.spark.atlas.utils.{Logging, SparkUtils}
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogTable}

trait AtlasEntityUtils extends Logging {

  def conf: AtlasClientConf

  def clusterName: String = conf.get(AtlasClientConf.CLUSTER_NAME)

  def sparkDbType: String = metadata.DB_TYPE_STRING

  def sparkDbToEntity(dbDefinition: CatalogDatabase): SACAtlasEntityWithDependencies = {
    internal.sparkDbToEntity(dbDefinition, clusterName, SparkUtils.currUser())
  }

  def sparkDbUniqueAttribute(db: String): String = {
    internal.sparkDbUniqueAttribute(db)
  }

  def sparkTableToEntity(
                          tableDefinition: CatalogTable,
                          mockDbDefinition: Option[CatalogDatabase] = None): SACAtlasReferenceable = {
    internal.sparkTableToEntity(tableDefinition, clusterName, mockDbDefinition)
  }

  def tableToEntity(
                     tableDefinition: CatalogTable,
                     mockDbDefinition: Option[CatalogDatabase] = None): SACAtlasReferenceable = {
    if (SparkUtils.usingRemoteMetastoreService()) {
      external.hiveTableToReference(tableDefinition, clusterName, mockDbDefinition)
    } else {
      internal.sparkTableToEntity(tableDefinition, clusterName, mockDbDefinition)
    }
  }

  // If there is cycle, return empty output entity list
  def cleanOutput(
                   inputs: Seq[SACAtlasReferenceable],
                   outputs: Seq[SACAtlasReferenceable]): List[SACAtlasReferenceable] = {
    val qualifiedNames = inputs.map(_.qualifiedName)
    val isCycle = outputs.exists(x => qualifiedNames.contains(x.qualifiedName))
    if (isCycle) {
      logWarn("Detected cycle - same entity observed to both input and output. " +
        "Discarding output entities as Atlas doesn't support cycle.")
      List.empty
    } else {
      outputs.toList
    }
  }

}
