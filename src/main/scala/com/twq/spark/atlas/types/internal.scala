package com.twq.spark.atlas.types

import java.util.Date

import com.twq.spark.atlas.{SACAtlasEntityWithDependencies, SACAtlasReferenceable}
import com.twq.spark.atlas.utils.{Logging, SparkUtils}
import org.apache.atlas.AtlasConstants
import org.apache.atlas.model.instance.AtlasEntity
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogStorageFormat, CatalogTable}

import scala.collection.JavaConverters._

object internal extends Logging {

  def sparkDbToEntity(
                       dbDefinition: CatalogDatabase,
                       cluster: String,
                       owner: String): SACAtlasEntityWithDependencies = {
    val dbEntity = new AtlasEntity(metadata.DB_TYPE_STRING)

    dbEntity.setAttribute(
      "qualifiedName", sparkDbUniqueAttribute(dbDefinition.name))
    dbEntity.setAttribute(AtlasConstants.CLUSTER_NAME_ATTRIBUTE, cluster)
    dbEntity.setAttribute("name", dbDefinition.name)
    dbEntity.setAttribute("description", dbDefinition.description)
    dbEntity.setAttribute("location", dbDefinition.locationUri.toString)
    dbEntity.setAttribute("parameters", dbDefinition.properties.asJava)
    dbEntity.setAttribute("owner", owner)
    dbEntity.setAttribute("ownerType", "USER")

    SACAtlasEntityWithDependencies(dbEntity)
  }

  def sparkDbUniqueAttribute(db: String): String = SparkUtils.getUniqueQualifiedPrefix() + db

  def sparkTableToEntity(tblDefinition: CatalogTable,
                         clusterName: String,
                         mockDbDefinition: Option[CatalogDatabase] = None): SACAtlasEntityWithDependencies = {
    val tableDefinition = SparkUtils.getCatalogTableIfExistent(tblDefinition)
    val db = SparkUtils.getDatabaseName(tableDefinition)
    val table = SparkUtils.getTableName(tableDefinition)
    val dbDefinition = mockDbDefinition
      .getOrElse(SparkUtils.getExternalCatalog().getDatabase(db))

    val dbEntity = sparkDbToEntity(dbDefinition, clusterName, tableDefinition.owner)
    val sdEntity = sparkStorageFormatToEntity(tableDefinition.storage, db, table)

    val tblEntity = new AtlasEntity(metadata.TABLE_TYPE_STRING)
    tblEntity.setAttribute("qualifiedName",
      sparkTableUniqueAttribute(db, table))
    tblEntity.setAttribute("name", table)
    tblEntity.setAttribute("tableType", tableDefinition.tableType.name)
    tblEntity.setAttribute("schemaDesc", tableDefinition.schema.simpleString)
    tblEntity.setAttribute("provider", tableDefinition.provider.getOrElse(""))
    if (tableDefinition.tracksPartitionsInCatalog) {
      tblEntity.setAttribute("partitionProvider", "Catalog")
    }
    tblEntity.setAttribute("partitionColumnNames", tableDefinition.partitionColumnNames.asJava)
    tableDefinition.bucketSpec.foreach(
      b => tblEntity.setAttribute("bucketSpec", b.toLinkedHashMap.asJava))
    tblEntity.setAttribute("owner", tableDefinition.owner)
    tblEntity.setAttribute("ownerType", "USER")
    tblEntity.setAttribute("createTime", new Date(tableDefinition.createTime))
    tblEntity.setAttribute("parameters", tableDefinition.properties.asJava)
    tableDefinition.comment.foreach(tblEntity.setAttribute("comment", _))
    tblEntity.setAttribute("unsupportedFeatures", tableDefinition.unsupportedFeatures.asJava)

    // TODO
    //tblEntity.setRelationshipAttribute("db", dbEntity.asObjectId)
    //tblEntity.setRelationshipAttribute("sd", sdEntity.asObjectId)

    new SACAtlasEntityWithDependencies(tblEntity, Seq(dbEntity, sdEntity))
  }

  def sparkStorageFormatToEntity(
                                  storageFormat: CatalogStorageFormat,
                                  db: String,
                                  table: String): SACAtlasEntityWithDependencies = {
    val sdEntity = new AtlasEntity(metadata.STORAGEDESC_TYPE_STRING)

    sdEntity.setAttribute("qualifiedName",
      sparkStorageFormatUniqueAttribute(db, table))
    storageFormat.locationUri.foreach(uri => sdEntity.setAttribute("location", uri.toString))
    storageFormat.inputFormat.foreach(sdEntity.setAttribute("inputFormat", _))
    storageFormat.outputFormat.foreach(sdEntity.setAttribute("outputFormat", _))
    storageFormat.serde.foreach(sdEntity.setAttribute("serde", _))
    sdEntity.setAttribute("compressed", storageFormat.compressed)
    sdEntity.setAttribute("parameters", storageFormat.properties.asJava)

    SACAtlasEntityWithDependencies(sdEntity)
  }

  def sparkStorageFormatUniqueAttribute(db: String, table: String): String = {
    SparkUtils.getUniqueQualifiedPrefix() + s"$db.$table.storageFormat"
  }

  def sparkTableUniqueAttribute(db: String, table: String): String = {
    SparkUtils.getUniqueQualifiedPrefix() + s"$db.$table"
  }

  def etlProcessToEntity(
                          inputs: Seq[SACAtlasReferenceable],
                          outputs: Seq[SACAtlasReferenceable],
                          logMap: Map[String, String]): SACAtlasEntityWithDependencies = {
    val entity = new AtlasEntity(metadata.PROCESS_TYPE_STRING)

    val appId = SparkUtils.sparkSession.sparkContext.applicationId
    val appName = SparkUtils.sparkSession.sparkContext.appName match {
      case "Spark shell" => s"Spark Job + $appId"
      case default => default + s" $appId"
    }
    entity.setAttribute("qualifiedName", appId)
    entity.setAttribute("name", appName)
    entity.setAttribute("currUser", SparkUtils.currUser())

    val inputObjIds = inputs.map(_.asObjectId).asJava
    val outputObjIds = outputs.map(_.asObjectId).asJava

    entity.setAttribute("inputs", inputObjIds)  // Dataset and Model entity
    entity.setAttribute("outputs", outputObjIds)  // Dataset entity
    logMap.foreach { case (k, v) => entity.setAttribute(k, v)}

    new SACAtlasEntityWithDependencies(entity, inputs ++ outputs)
  }
}
