package com.twq.spark.atlas.sql

import com.twq.spark.atlas.types.{AtlasEntityUtils, external}
import com.twq.spark.atlas.utils.SparkUtils
import com.twq.spark.atlas.{AbstractEventProcessor, AtlasClient, AtlasClientConf, AtlasEntityReadHelper}
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CreateDatabasePreEvent, CreateTableEvent, CreateTablePreEvent, DropDatabaseEvent, DropDatabasePreEvent, ExternalCatalogEvent}

import scala.collection.mutable

class SparkCatalogEventProcessor(val atlasClient: AtlasClient,
                                 val conf: AtlasClientConf)
  extends AbstractEventProcessor[ExternalCatalogEvent] with AtlasEntityUtils {

  private val cachedObject = new mutable.WeakHashMap[String, Object]

  override protected def process(e: ExternalCatalogEvent): Unit = {
    if (SparkUtils.usingRemoteMetastoreService()) {
      // SAC will not handle any DDL events when remote HMS is used:
      // Hive hook will take care of all DDL events in Hive Metastore Service.
      // No-op here.
      return
    }

    e match {
      case CreateDatabasePreEvent(_) => // No-op

      case CreateDatabasePreEvent(db) =>
        val dbDefinition = SparkUtils.getExternalCatalog().getDatabase(db)
        val entity = sparkDbToEntity(dbDefinition)
        atlasClient.createEntitiesWithDependencies(entity)
        logDebug(s"Created db entity $db")

      case DropDatabasePreEvent(db) =>
        try {
          cachedObject.put(sparkDbUniqueAttribute(db),
            SparkUtils.getExternalCatalog().getDatabase(db))
        } catch {
          case _: NoSuchDatabaseException =>
            logDebug(s"Spark already deleted the database: $db")
        }

      case DropDatabaseEvent(db) =>
        atlasClient.deleteEntityWithUniqueAttr(sparkDbType, sparkDbUniqueAttribute(db))

        cachedObject.remove(sparkDbUniqueAttribute(db)).foreach { o =>
          val dbDef = o.asInstanceOf[CatalogDatabase]
          val path = dbDef.locationUri.toString
          val pathEntity = external.pathToEntity(path)

          atlasClient.deleteEntityWithUniqueAttr(pathEntity.entity.getTypeName,
            AtlasEntityReadHelper.getQualifiedName(pathEntity.entity))
        }

        logDebug(s"Deleted db entity $db")

      case CreateTablePreEvent(_, _) => // No-op

      case CreateTableEvent(db, table) =>
        val tableDefinition = SparkUtils.getExternalCatalog().getTable(db, table)
        val tableEntity = sparkTableToEntity(tableDefinition)
        atlasClient.createEntitiesWithDependencies(tableEntity)
        logDebug(s"Created table entity $table without columns")

      case f =>
        logDebug(s"Drop unknown event $f")
    }
  }

}
