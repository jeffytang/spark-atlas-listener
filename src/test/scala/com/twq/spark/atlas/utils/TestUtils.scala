package com.twq.spark.atlas.utils

import java.net.URI

import com.twq.spark.atlas.SACAtlasReferenceable
import org.apache.atlas.model.instance.AtlasObjectId
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogStorageFormat, CatalogTable, CatalogTableType}
import org.apache.spark.sql.types.StructType

object TestUtils {
  def createDB(name: String, location: String): CatalogDatabase = {
    CatalogDatabase(name, "", new URI(location), Map.empty)
  }

  def createStorageFormat(
                           locationUri: Option[URI] = None,
                           inputFormat: Option[String] = None,
                           outputFormat: Option[String] = None,
                           serd: Option[String] = None,
                           compressed: Boolean = false,
                           properties: Map[String, String] = Map.empty): CatalogStorageFormat = {
    CatalogStorageFormat(locationUri, inputFormat, outputFormat, serd, compressed, properties)
  }

  def createTable(
                   db: String,
                   table: String,
                   schema: StructType,
                   storage: CatalogStorageFormat,
                   isHiveTable: Boolean = false): CatalogTable = {
    CatalogTable(
      TableIdentifier(table, Some(db)),
      CatalogTableType.MANAGED,
      storage,
      schema,
      provider = if (isHiveTable) Some("hive") else None,
      bucketSpec = None,
      owner = SparkUtils.currUser())
  }

  def assertSubsetOf[T](set: Set[T], subset: Set[T]): Unit = {
    assert(subset.subsetOf(set), s"$subset is not a subset of $set")
  }

  def findEntity(
                  entities: Seq[SACAtlasReferenceable],
                  objId: AtlasObjectId): Option[SACAtlasReferenceable] = {
    entities.find(p => p.asObjectId == objId)
  }

  def findEntities(
                    entities: Seq[SACAtlasReferenceable],
                    objIds: Seq[AtlasObjectId]): Seq[SACAtlasReferenceable] = {
    entities.filter(p => objIds.contains(p.asObjectId))
  }
}
