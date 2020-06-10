package com.twq.spark.atlas.types

import java.io.File
import java.net.{URI, URL}

import com.twq.spark.atlas.{SACAtlasEntityReference, SACAtlasEntityWithDependencies, SACAtlasReferenceable}
import com.twq.spark.atlas.utils.SparkUtils
import org.apache.atlas.AtlasConstants
import org.apache.atlas.model.instance.{AtlasEntity, AtlasObjectId}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogTable}

object external {

  // ================ File system entities ======================
  val FS_PATH_TYPE_STRING = "fs_path"
  val HDFS_PATH_TYPE_STRING = "hdfs_path"

  def pathToEntity(path: String): SACAtlasEntityWithDependencies = {
    val uri = resolveURI(path)
    val fsPath = new Path(uri)
    if (uri.getScheme == "hdfs") {
      val entity = new AtlasEntity(HDFS_PATH_TYPE_STRING)
      entity.setAttribute("name",
        Path.getPathWithoutSchemeAndAuthority(fsPath).toString.toLowerCase)
      entity.setAttribute("path",
        Path.getPathWithoutSchemeAndAuthority(fsPath).toString.toLowerCase)
      entity.setAttribute("qualifiedName", uri.toString)
      entity.setAttribute(AtlasConstants.CLUSTER_NAME_ATTRIBUTE, uri.getAuthority)

      SACAtlasEntityWithDependencies(entity)
    } else {
      val entity = new AtlasEntity(FS_PATH_TYPE_STRING)
      entity.setAttribute("name",
        Path.getPathWithoutSchemeAndAuthority(fsPath).toString.toLowerCase)
      entity.setAttribute("path",
        Path.getPathWithoutSchemeAndAuthority(fsPath).toString.toLowerCase)
      entity.setAttribute("qualifiedName", uri.toString)

      SACAtlasEntityWithDependencies(entity)
    }
  }

  private def resolveURI(path: String): URI = {
    val uri = new URI(path)
    if (uri.getScheme != null) {
      return uri
    }

    val qUri = qualifiedPath(path).toUri

    // Path.makeQualified always converts the path as absolute path, but for file scheme
    // it provides different prefix.
    // (It provides prefix as "file" instead of "file://", which both are actually valid.)
    // Given we have been providing it as "file://", the logic below changes the scheme of
    // type "file" to "file://".
    if (qUri.getScheme == "file") {
      // make sure to handle if the path has a fragment (applies to yarn
      // distributed cache)
      if (qUri.getFragment() != null) {
        val absoluteURI = new File(qUri.getPath()).getAbsoluteFile().toURI()
        new URI(absoluteURI.getScheme(), absoluteURI.getHost(), absoluteURI.getPath(),
          uri.getFragment())
      } else {
        new File(path).getAbsoluteFile().toURI()
      }
    } else {
      qUri
    }
  }

  private def qualifiedPath(path: String): Path = {
    val p = new Path(path)
    val fs = p.getFileSystem(SparkUtils.sparkSession.sparkContext.hadoopConfiguration)
    p.makeQualified(fs.getUri, fs.getWorkingDirectory)
  }

  // ================== Hive Catalog entities =====================
  val HIVE_TABLE_TYPE_STRING = "hive_table"

  // scalastyle:off
  /**
    * This is based on the logic how Hive Hook defines qualifiedName for Hive DB (borrowed from Apache Atlas v1.1).
    * https://github.com/apache/atlas/blob/release-1.1.0-rc2/addons/hive-bridge/src/main/java/org/apache/atlas/hive/bridge/HiveMetaStoreBridge.java#L833-L841
    *
    * As we cannot guarantee same qualifiedName for temporary table, we just don't support
    * temporary table in SAC.
    */
  // scalastyle:on
  def hiveTableUniqueAttribute(
                                cluster: String,
                                db: String,
                                table: String): String = {
    s"${db.toLowerCase}.${table.toLowerCase}@$cluster"
  }

  def hiveTableToReference(
                            tblDefinition: CatalogTable,
                            cluster: String,
                            mockDbDefinition: Option[CatalogDatabase] = None): SACAtlasReferenceable = {
    val tableDefinition = SparkUtils.getCatalogTableIfExistent(tblDefinition)
    val db = SparkUtils.getDatabaseName(tableDefinition)
    val table = SparkUtils.getTableName(tableDefinition)
    hiveTableToReference(db, table, cluster)
  }

  def hiveTableToReference(
                            db: String,
                            table: String,
                            cluster: String): SACAtlasReferenceable = {
    val qualifiedName = hiveTableUniqueAttribute(cluster, db, table)
    SACAtlasEntityReference(
      new AtlasObjectId(HIVE_TABLE_TYPE_STRING, "qualifiedName", qualifiedName))
  }
}
