package com.twq.spark.atlas.sql

import java.io.File
import java.nio.file.Files

import com.sun.jersey.core.util.MultivaluedMapImpl
import com.twq.spark.atlas.utils.SparkUtils
import com.twq.spark.atlas.{AtlasClient, AtlasClientConf}
import org.apache.atlas.model.instance.AtlasEntity
import org.apache.atlas.model.typedef.AtlasTypesDef
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.{CreateDatabaseEvent, CreateDatabasePreEvent, DropDatabaseEvent, DropDatabasePreEvent}
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}
import org.scalatest.concurrent.Eventually._

import scala.collection.mutable
import scala.concurrent.duration._

class SparkCatalogEventProcessorTest extends FunSuite with Matchers with BeforeAndAfterAll {

  import com.twq.spark.atlas.utils.TestUtils._

  private var sparkSession: SparkSession = _
  private val atlasClientConf = new AtlasClientConf()

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    sparkSession = SparkSession.builder()
      .master("local")
      .config("spark.ui.enabled", "false")
      .config("spark.testing.memory", 471859200L)
      .getOrCreate()
  }

  override protected def afterAll(): Unit = {
    sparkSession.sessionState.catalog.reset()
    sparkSession.stop()
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    sparkSession = null

    FileUtils.deleteDirectory(new File("spark-warehouse"))

    super.afterAll()
  }

  test("correctly handle spark DB related events") {
    val processor = new SparkCatalogEventProcessor(
      new FirehoseAtlasClient(atlasClientConf), atlasClientConf)
    processor.startThread()

    var atlasClient: FirehoseAtlasClient = null
    eventually(timeout(30 seconds), interval(100 milliseconds)) {
      processor.atlasClient should not be (null)
      atlasClient = processor.atlasClient.asInstanceOf[FirehoseAtlasClient]
    }

    val tempPath = Files.createTempDirectory("db_")
    val dbDefinition = createDB("db1", tempPath.normalize().toUri.toString)
    SparkUtils.getExternalCatalog().createDatabase(dbDefinition, ignoreIfExists = false)
    processor.pushEvent(CreateDatabasePreEvent("db1"))
    processor.pushEvent(CreateDatabaseEvent("db1"))

    eventually(timeout(30 seconds), interval(100 milliseconds)) {
      assert(atlasClient.createEntityCall.size > 0)
      assert(atlasClient.createEntityCall(processor.sparkDbType) == 1)
    }

    // SAC-97: Spark delete the table before SAC receives the message.
    sparkSession.sessionState.catalog.dropDatabase("db1", ignoreIfNotExists = false, cascade = true)
    processor.pushEvent(DropDatabasePreEvent("db1"))
    processor.pushEvent(DropDatabaseEvent("db1"))
    eventually(timeout(30 seconds), interval(100 milliseconds)) {
      assert(atlasClient.deleteEntityCall.size > 0)
      assert(atlasClient.deleteEntityCall(processor.sparkDbType) == 1)
    }
  }
}

class FirehoseAtlasClient(conf: AtlasClientConf) extends AtlasClient {
  var createEntityCall = new mutable.HashMap[String, Int]
  var updateEntityCall = new mutable.HashMap[String, Int]
  var deleteEntityCall = new mutable.HashMap[String, Int]

  var processedEntity: AtlasEntity = _

  override def createAtlasTypeDefs(typeDefs: AtlasTypesDef): Unit = { }

  override def updateAtlasTypeDefs(typeDefs: AtlasTypesDef): Unit = { }

  override def getAtlasTypeDefs(searchParams: MultivaluedMapImpl): AtlasTypesDef = {
    new AtlasTypesDef()
  }

  override protected def doCreateEntities(entities: Seq[AtlasEntity]): Unit = {
    entities.foreach { e =>
      createEntityCall(e.getTypeName) =
        createEntityCall.getOrElseUpdate(e.getTypeName, 0) + 1
      processedEntity = e
    }
  }

  override protected def doDeleteEntityWithUniqueAttr(
                                                       entityType: String,
                                                       attribute: String): Unit = {
    deleteEntityCall(entityType) = deleteEntityCall.getOrElse(entityType, 0) + 1
  }

}
