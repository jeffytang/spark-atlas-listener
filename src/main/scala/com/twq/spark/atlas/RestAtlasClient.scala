package com.twq.spark.atlas
import com.sun.jersey.core.util.MultivaluedMapImpl
import org.apache.atlas.AtlasClientV2
import org.apache.atlas.model.SearchFilter
import org.apache.atlas.model.instance.AtlasEntity
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo
import org.apache.atlas.model.typedef.AtlasTypesDef
import org.apache.atlas.utils.AuthenticationUtil

import scala.collection.JavaConverters._

class RestAtlasClient(atlasClientConf: AtlasClientConf) extends AtlasClient {

  private val client = {
    if (!AuthenticationUtil.isKerberosAuthenticationEnabled) {
      val basicAuth = Array(atlasClientConf.get(AtlasClientConf.CLIENT_USERNAME),
        atlasClientConf.get(AtlasClientConf.CLIENT_PASSWORD))
      new AtlasClientV2(getServerUrl(), basicAuth)
    } else {
      new AtlasClientV2(getServerUrl():_*)
    }
  }

  private def getServerUrl(): Array[String] = {

    atlasClientConf.getUrl(AtlasClientConf.ATLAS_REST_ENDPOINT.key) match {
      case a: java.util.ArrayList[_] => a.toArray().map(b => b.toString)
      case s: String => Array(s)
      case _: Throwable => throw new IllegalArgumentException(s"Fail to get atlas.rest.address")
    }
  }

  override def createAtlasTypeDefs(typeDefs: AtlasTypesDef): Unit = {
    client.createAtlasTypeDefs(typeDefs)
  }

  override def getAtlasTypeDefs(searchParams: MultivaluedMapImpl): AtlasTypesDef = {
    val searchFilter = new SearchFilter(searchParams)
    client.getAllTypeDefs(searchFilter);
  }

  override def updateAtlasTypeDefs(typeDefs: AtlasTypesDef): Unit = {
    client.updateAtlasTypeDefs(typeDefs)
  }

  override protected def doCreateEntities(entities: Seq[AtlasEntity]): Unit = {
    val entitesWithExtInfo = new AtlasEntitiesWithExtInfo()
    entities.foreach(entitesWithExtInfo.addEntity)
    val response = client.createEntities(entitesWithExtInfo)
    try {
      logInfo(s"Entities ${response.getCreatedEntities.asScala.map(_.getGuid).mkString(", ")} " +
        s"created")
    } catch {
      case _: Throwable => throw new IllegalStateException(s"Fail to get create entities")
    }
  }

  override protected def doDeleteEntityWithUniqueAttr(entityType: String, attribute: String): Unit = {
    client.deleteEntityByAttribute(entityType,
      Map(org.apache.atlas.AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME -> attribute).asJava)
  }

}
