package com.twq.spark.atlas

import com.sun.jersey.core.util.MultivaluedMapImpl
import com.twq.spark.atlas.utils.SparkUtils
import org.apache.atlas.hook.AtlasHook
import org.apache.atlas.model.instance.{AtlasEntity, AtlasObjectId}
import org.apache.atlas.model.instance.AtlasEntity.AtlasEntitiesWithExtInfo
import org.apache.atlas.model.typedef.AtlasTypesDef
import org.apache.atlas.notification.hook.HookNotification
import org.apache.atlas.notification.hook.HookNotification.{EntityCreateRequestV2, EntityDeleteRequestV2}

import scala.collection.JavaConverters._

class KafkaAtlasClient(atlasClientConf: AtlasClientConf) extends AtlasHook with AtlasClient {
  override def createAtlasTypeDefs(typeDefs: AtlasTypesDef): Unit = {
    throw new UnsupportedOperationException("Kafka atlas client doesn't support create type defs")
  }

  override def getAtlasTypeDefs(searchParams: MultivaluedMapImpl): AtlasTypesDef = {
    throw new UnsupportedOperationException("Kafka atlas client doesn't support get type defs")
  }

  override def updateAtlasTypeDefs(typeDefs: AtlasTypesDef): Unit = {
    throw new UnsupportedOperationException("Kafka atlas client doesn't support update type defs")
  }

  override protected def doCreateEntities(entities: Seq[AtlasEntity]): Unit = {
    val entitiesWithExtInfo = new AtlasEntitiesWithExtInfo()
    entities.foreach(entitiesWithExtInfo.addEntity)
    val createRequest = new EntityCreateRequestV2(
      SparkUtils.currUser(), entitiesWithExtInfo): HookNotification.HookNotificationMessage

    notifyEntities(Seq(createRequest).asJava, SparkUtils.ugi())
  }

  override protected def doDeleteEntityWithUniqueAttr(entityType: String, attribute: String): Unit = {
    val deleteRequest = new EntityDeleteRequestV2(
      SparkUtils.currUser(),
      Seq(new AtlasObjectId(entityType,
        org.apache.atlas.AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME,
        attribute)).asJava
    ): HookNotification.HookNotificationMessage

    notifyEntities(Seq(deleteRequest).asJava, SparkUtils.ugi())
  }
}
