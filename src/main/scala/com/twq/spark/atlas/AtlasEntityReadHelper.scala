package com.twq.spark.atlas

import org.apache.atlas.model.instance.{AtlasEntity, AtlasObjectId}

import scala.collection.convert.Wrappers.SeqWrapper

object AtlasEntityReadHelper {
  def listAtlasEntitiesAsType(entities: Seq[AtlasEntity], typeStr: String): Seq[AtlasEntity] = {
    entities.filter(p => p.getTypeName.equals(typeStr))
  }

  def getOnlyOneEntity(entities: Seq[AtlasEntity], typeStr: String): AtlasEntity = {
    val filteredEntities = entities.filter { p =>
      p.getTypeName.equals(typeStr)
    }
    assert(filteredEntities.size == 1)
    filteredEntities.head
  }

  def getOnlyOneObjectId(objIds: Seq[AtlasObjectId], typeStr: String): AtlasObjectId = {
    val filteredObjIds = objIds.filter { p =>
      p.getTypeName.equals(typeStr)
    }
    assert(filteredObjIds.size == 1)
    filteredObjIds.head
  }

  def getOnlyOneEntityOnAttribute(
                                   entities: Seq[AtlasEntity],
                                   attrName: String,
                                   attrValue: String): AtlasEntity = {
    val filteredEntities = entities.filter { p =>
      p.getAttribute(attrName).equals(attrValue)
    }
    assert(filteredEntities.size == 1)
    filteredEntities.head
  }

  def getStringAttribute(entity: AtlasEntity, attrName: String): String = {
    entity.getAttribute(attrName).asInstanceOf[String]
  }

  def getQualifiedName(entity: AtlasEntity): String = {
    entity.getAttribute(org.apache.atlas.AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME)
      .asInstanceOf[String]
  }

  def getAtlasEntityAttribute(entity: AtlasEntity, attrName: String): AtlasEntity = {
    entity.getAttribute(attrName).asInstanceOf[AtlasEntity]
  }

  def getAtlasObjectIdAttribute(entity: AtlasEntity, attrName: String): AtlasObjectId = {
    entity.getAttribute(attrName).asInstanceOf[AtlasObjectId]
  }



  def getSeqAtlasEntityAttribute(
                                  entity: AtlasEntity,
                                  attrName: String): Seq[AtlasEntity] = {
    entity.getAttribute(attrName).asInstanceOf[SeqWrapper[AtlasEntity]].underlying
  }

  def getSeqAtlasObjectIdAttribute(
                                    entity: AtlasEntity,
                                    attrName: String): Seq[AtlasObjectId] = {
    entity.getAttribute(attrName).asInstanceOf[SeqWrapper[AtlasObjectId]].underlying
  }


}
