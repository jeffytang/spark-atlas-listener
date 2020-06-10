package com.twq.spark.atlas

import com.twq.spark.atlas.utils.AtlasUtils
import org.apache.atlas.model.instance.{AtlasEntity, AtlasObjectId}

trait SACAtlasReferenceable {
  def typeName: String
  def qualifiedName: String
  def asObjectId: AtlasObjectId
}

case class SACAtlasEntityReference(ref: AtlasObjectId) extends SACAtlasReferenceable {
  require(typeName != null && !typeName.isEmpty)
  require(qualifiedName != null && !qualifiedName.isEmpty)

  override def typeName: String = ref.getTypeName

  override def qualifiedName: String = ref.getUniqueAttributes.get(
    org.apache.atlas.AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME).toString

  override def asObjectId: AtlasObjectId = ref
}

case class SACAtlasEntityWithDependencies(entity: AtlasEntity,
                                          dependencies: Seq[SACAtlasReferenceable])
  extends SACAtlasReferenceable {

  require(typeName != null && !typeName.isEmpty)
  require(qualifiedName != null && !qualifiedName.isEmpty)

  override def typeName: String = entity.getTypeName

  override def qualifiedName: String = entity.getAttribute(
    org.apache.atlas.AtlasClient.REFERENCEABLE_ATTRIBUTE_NAME).toString

  override def asObjectId: AtlasObjectId = AtlasUtils.entityToReference(entity, useGuid = false)

  def dependenciesAdded(deps: Seq[SACAtlasReferenceable]): SACAtlasEntityWithDependencies = {
    new SACAtlasEntityWithDependencies(entity, dependencies ++ deps)
  }
}

object SACAtlasEntityWithDependencies {
  def apply(entity: AtlasEntity): SACAtlasEntityWithDependencies = {
    new SACAtlasEntityWithDependencies(entity, Seq.empty)
  }
}
