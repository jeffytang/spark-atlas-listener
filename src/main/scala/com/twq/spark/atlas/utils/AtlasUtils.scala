package com.twq.spark.atlas.utils

import java.util.concurrent.atomic.AtomicLong

import com.twq.spark.atlas.AtlasClientConf
import org.apache.atlas.model.instance.{AtlasEntity, AtlasObjectId}

object AtlasUtils extends Logging {
  private val executionId = new AtomicLong(0L)

  def entityToReference(entity: AtlasEntity, useGuid: Boolean = false): AtlasObjectId = {
    if (useGuid) {
      new AtlasObjectId(entity.getGuid)
    } else {
      new AtlasObjectId(entity.getTypeName, "qualifiedName", entity.getAttribute("qualifiedName"))
    }
  }

  def entitiesToReferences(
                            entities: Seq[AtlasEntity],
                            useGuid: Boolean = false): Set[AtlasObjectId] = {
    entities.map(entityToReference(_, useGuid)).toSet
  }

  def issueExecutionId(): Long = executionId.getAndIncrement()

  def isSacEnabled(conf: AtlasClientConf): Boolean = {
    if (!conf.get(AtlasClientConf.ATLAS_SPARK_ENABLED).toBoolean) {
      logWarn("Spark Atlas Connector is disabled.")
      false
    } else {
      true
    }
  }
}
