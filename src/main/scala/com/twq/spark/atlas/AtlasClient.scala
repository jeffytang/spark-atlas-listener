package com.twq.spark.atlas

import com.sun.jersey.core.util.MultivaluedMapImpl
import com.twq.spark.atlas.utils.Logging
import org.apache.atlas.model.instance.AtlasEntity
import org.apache.atlas.model.typedef.AtlasTypesDef

import scala.util.control.NonFatal

trait AtlasClient extends Logging {
  def createAtlasTypeDefs(typeDefs: AtlasTypesDef): Unit

  def getAtlasTypeDefs(searchParams: MultivaluedMapImpl): AtlasTypesDef

  def updateAtlasTypeDefs(typeDefs: AtlasTypesDef): Unit

  final def createEntitiesWithDependencies(entity: SACAtlasReferenceable): Unit = this.synchronized {
    entity match {
      case e: SACAtlasEntityWithDependencies =>
        // handle dependencies first
        if (e.dependencies.nonEmpty) {
          val deps = e.dependencies.filter(_.isInstanceOf[SACAtlasEntityWithDependencies])
            .map(_.asInstanceOf[SACAtlasEntityWithDependencies])

          val depsHavingAnotherDeps = deps.filter(_.dependencies.nonEmpty)
          val depsHavingNoDeps = deps.filterNot(_.dependencies.nonEmpty)

          // we should handle them one by one if they're having additional dependencies
          depsHavingAnotherDeps.foreach(createEntitiesWithDependencies)
          // otherwise, we can handle them at once
          createEntities(depsHavingNoDeps.map(_.entity))
        }

      // done with dependencies, process origin entity
      createEntities(Seq(e.entity))

      case _ => // don't request creation entity for reference
    }
  }

  final def createEntitiesWithDependencies(
                                            entities: Seq[SACAtlasReferenceable]): Unit = this.synchronized {
    entities.foreach(createEntitiesWithDependencies)
  }

  final def createEntities(entities: Seq[AtlasEntity]): Unit = this.synchronized {
    if (entities.isEmpty) {
      return
    }

    try {
      doCreateEntities(entities)
    } catch {
      case NonFatal(e) =>
        logWarn(s"Failed to create entities", e)
    }
  }

  protected def doCreateEntities(entities: Seq[AtlasEntity]): Unit

  final def deleteEntityWithUniqueAttr(
                                        entityType: String, attribute: String): Unit = this.synchronized {
    try {
      doDeleteEntityWithUniqueAttr(entityType, attribute)
    } catch {
      case NonFatal(e) =>
        logWarn(s"Failed to delete entity with type $entityType", e)
    }
  }

  protected def doDeleteEntityWithUniqueAttr(entityType: String, attribute: String): Unit
}
