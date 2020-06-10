package com.twq.spark.atlas

import java.util.UUID

import com.twq.spark.atlas.utils.Logging

class AtlasEntityCreationRequestHelper(
                                        atlasClient: AtlasClient) extends Logging {


  def requestCreation(entities: Seq[SACAtlasReferenceable], queryId: Option[UUID] = None): Unit = {
     updateEntitiesForBatchQuery(entities)
  }

  private def updateEntitiesForBatchQuery(entities: Seq[SACAtlasReferenceable]): Unit = {
    // the query is batch, hence always create entities
    // create input/output entities as well as update process entity(-ies)
    createEntities(entities)
  }

  private def createEntities(entities: Seq[SACAtlasReferenceable]): Unit = {
    // create input/output entities as well as update process entity(-ies)
    atlasClient.createEntitiesWithDependencies(entities)
    logDebug(s"Created entities without columns")
  }

}
