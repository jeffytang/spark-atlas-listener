package com.twq.spark.atlas.sql

import com.twq.spark.atlas.SACAtlasReferenceable

trait Harvester[T] {
  def harvest(node: T, qd: QueryDetail): Seq[SACAtlasReferenceable]
}
