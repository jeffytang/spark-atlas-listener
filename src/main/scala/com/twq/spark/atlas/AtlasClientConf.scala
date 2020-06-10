package com.twq.spark.atlas

import com.twq.spark.atlas.AtlasClientConf.ConfigEntry
import org.apache.atlas.ApplicationProperties

class AtlasClientConf {
  private lazy val configuration = ApplicationProperties.get();

  def set(key: String, value: String): AtlasClientConf = {
    configuration.setProperty(key, value)
    this
  }

  def set(key: ConfigEntry, value: String): AtlasClientConf = {
    configuration.setProperty(key.key, value)
    this
  }

  def get(key: String, defaultValue: String): String = {
    Option(configuration.getProperty(key).asInstanceOf[String]).getOrElse(defaultValue)
  }

  def getOption(key: String): Option[String] = {
    Option(configuration.getProperty(key).asInstanceOf[String])
  }

  def getUrl(key: String): Object = {
    configuration.getProperty(key)
  }

  def get(t: ConfigEntry): String = {
    Option(configuration.getProperty(t.key).asInstanceOf[String]).getOrElse(t.defaultValue)
  }
}

object AtlasClientConf {
  // 放在这里的原因是：这个 case class 只能被当前的文件使用
  case class ConfigEntry(key: String, defaultValue: String)

  val ATLAS_SPARK_ENABLED = ConfigEntry("atlas.spark.enabled", "true")

  val ATLAS_REST_ENDPOINT = ConfigEntry("atlas.rest.address", "localhost:21000")

  val BLOCKING_QUEUE_CAPACITY = ConfigEntry("atlas.blockQueue.size", "10000")
  val BLOCKING_QUEUE_PUT_TIMEOUT = ConfigEntry("atlas.blockQueue.putTimeout.ms", "3000")

  val CLIENT_TYPE = ConfigEntry("atlas.client.type", "kafka")
  val CLIENT_USERNAME = ConfigEntry("atlas.client.username", "admin")
  val CLIENT_PASSWORD = ConfigEntry("atlas.client.password", "admin123")
  val CLIENT_NUM_RETRIES = ConfigEntry("atlas.client.numRetries", "3")

  val CLUSTER_NAME = ConfigEntry("atlas.cluster.name", "primary")
}
