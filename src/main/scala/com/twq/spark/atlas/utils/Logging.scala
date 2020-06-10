package com.twq.spark.atlas.utils

import org.slf4j.LoggerFactory

trait Logging {
  lazy val logger = LoggerFactory.getLogger(this.getClass)

  def logTrace(message: => Any): Unit = {
    if (logger.isTraceEnabled) {
      logger.trace(message.toString)
    }
  }

  def logDebug(message: => Any): Unit = {
    if (logger.isDebugEnabled) {
      logger.debug(message.toString)
    }
  }

  def logInfo(message: => Any): Unit = {
    if (logger.isInfoEnabled) {
      logger.info(message.toString)
    }
  }

  def logWarn(message: => Any): Unit = {
    logger.warn(message.toString)
  }

  def logWarn(message: => Any, t: Throwable): Unit = {
    logger.warn(message.toString, t)
  }

  def logError(message: => Any, t: Throwable): Unit = {
    logger.error(message.toString, t)
  }

  def logError(message: => Any): Unit = {
    logger.error(message.toString)
  }
}
