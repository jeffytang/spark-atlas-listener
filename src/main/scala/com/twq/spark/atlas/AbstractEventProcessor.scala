package com.twq.spark.atlas

import java.util.concurrent.{LinkedBlockingDeque, TimeUnit}

import com.twq.spark.atlas.utils.Logging
import spire.ClassTag

import scala.util.control.NonFatal

abstract class AbstractEventProcessor[T: ClassTag] extends Logging {
  def conf: AtlasClientConf

  private val capacity = conf.get(AtlasClientConf.BLOCKING_QUEUE_CAPACITY).toInt

  private[atlas] val eventQueue = new LinkedBlockingDeque[T](capacity)

  private val timeout = conf.get(AtlasClientConf.BLOCKING_QUEUE_PUT_TIMEOUT).toInt

  private val eventProcessThread = new Thread {
    override def run(): Unit = {
      eventProcess()
    }
  }

  def startThread(): Unit = {
    eventProcessThread.setName(this.getClass.getSimpleName + "-thread")
    eventProcessThread.setDaemon(true)

    val ctxClassLoader = Thread.currentThread().getContextClassLoader
    if (ctxClassLoader != null && getClass.getClassLoader != ctxClassLoader) {
      eventProcessThread.setContextClassLoader(ctxClassLoader)
    }

    eventProcessThread.start()
  }

  def pushEvent(event: T): Unit = {
    event match {
      case e: T =>
        if (!eventQueue.offer(e, timeout, TimeUnit.MILLISECONDS)) {
          logError(s"Fail to put event $e into queue within time limit $timeout, will throw it")
        }
      case _ => // Ignore other events
    }
  }

  protected def process(e: T): Unit

  private[atlas] def eventProcess(): Unit = {
    var stopped = false
    while (!stopped) {
      try {
        Option(eventQueue.poll(3000, TimeUnit.MILLISECONDS)).foreach {e =>
          process(e)
        }
      } catch {
        case _: InterruptedException =>
          logDebug("Thread is interrupted")
          stopped = true;
        case NonFatal(f) =>
          logWarn(s"Caught exception during parsing event", f)
      }
    }
  }

}
