package ch.unibas.dmi.dbis.adam.query.datastructures

import ch.unibas.dmi.dbis.adam.config.AdamConfig
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.progressive.ScanFuture
import org.apache.spark.Logging
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.Duration
import scala.concurrent.{CanAwait, ExecutionContext, Future}
import scala.util.{Success, Try}


/**
  * adamtwo
  *
  * Tracks the status of a progressively running query.
  *
  * Ivan Giangreco
  * September 2015
  */
//TODO: add time
case class ProgressiveQueryIntermediateResults(confidence: Float, results: DataFrame, source: String) {
  def this(future: ScanFuture, results: DataFrame) {
    this(future.confidence, results, future.typename)
  }
}


class ProgressiveQueryStatusTracker(queryID: String)(implicit ac : AdamContext) extends Future[ProgressiveQueryIntermediateResults] with Logging {
  private val futures = ListBuffer[ScanFuture]()
  private val runningStatusLock = new Object()
  @volatile private var runningStatus = ProgressiveQueryStatus.RUNNING
  private var intermediateResult = ProgressiveQueryIntermediateResults(0, null, "empty")

  /**
    * Register a scan future.
    *
    * @param future
    */
  def register(future: ScanFuture): Unit = {
    log.debug("registered new scan future")
    futures.synchronized(futures += future)
  }

  /**
    * Notifies the tracker of its completion.
    *
    * @param future
    */
  def notifyCompletion(future: ScanFuture, results: DataFrame): Unit = {
    log.debug("scanning completed")

    futures.synchronized({
      if (future.confidence > intermediateResult.confidence && runningStatus == ProgressiveQueryStatus.RUNNING) {
        intermediateResult = new ProgressiveQueryIntermediateResults(future, results)
      }

      if (!AdamConfig.evaluation) {
        // in evaluation mode we want to keep all results and do not stop, as to be able to measure how
        // much time each index would run
        if (math.abs(intermediateResult.confidence - 1.0) < 0.000001) {
          log.debug("confident results retrieved")
          stop(ProgressiveQueryStatus.FINISHED)
        }
      }

      futures -= future

      if (futures.isEmpty) {
        stop(ProgressiveQueryStatus.FINISHED)
      }
    })
  }

  /**
    * Prematurely stops the progressive query.
    */
  def stop(): Unit = {
    stop(ProgressiveQueryStatus.PREMATURE_FINISHED)
  }

  /**
    * Stops the progressive query with the new status.
    *
    * @param newStatus
    */
  private def stop(newStatus: ProgressiveQueryStatus.Value): Unit = {
    if (runningStatus == ProgressiveQueryStatus.FINISHED) {
      //already finished
      log.debug("requested stopping progresive query, but stopped already")
      return
    }

    futures.synchronized {
      log.debug("stopping progressive query with status " + newStatus)
      ac.sc.cancelJobGroup(queryID)
      futures.clear()
    }

    runningStatusLock.synchronized {
      runningStatus = newStatus
      runningStatusLock.notifyAll()
    }
  }

  /**
    * Returns the most up-to-date results together with a confidence score.
    *
    * @return
    */
  def results = intermediateResult

  /**
    * Returns the current status of the progressive query.
    *
    * @return
    */
  def status = futures.synchronized {
    runningStatus
  }

  /**
    * Returns the number of current queries running in parallel.
    *
    * @return
    */
  def length = {
    futures.size
  }


  override def isCompleted: Boolean = (status != ProgressiveQueryStatus.RUNNING)

  val observers = ListBuffer[(Try[ProgressiveQueryIntermediateResults]) => _]()
  override def onComplete[U](func: (Try[ProgressiveQueryIntermediateResults]) => U)(implicit executor: ExecutionContext): Unit = {
    observers.+=(func)
  }

  override def value: Option[Try[ProgressiveQueryIntermediateResults]] = {
    if (isCompleted) {
      return Some(Success(intermediateResult))
    } else {
      None
    }
  }

  override def result(atMost: Duration)(implicit permit: CanAwait): ProgressiveQueryIntermediateResults = {
    runningStatusLock.synchronized {
      while (runningStatus == ProgressiveQueryStatus.RUNNING) {
        runningStatusLock.wait(atMost.toMillis)
      }
    }

    intermediateResult
  }

  override def ready(atMost: Duration)(implicit permit: CanAwait): ProgressiveQueryStatusTracker.this.type = {
    runningStatusLock.synchronized {
      while (runningStatus == ProgressiveQueryStatus.RUNNING) {
        runningStatusLock.wait(atMost.toMillis)
      }
    }

    this
  }
}

/**
  *
  */
object ProgressiveQueryStatus extends Enumeration {
  val RUNNING = Value("running")
  val PREMATURE_FINISHED = Value("premature")
  val FINISHED = Value("finished")
}
