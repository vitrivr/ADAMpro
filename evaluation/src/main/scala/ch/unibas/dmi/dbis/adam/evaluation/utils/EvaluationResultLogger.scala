package ch.unibas.dmi.dbis.adam.evaluation.utils

import java.io.{BufferedWriter, FileWriter, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Calendar

/**
  * Created by silvan on 08.07.16.
  */
object EvaluationResultLogger {

  /** Everything that gets logged */
  private val names = Seq("tuples", "dimensions", "index", "partitioner", "partitions", "k", "skipPercentage", "time", "nores", "skip_recall", "noskip_recall")

  /** Precision/Recall @k */
  def init(k: Int): Unit = {
    /** Header line. Not formatted in line with results, but that's life */
    val pr = Seq.tabulate(k)(el => "p@" + el + seperator + "r@" + el)
    out.println(names.mkString(seperator) + seperator + pr.mkString(seperator))
    out.flush()
  }

  private val out = new PrintWriter(new BufferedWriter(new FileWriter("results_" + new SimpleDateFormat("MMdd_HHmm").format(Calendar.getInstance.getTime) + ".tsv", true)))
  private val seperator = "\t"

  def write(values: Map[String, Any]) = {
    writePR(values, IndexedSeq[(Float, Float)]())
  }

  def writePR(values: Map[String, Any], pr: IndexedSeq[(Float, Float)]): Unit = {
    out.println(names.map(values(_)).mkString(seperator) + seperator + pr.map(el => el._1 + seperator + el._2).mkString(seperator))
    out.flush()
  }
}
