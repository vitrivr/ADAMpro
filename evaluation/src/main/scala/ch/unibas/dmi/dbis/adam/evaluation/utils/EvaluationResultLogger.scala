package ch.unibas.dmi.dbis.adam.evaluation.utils

import java.io.{BufferedWriter, FileWriter, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Calendar

/**
  * Created by silvan on 08.07.16.
  */
trait EvaluationResultLogger extends Logging {
  val out = new PrintWriter(new BufferedWriter(new FileWriter("results_" + new SimpleDateFormat("MMdd_HHmm").format(Calendar.getInstance.getTime) + ".tsv", true)))
  val seperator = "\t"

  /** Everything that gets logged */
  val names = Seq("tuples", "dimensions", "index", "partitioner", "partitions", "k", "skipPercentage", "time", "nores", "topk", "noskip_topk")


  /** Header line. Not formatted in line with results, but that's life */
  out.println("curr_time" + seperator + names.mkString(seperator))
  out.flush()

  /** Pads some numbers for better readability */
  def write(values: Map[String, Any]) = {
    out.println(Calendar.getInstance.getTime + seperator + names.map(values(_)).mkString(seperator))
    out.flush()
  }
}
