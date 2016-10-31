package ch.unibas.dmi.dbis.adam.evaluation.utils

import java.io.{BufferedWriter, FileWriter, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Calendar

/**
  * Created by silvan on 15.08.16.
  */
object PartitionResultLogger {

  /** stub */
  def init : Unit = {}
  private val out = new PrintWriter(new BufferedWriter(new FileWriter("part_" + new SimpleDateFormat("MMdd_HHmm").format(Calendar.getInstance.getTime) + ".tsv", true)))
  private val seperator = "\t"
  private val names = Seq("index", "tuples", "dimensions", "partitions", "partitioner", "partition")

  /** Header */
  out.println(names.mkString(seperator)+seperator+"notuples")
  out.flush()

  def write(values: Map[String, Any]): Unit = {
    values("distribution").toString.split(":").map(el => (el.split(",")(0), el.split(",")(1))).foreach(el => {
    out.println(names.filter(_!="partition").map(values(_)).mkString(seperator)+seperator+el._1+seperator+el._2)
    })
    out.flush()
  }

}
