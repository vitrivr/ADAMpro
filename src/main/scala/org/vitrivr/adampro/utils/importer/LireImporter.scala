package org.vitrivr.adampro.utils.importer

import java.io.File

import org.apache
import org.apache.commons.io.FileUtils
import org.apache.spark
import org.apache.spark.sql.DataFrame
import org.vitrivr.adampro.communication.api.EntityOp
import org.vitrivr.adampro.data.entity.Entity.EntityName
import org.vitrivr.adampro.process.{SharedComponentContext, SparkStartup}


/**
  * ADAMpro
  *
  * Ivan Giangreco
  * September 2017
  */
class LireImporter(path : String, filetype : String, entityname : EntityName)(implicit ac: SharedComponentContext) {

  /**
    *
    */
  def apply(): Unit ={
    val gfiles = getAllFiles(path, filetype)

    gfiles.foreach{ files =>
      val data = readFile(files : _*)

      EntityOp.insert(entityname, data)
    }
  }


  /**
    *
    * @param path
    * @return
    */
  private def getAllFiles(path: String, filetype : String) = {
    import scala.collection.JavaConverters._
    FileUtils.listFiles(new File(path), Array(filetype), true).asScala.toList.sortBy(_.getAbsolutePath.reverse).map(_.getAbsolutePath).sliding(100, 100)
  }


  private def readFile(paths : String*) : DataFrame = {
    import ac.spark.implicits._
    ac.spark.read.textFile(paths : _*).filter(_.contains("\t")).map(_.split("\t")).map(row => (row(0), row(1), row(2), row(3).split(" ").map(_.toFloat))).toDF("id", "featuretype", "length", "feature").select("id", "feature")
  }



}

object LireImporter {
  def apply(path : String, filetype : String, entityname : EntityName)(implicit ac: SharedComponentContext): Unit = {
    new LireImporter(path, filetype, entityname)(ac)()
  }


  def main(args: Array[String]): Unit = {
    //for experimental reasons only
    val ac = SparkStartup.mainContext

    apply(args(0), args(1), args(2))(ac)
  }
}