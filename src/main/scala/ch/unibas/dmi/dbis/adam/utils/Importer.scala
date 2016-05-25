package ch.unibas.dmi.dbis.adam.utils

import java.sql.{Connection, DriverManager}

import ch.unibas.dmi.dbis.adam.api.{IndexOp, EntityOp}
import ch.unibas.dmi.dbis.adam.config.AdamConfig
import ch.unibas.dmi.dbis.adam.datatypes.FieldTypes
import ch.unibas.dmi.dbis.adam.datatypes.FieldTypes.FEATURETYPE
import ch.unibas.dmi.dbis.adam.datatypes.feature.FeatureVectorWrapper
import ch.unibas.dmi.dbis.adam.entity.FieldDefinition
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.query.distance.NormBasedDistanceFunction
import org.apache.spark.Logging
import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.jdbc.AdamDialectRegistrar

import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
  * adamtwo
  *
  * Ivan Giangreco
  * May 2016
  */
@Experimental
class Importer(url: String, user: String, password: String) extends Logging {
  AdamDialectRegistrar.register(url)

  private def openConnection(): Connection = {
    DriverManager.getConnection(url, user, password)
  }

  def importTable(schemaname: String, tablename: String) {
    try {
      log.info("importing table " + tablename)

      val renamingRules = Seq(("shotid" -> "id"))


      import SparkStartup.Implicits._
      val df = sqlContext.read.format("jdbc").options(
        Map("url" -> url, "dbtable" -> (schemaname + "." + tablename), "user" -> AdamConfig.jdbcUser, "password" -> AdamConfig.jdbcPassword, "driver" -> "org.postgresql.Driver")
      ).load()

      val conn = openConnection()

      val pkResult = conn.createStatement().executeQuery(
        "SELECT a.attname FROM pg_index i JOIN   pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) WHERE  i.indrelid = '" + schemaname + "." + tablename + "'::regclass AND i.indisprimary;")
      pkResult.next()
      val pk = {
        val adamPK = pkResult.getString(1)

        val renamedPK = renamingRules.filter(_._1 == adamPK)
        if(!renamedPK.isEmpty){
          renamedPK.head._2
        } else {
          adamPK
        }
      }

      val featureResult = conn.createStatement().executeQuery(
        "SELECT column_name FROM information_schema.columns WHERE table_schema = '" + schemaname + "' AND table_name = '" + tablename + "' AND data_type = 'feature';")
      val lb = ListBuffer[String]()
      while (featureResult.next()) {
        lb.append(featureResult.getString(1))
      }
      val featureFields = lb.toSeq


      var insertDF = df

      import org.apache.spark.sql.functions.udf
      val toFeatureVectorWrapper = udf((c: String) => {
        val vector = c.substring(1, c.length - 1).split(",").map(_.toFloat)
        new FeatureVectorWrapper(vector)
      })

      df.schema.fields.filter(x => featureFields.contains(x.name)).map { field =>
        val newName = field.name + "__" + math.abs(Random.nextInt)
        insertDF = insertDF
          .withColumn("feature", toFeatureVectorWrapper(insertDF(field.name)))
          .drop(field.name)
      }

      renamingRules.foreach{
        rule => insertDF = insertDF.withColumnRenamed(rule._1, rule._2)
      }

      val schema = insertDF.schema.fields.map(field => {
        if(featureFields.contains(field.name)){
          FieldDefinition("feature", FEATURETYPE, field.name.equals(pk))
        } else {
          val fieldType =  FieldTypes.fromDataType(field.dataType)
          FieldDefinition(field.name, fieldType, field.name.equals(pk))

        }
      })


      val entityname = schemaname + "_" + tablename
      log.info("creating new entity " + entityname + " with schema: " + schema.map(field => field.name + "(" + field.fieldtype.name + ")").mkString(", "))
      val entity = EntityOp.create(entityname, schema)

      if (entity.isSuccess) {
        log.info("importing to new entity " + entityname + " with schema: " + insertDF.schema.toString())
        EntityOp.insert(entity.get.entityname, insertDF)

        val insertDFcount = insertDF.count
        val entitycount = EntityOp.count(entityname).get
        if (insertDFcount != entitycount) {
          log.error("missing some tuples in entity " + entityname + "; in df: " + insertDFcount + ", inserted: " + entitycount)
        }
        log.info("successfully imported data into entity " + entityname + "; in df: " + insertDFcount + ", inserted: " + entitycount)
        assert(insertDFcount == entitycount)

        IndexOp.create(entityname, "feature", "vaf", NormBasedDistanceFunction(2))
      } else {
        log.error("entity not created", entity.failed.get)
      }
    } catch {
      case e: Exception => log.error("error when importing table " + tablename, e)
    }
  }
}

object Importer {
  def main(args: Array[String]): Unit = {
    SparkStartup

    val host = "192.168.99.100" + ":" + "5433"
    val database = "osvc"
    val username = "docker"
    val password = "docker"

    val importer = new Importer("jdbc:postgresql://" + host + "/" + database, username, password)

    //tables to import

    val cineast = Seq("representativeframes", "resultcachenames", "shots", "videos")
    cineast.map(table => importer.importTable("cineast", table))

    val features = Seq("averagecolor", "averagecolorarp44", "averagecolorarp44normalized", "averagecolorcld", "averagecolorcldnormalized", "averagecolorgrid8", "averagecolorgrid8normalized", "averagecolorraster", "averagefuzzyhist", "averagefuzzyhistnormalized", "chromagrid8", "cld", "cldnormalized", "contrast", "dominantcolors", "dominantedgegrid16", "dominantedgegrid8", "edgearp88", "edgearp88full", "edgegrid16", "edgegrid16full", "ehd", "huevaluevariancegrid8", "mediancolor", "mediancolorarp44", "mediancolorarp44normalized", "mediancolorgrid8", "mediancolorgrid8normalized", "mediancolorraster", "medianfuzzyhist", "medianfuzzyhistnormalized", "motionhistogram", "saturationandchroma", "saturationgrid8", "simpleperceptualhash", "stmp7eh", "subdivaveragefuzzycolor", "subdivmedianfuzzycolor", "subdivmotionhistogram2", "subdivmotionhistogram3", "subdivmotionhistogram4", "subdivmotionhistogram5", "surf", "surffull")
    features.map(table => importer.importTable("features", table))
  }
}
