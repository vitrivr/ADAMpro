package ch.unibas.dmi.dbis.adam

import java.sql.DriverManager

import ch.unibas.dmi.dbis.adam.config.{AdamConfig, FieldNames}
import ch.unibas.dmi.dbis.adam.datatypes.feature.{FeatureVectorWrapper, FeatureVectorWrapperUDT}
import ch.unibas.dmi.dbis.adam.entity.{Entity, FieldDefinition, FieldTypes}
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.query.distance.{ManhattanDistance, MinkowskiDistance}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, types}
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.{FeatureSpec, GivenWhenThen}

import scala.util.Random

/**
  * adampro
  *
  * Ivan Giangreco
  * March 2016
  */
class AdamTestBase extends FeatureSpec with GivenWhenThen with Eventually with IntegrationPatience {
  SparkStartup

  /**
    * Precision
    */
  val EPSILON = 0.0001

  /**
    * Creates a connection via JDBC to database
    *
    * @return
    */
  def getJDBCConnection = {
    Class.forName("org.postgresql.Driver").newInstance
    DriverManager.getConnection(AdamConfig.jdbcUrl, AdamConfig.jdbcUser, AdamConfig.jdbcPassword)
  }


  /**
    *
    * @param len
    * @return
    */
  def getRandomName(len: Int = 10) = {
    val sb = new StringBuilder(len)
    val ab = "abcdefghijklmnopqrstuvwxyz"
    for (i <- 0 until len) {
      sb.append(ab(Random.nextInt(ab.length)))
    }
    sb.toString
  }


  /**
    *
    * @param ntuples
    * @param ndims
    * @return
    */
  def createSimpleEntity(ntuples: Int, ndims: Int): String = {
    val entityname = getRandomName()
    Entity.create(entityname)

    val schema = StructType(Seq(
      StructField(FieldNames.featureColumnName, new FeatureVectorWrapperUDT, false)
    ))

    val rdd = SparkStartup.sc.parallelize((0 until ntuples).map(id =>
      Row(new FeatureVectorWrapper(Seq.fill(ndims)(Random.nextFloat())))
    ))

    val data = SparkStartup.sqlContext.createDataFrame(rdd, schema)

    Entity.insertData(entityname, data)

    entityname
  }


  /**
    *
    * @param ntuples
    * @param ndims
    * @return
    */
  def createEntityWithMetadata(ntuples: Int, ndims: Int): String = {
    val entityname = getRandomName()

    val fieldTemplate = Seq(
      ("stringfield", FieldTypes.STRINGTYPE, "text"),
      ("floatfield", FieldTypes.FLOATTYPE, "real"),
      ("doublefield", FieldTypes.DOUBLETYPE, "double precision"),
      ("intfield", FieldTypes.INTTYPE, "integer"),
      ("longfield", FieldTypes.LONGTYPE, "bigint"),
      ("booleanfield", FieldTypes.BOOLEANTYPE, "boolean")
    )

    Entity.create(entityname, Some(fieldTemplate.map(ft => (ft._1, FieldDefinition(ft._2))).toMap))

    val stringLength = 10
    val maxInt = 50000

    val schema = StructType(fieldTemplate
      .map(field => StructField(field._1, field._2.datatype, false)).+:(StructField(FieldNames.featureColumnName, new FeatureVectorWrapperUDT, false)))


    val rdd = SparkStartup.sc.parallelize((0 until ntuples).map(id =>
      Row(
        getRandomFeatureVector(ndims),
        Random.nextString(stringLength),
        math.abs(Random.nextFloat()),
        math.abs(Random.nextDouble()),
        math.abs(Random.nextInt(maxInt)),
        id.toLong, //we use this field as id field
        Random.nextBoolean()
      )))

    val data = SparkStartup.sqlContext.createDataFrame(rdd, schema)

    Entity.insertData(entityname, data)

    entityname
  }

  /**
    *
    * @param entity
    * @param fullData
    * @param feature
    * @param distance
    * @param k
    * @param where
    * @param nnResults
    * @param nnbqResults
    */
  case class EvaluationSet(entity: Entity, fullData: DataFrame,
                           feature: FeatureVectorWrapper, distance: MinkowskiDistance, k: Int,
                           where: Option[Seq[(String, String)]],
                           nnResults: Seq[(Double, Long)], nnbqResults: Seq[(Double, Long)])

  /**
    * Reads the ground truth evaluation set composed of an entity and the kNN results.
    *
    * @return
    */
  def getGroundTruthEvaluationSet(): EvaluationSet = {
    /**
      *
      * @param filename
      * @param hasHeader
      * @return
      */
    def readResourceFile(filename: String, hasHeader: Boolean = false): Seq[String] = {
      val stream = getClass.getResourceAsStream("/" + filename)
      val lines = scala.io.Source.fromInputStream(stream).getLines.toSeq

      if (hasHeader) {
        lines.drop(1)
      } else {
        lines
      }
    }

    /**
      *
      * @param filename
      * @return
      */
    def getData(filename: String): DataFrame = {
      val rows = readResourceFile(filename, true)
        .map(line => {
          val splitted = line.split("\t")
          val distance = splitted(0).toDouble
          val id = splitted(1).toLong
          val feature = new FeatureVectorWrapper(splitted(2).split(",").map(_.toFloat).toSeq)
          val stringfield = splitted(3)
          val floatfield = splitted(4).toFloat
          val doublefield = splitted(5).toDouble
          val intfield = splitted(6).toInt
          val longfield = splitted(7).toLong
          val booleanfield = splitted(8).toBoolean

          Row(id, feature, stringfield, floatfield, doublefield, intfield, longfield, booleanfield, distance)
        })

      val schema = StructType(Seq(
        StructField("tid", types.LongType, false),
        StructField(FieldNames.featureColumnName, new FeatureVectorWrapperUDT, false),
        StructField("stringfield", types.StringType, false),
        StructField("floatfield", types.FloatType, false),
        StructField("doublefield", types.DoubleType, false),
        StructField("intfield", types.IntegerType, false),
        StructField("longfield", types.LongType, false),
        StructField("booleanfield", types.BooleanType, false),
        StructField("gtdistance", types.DoubleType, false)
      ))

      val rdd = SparkStartup.sc.parallelize(rows)
     SparkStartup.sqlContext.createDataFrame(rdd, schema)
    }

    /**
      *
      * @param filename
      * @return
      */
    def getResults(filename : String): Seq[(Double, Long)] ={
      readResourceFile(filename).map(line => {
        val splitted = line.split("\t")
        val distance = splitted(0).toDouble
        val id = splitted(1).toLong
        (distance, id)
      })
    }

    //prepare data
    val data = getData("groundtruth/data.tsv")

    val fieldTemplate = Seq(
      ("tid", FieldTypes.LONGTYPE, "bigint"),
      ("stringfield", FieldTypes.STRINGTYPE, "text"),
      ("floatfield", FieldTypes.FLOATTYPE, "real"),
      ("doublefield", FieldTypes.DOUBLETYPE, "double precision"),
      ("intfield", FieldTypes.INTTYPE, "integer"),
      ("longfield", FieldTypes.LONGTYPE, "bigint"),
      ("booleanfield", FieldTypes.BOOLEANTYPE, "boolean")
    )
    val entity = Entity.create(getRandomName(), Some(fieldTemplate.map(ft => (ft._1, FieldDefinition(ft._2))).toMap))
    assert(entity.isSuccess)
    entity.get.insert(data.drop("gtdistance"))

    //queries
    val feature = new FeatureVectorWrapper(readResourceFile("groundtruth/nnquery.txt").head.split(",").map(_.toFloat))

    val where = readResourceFile("groundtruth/bquery.tsv").map(line => {
      val splitted = line.split("\t")
      splitted(0) -> splitted(1)
    })


    //100 nn results
    val nnres = getResults("groundtruth/100nn-results.tsv")

    //100 nn results and bq
    val nnbqres = getResults("groundtruth/100nn-bq-results.tsv")


    EvaluationSet(entity.get, data, feature, ManhattanDistance, nnres.length, Option(where), nnres, nnbqres)
  }


  /**
    * Gets a random feature vector.
    *
    * @param ndims
    * @return
    */
  def getRandomFeatureVector(ndims: Int) = new FeatureVectorWrapper(Seq.fill(ndims)(Random.nextFloat()))

  /**
    * Corrspondence score between two lists
    *
    * @param groundtruth
    * @param results
    * @return
    */
  def getScore(groundtruth: Seq[Long], results: Seq[Long]): Double = {
    groundtruth.intersect(results).length.toFloat / groundtruth.size.toFloat
  }

  /**
    *
    * @param thunk
    * @tparam T
    * @return
    */
  def time[T](thunk: => T): T = {
    val t1 = System.currentTimeMillis
    val x = thunk
    val t2 = System.currentTimeMillis
    println((t2 - t1) + " msecs")
    x
  }
}
