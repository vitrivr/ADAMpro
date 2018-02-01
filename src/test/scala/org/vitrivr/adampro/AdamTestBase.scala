package org.vitrivr.adampro

import java.sql.DriverManager

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, types}
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.{FeatureSpec, GivenWhenThen}
import org.vitrivr.adampro.communication.api.EntityOp
import org.vitrivr.adampro.data.datatypes.AttributeTypes
import org.vitrivr.adampro.data.datatypes.vector.Vector.MathVector
import org.vitrivr.adampro.data.entity.{AttributeDefinition, Entity}
import org.vitrivr.adampro.query.distance.{ManhattanDistance, MinkowskiDistance}
import org.vitrivr.adampro.query.query.Predicate
import org.vitrivr.adampro.utils.Logging
import org.vitrivr.adampro.data.datatypes.vector.{ADAMNumericalVector, Vector}
import org.vitrivr.adampro.process.{SharedComponentContext, SparkStartup}

import scala.util.Random


/**
  * adampro
  *
  * Ivan Giangreco
  * March 2016
  */
class AdamTestBase extends FeatureSpec with GivenWhenThen with Eventually with IntegrationPatience with Logging {
  val startup = SparkStartup
  implicit val ac: SharedComponentContext = startup.mainContext

  /**
    * Precision
    */
  val EPSILON = 0.0001

  /**
    * Creates a connection via JDBC to database
    *
    * @return
    */
  def getMetadataConnection = {
    Class.forName("org.postgresql.Driver").newInstance
    DriverManager.getConnection(ac.config.getString("storage.postgres.url"), ac.config.getString("storage.postgres.user"), ac.config.getString("storage.postgres.password"))
  }

  /**
    *
    * @param len
    * @return
    */
  def getRandomName(len: Int = 10) = {
    val sb = new StringBuilder(len)
    val ab = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
    for (i <- 0 until len) {
      sb.append(ab(Random.nextInt(ab.length)))
    }
    sb.toString
  }


  /**
    *
    * @param testCode
    */
  def withEntityName(testCode: String => Any) {
    val entityname = getRandomName()
    try {
      testCode(entityname)
    } finally {
      EntityOp.drop(entityname, true)
    }
  }


  /**
    *
    * @param ntuples
    * @param ndims
    * @return
    */
  def withSimpleEntity(ntuples: Int, ndims: Int)(testCode: String => Any) {
    val entityname = getRandomName()

    try {
      Entity.create(entityname,
        Seq(
          AttributeDefinition("tid", AttributeTypes.LONGTYPE, storagehandlername = "parquet"),
          AttributeDefinition("feature", AttributeTypes.VECTORTYPE, storagehandlername =  "parquet")
        ))

      val schema = StructType(Seq(
        StructField("tid", LongType, false),
        StructField("feature", AttributeTypes.VECTORTYPE.datatype, false)
      ))

      val rdd = ac.sc.parallelize((0 until ntuples).map(id =>
        Row(Random.nextLong(), Seq.fill(ndims)(Vector.nextRandom()))
      ))

      val data = ac.sqlContext.createDataFrame(rdd, schema)

      EntityOp.insert(entityname, data)

      testCode(entityname)
    }
    finally {
      EntityOp.drop(entityname, true)
    }
  }

  /**
    *
    * @param testCode
    */
  def withQueryEvaluationSet(testCode: EvaluationSet => Any) {
    val es = getGroundTruthEvaluationSet()

    try {
      testCode(es)
    }
    finally {
      EntityOp.drop(es.entity.entityname)
    }
  }

  /**
    *
    * @param entity
    * @param fullData
    * @param vector
    * @param distance
    * @param k
    * @param where
    * @param options
    * @param nnResults
    * @param nnbqResults
    */
  case class EvaluationSet(entity: Entity, fullData: DataFrame,
                           vector: ADAMNumericalVector, distance: MinkowskiDistance, k: Int,
                           where: Seq[Predicate], options: Map[String, String],
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
          val vector = splitted(2).split(",").map(Vector.conv_str2vb).toSeq
          val stringfield = splitted(3)
          val floatfield = splitted(4).toFloat
          val doublefield = splitted(5).toDouble
          val intfield = splitted(6).toInt
          val longfield = splitted(7).toLong
          val booleanfield = splitted(8).toBoolean

          Row(id, vector, stringfield, floatfield, doublefield, intfield, longfield, booleanfield, distance)
        })

      val schema = StructType(Seq(
        StructField("tid", types.LongType, false),
        StructField("vectorfield", AttributeTypes.VECTORTYPE.datatype, false),
        StructField("stringfield", types.StringType, false),
        StructField("floatfield", types.FloatType, false),
        StructField("doublefield", types.DoubleType, false),
        StructField("intfield", types.IntegerType, false),
        StructField("longfield", types.LongType, false),
        StructField("booleanfield", types.BooleanType, false),
        StructField("gtdistance", types.DoubleType, false)
      ))

      val rdd = ac.sc.parallelize(rows)
      ac.sqlContext.createDataFrame(rdd, schema)
    }

    /**
      *
      * @param filename
      * @return
      */
    def getResults(filename: String): Seq[(Double, Long)] = {
      readResourceFile(filename).map(line => {
        val splitted = line.split("\t")
        val distance = splitted(0).toDouble
        val id = splitted(1).toLong
        (distance, id)
      })
    }

    //prepare data
    val data = getData("groundtruth/data.tsv")

    val entityname = getRandomName()

    val fieldTemplate = Seq(
      ("tid", AttributeTypes.LONGTYPE, "bigint"),
      ("vectorfield", AttributeTypes.VECTORTYPE, ""),
      ("stringfield", AttributeTypes.STRINGTYPE, "text"),
      ("floatfield", AttributeTypes.FLOATTYPE, "real"),
      ("doublefield", AttributeTypes.DOUBLETYPE, "double precision"),
      ("intfield", AttributeTypes.INTTYPE, "integer"),
      ("longfield", AttributeTypes.LONGTYPE, "bigint"),
      ("booleanfield", AttributeTypes.BOOLEANTYPE, "boolean")
    )

    val entity = Entity.create(entityname, fieldTemplate.map(ft => new AttributeDefinition(ft._1, ft._2, Map[String, String]())))
    assert(entity.isSuccess)
    entity.get.insert(data.drop("gtdistance"))

    //queries
    val vector = readResourceFile("groundtruth/nnquery.txt").head.split(",").map(Vector.conv_str2vb)

    val where = readResourceFile("groundtruth/bquery.tsv").map(line => {
      val splitted = line.split("\t")
      new Predicate(splitted(0), None, Seq(splitted(1)))
    }).toList


    //100 nn results
    val nnres = getResults("groundtruth/100nn-results.tsv")

    //100 nn results and bq
    val nnbqres = getResults("groundtruth/100nn-bq-results.tsv")

    //nnq options
    val options = Map[String, String]()


    EvaluationSet(entity.get, data, ADAMNumericalVector(Vector.conv_draw2vec(vector)), ManhattanDistance, nnres.length, where, options, nnres, nnbqres)
  }


  /**
    * Gets a random feature vector.
    *
    * @param ndims
    * @return
    */
  def getRandomFeatureVector(ndims: Int) = Seq.fill(ndims)(Random.nextFloat())

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
    log.info((t2 - t1) + " msecs")
    x
  }
}
