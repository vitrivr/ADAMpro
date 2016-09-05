package ch.unibas.dmi.dbis.adam

import java.sql.DriverManager

import ch.unibas.dmi.dbis.adam.api.EntityOp
import ch.unibas.dmi.dbis.adam.config.AdamConfig
import ch.unibas.dmi.dbis.adam.datatypes.FieldTypes
import ch.unibas.dmi.dbis.adam.datatypes.feature.{FeatureVectorWrapper, FeatureVectorWrapperUDT}
import ch.unibas.dmi.dbis.adam.entity.{AttributeDefinition, Entity}
import ch.unibas.dmi.dbis.adam.main.{AdamContext, SparkStartup}
import ch.unibas.dmi.dbis.adam.query.distance.{ManhattanDistance, MinkowskiDistance}
import ch.unibas.dmi.dbis.adam.utils.Logging
import org.apache.spark.sql.types.{LongType, StructField, StructType}
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
class AdamTestBase extends FeatureSpec with GivenWhenThen with Eventually with IntegrationPatience with Logging {
  val startup = SparkStartup
  implicit val ac: AdamContext = startup.mainContext

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
    DriverManager.getConnection(AdamConfig.getString("storage.jdbc.url"), AdamConfig.getString("storage.jdbc.user"), AdamConfig.getString("storage.jdbc.password"))
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
          AttributeDefinition("tid", FieldTypes.LONGTYPE, true),
          AttributeDefinition("feature", FieldTypes.FEATURETYPE, false, false, false)
        ))

      val schema = StructType(Seq(
        StructField("tid", LongType, false),
        StructField("feature", new FeatureVectorWrapperUDT, false)
      ))

      val rdd = ac.sc.parallelize((0 until ntuples).map(id =>
        Row(Random.nextLong(), new FeatureVectorWrapper(Seq.fill(ndims)(Random.nextFloat())))
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
    * @param feature
    * @param distance
    * @param k
    * @param where
    * @param options
    * @param nnResults
    * @param nnbqResults
    */
  case class EvaluationSet(entity: Entity, fullData: DataFrame,
                           feature: FeatureVectorWrapper, distance: MinkowskiDistance, k: Int,
                           where: Option[Seq[(String, String)]], options: Map[String, String],
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
        StructField("tid", types.LongType, true),
        StructField("featurefield", new FeatureVectorWrapperUDT, false),
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
      ("tid", FieldTypes.LONGTYPE, true, "bigint"),
      ("featurefield", FieldTypes.FEATURETYPE, false, ""),
      ("stringfield", FieldTypes.STRINGTYPE, false, "text"),
      ("floatfield", FieldTypes.FLOATTYPE, false, "real"),
      ("doublefield", FieldTypes.DOUBLETYPE, false, "double precision"),
      ("intfield", FieldTypes.INTTYPE, false, "integer"),
      ("longfield", FieldTypes.LONGTYPE, false, "bigint"),
      ("booleanfield", FieldTypes.BOOLEANTYPE, false, "boolean")
    )

    val entity = Entity.create(entityname, fieldTemplate.map(ft => AttributeDefinition(ft._1, ft._2, ft._3)))
    assert(entity.isSuccess)
    entity.get.insert(data.drop("gtdistance"))

    //queries
    val feature = new FeatureVectorWrapper(readResourceFile("groundtruth/nnquery.txt").head.split(",").map(_.toFloat))

    val where = readResourceFile("groundtruth/bquery.tsv").map(line => {
      val splitted = line.split("\t")
      splitted(0) -> splitted(1)
    }).toList


    //100 nn results
    val nnres = getResults("groundtruth/100nn-results.tsv")

    //100 nn results and bq
    val nnbqres = getResults("groundtruth/100nn-bq-results.tsv")

    //nnq options
    val options = Map[String, String]()


    EvaluationSet(entity.get, data, feature, ManhattanDistance, nnres.length, Option(where), options, nnres, nnbqres)
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
    log.info((t2 - t1) + " msecs")
    x
  }
}
