package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.datatypes.FieldTypes
import ch.unibas.dmi.dbis.adam.datatypes.FieldTypes.FieldType
import ch.unibas.dmi.dbis.adam.datatypes.feature.FeatureVectorWrapper
import ch.unibas.dmi.dbis.adam.datatypes.gis.{GeographyWrapper, GeometryWrapper}
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.exception.GeneralAdamException
import ch.unibas.dmi.dbis.adam.main.AdamContext
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, StructType}

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Random, Success, Try}

/**
  * adampro
  *
  *
  * Ivan Giangreco
  * March 2016
  */
object RandomDataOp extends GenericOp {
  private val MAX_TUPLES_PER_BATCH = 100000

  /**
    * Generates random data.
    *
    * @param entityname name of entity
    * @param ntuples    size of collection to generate
    * @param params     parameters data generation
    */
  def apply(entityname: EntityName, ntuples: Int, params: Map[String, String])(implicit ac: AdamContext): Try[Void] = {
    execute("generate random data for " + entityname) {

      if (ntuples == 0) {
        throw new GeneralAdamException("please choose to create more than zero tuples")
      }

      val entity = Entity.load(entityname)
      if (entity.isFailure) {
        Failure(entity.failed.get)
      }

      //schema of random data dataframe to insert
      val schema = entity.get.schema()

      //data
      val limit = math.min(ntuples, MAX_TUPLES_PER_BATCH)
      (0 until ntuples).sliding(limit, limit).foreach { seq =>
        log.trace("starting generating data")
        val rdd = ac.sc.parallelize(
          seq.map(idx => {
            var data = schema.filterNot(_.fieldtype == FieldTypes.AUTOTYPE).map(field => randomGenerator(field.fieldtype, params)())
            Row(data: _*)
          })
        )
        val data = ac.sqlContext.createDataFrame(rdd, StructType(schema.filterNot(_.fieldtype == FieldTypes.AUTOTYPE).map(field => StructField(field.name, field.fieldtype.datatype))))

        log.trace("inserting generated data")
        val status = entity.get.insert(data, true)

        if (status.isFailure) {
          log.error("batch contained error, aborting random data insertion")
          throw status.failed.get
        }
      }
      log.debug("finished inserting")
      Success(null)
    }
  }

  /**
    *
    * @param fieldtype
    * @param params
    * @return
    */
  def randomGenerator(fieldtype: FieldType, params: Map[String, String]): () => Any = {
    fieldtype match {
      case FieldTypes.INTTYPE => () => generateInt(params)
      case FieldTypes.LONGTYPE => () => generateLong(params)
      case FieldTypes.FLOATTYPE => () => generateFloat(params)
      case FieldTypes.DOUBLETYPE => () => generateDouble(params)
      case FieldTypes.STRINGTYPE => () => generateString(params)
      case FieldTypes.TEXTTYPE => () => generateText(params)
      case FieldTypes.BOOLEANTYPE => () => generateBoolean(params)
      case FieldTypes.FEATURETYPE => () => generateFeatureVector(params)
      case FieldTypes.GEOMETRYTYPE => () => generateGeometry(params)
      case FieldTypes.GEOGRAPHYTYPE => () => generateGeography(params)
      case _ => log.error("unkown datatype"); null
    }
  }

  /**
    *
    * @param params
    */
  private def generateInt(params: Map[String, String]) : Int = {
    val max = params.get("int-max").map(_.toInt).getOrElse(Integer.MAX_VALUE)
    generateInt(max)
  }

  /**
    *
    * @param max
    * @return
    */
  private def generateInt(max: Int) = Random.nextInt(max)

  /**
    *
    * @param params
    */
  private def generateLong(params: Map[String, String]) : Long = {
    generateLong()
  }

  /**
    *
    * @return
    */
  private def generateLong() = Random.nextLong

  /**
    *
    * @param params
    */
  private def generateFloat(params: Map[String, String]) : Float = {
    val min = params.get("float-min").map(_.toFloat).getOrElse(0.toFloat)
    val max = params.get("float-max").map(_.toFloat).getOrElse(1.toFloat)

    generateFloat(min, max)
  }

  /**
    *
    * @return
    */
  private def generateFloat(min: Float, max: Float) = Random.nextFloat * (max - min) + min


  /**
    *
    * @param params
    */
  private def generateDouble(params: Map[String, String]) : Double = {
    val min = params.get("double-min").map(_.toDouble).getOrElse(0.toDouble)
    val max = params.get("double-max").map(_.toDouble).getOrElse(1.toDouble)

    generateDouble(min, max)
  }

  /**
    *
    * @return
    */
  private def generateDouble(min: Double, max: Double) = Random.nextDouble * (max - min) + min

  /**
    *
    * @param params
    * @return
    */
  private def generateString(params: Map[String, String]) : String = {
    val nletters = params.get("string-nletters").map(_.toInt).getOrElse(10)

    generateString(nletters)
  }

  /**
    *
    * @param nletters
    * @return
    */
  private def generateString(nletters: Int) = (0 until nletters).map(x => Random.nextInt(26)).map(x => ('a' + x).toChar).mkString

  /**
    *
    * @param params
    * @return
    */
  private def generateText(params: Map[String, String]) : String = {
    val nwords = params.get("text-nwords").map(_.toInt).getOrElse(100)
    val nletters = params.get("text-nletters").map(_.toInt).getOrElse(10)

    generateText(nwords, nletters)
  }

  /**
    *
    * @param nwords
    * @param nletters
    * @return
    */
  private def generateText(nwords: Int, nletters: Int) = {
    (0 until nwords).map(x => generateString((Random.nextGaussian() * nletters).toInt)).mkString(" ")
  }

  /**
    *
    * @param params
    * @return
    */
  private def generateFeatureVector(params: Map[String, String]) : FeatureVectorWrapper = {
    val dimensions = params.get("fv-dimensions").map(_.toInt)

    if (dimensions.isEmpty) {
      throw new GeneralAdamException("dimensionality not specified for feature vectors")
    }

    val sparsity = params.get("fv-sparsity").map(_.toFloat).getOrElse(0.toFloat)
    val min = params.get("fv-min").map(_.toFloat).getOrElse(0.toFloat)
    val max = params.get("fv-max").map(_.toFloat).getOrElse(1.toFloat)

    val sparse = params.get("fv-sparse").map(_.toBoolean).getOrElse(false)

    generateFeatureVector(dimensions.get, sparsity, min, max, sparse)
  }

  /**
    *
    * @param dimensions
    * @param sparsity
    * @param min
    * @param max
    * @param sparse
    * @return
    */
  private def generateFeatureVector(dimensions: Int, sparsity: Float, min: Float, max: Float, sparse: Boolean) = {
    if (dimensions == 0) {
      throw new GeneralAdamException("please choose to create vectors with more than zero dimensions")
    }

    if (!(sparsity >= 0 && sparsity <= 1.0)) {
      throw new GeneralAdamException("sparsity should be between 0 and 1")
    }

    var fv: Array[Float] = (0 until dimensions).map(i => {
      var rval = generateFloat(min, max)
      //ensure that we do not have any zeros in vector, sparsify later
      while (math.abs(rval) < 10E-6) {
        rval = generateFloat(min, max)
      }

      rval
    }).toArray

    //zero the elements in the vector
    val nzeros = math.floor(dimensions * sparsity).toInt
    (0 until nzeros).map(i => Random.nextInt(dimensions)).foreach { i =>
      fv(i) = 0.toFloat
    }

    if (sparse) {
      //sparsify vector
      val sfv = sparsify(fv)
      new FeatureVectorWrapper(sfv._2, sfv._1, sfv._3)
    } else {
      new FeatureVectorWrapper(fv.toSeq)
    }
  }

  /**
    *
    * @param vec
    * @return
    */
  private def sparsify(vec: Seq[Float]) = {
    val ii = new ListBuffer[Int]()
    val vv = new ListBuffer[Float]()

    vec.zipWithIndex.foreach { x =>
      val v = x._1
      val i = x._2

      if (math.abs(v) > 1E-10) {
        ii.append(i)
        vv.append(v)
      }
    }

    (vv.toArray, ii.toArray, vec.size)
  }

  /**
    *
    * @param params
    */
  private def generateBoolean(params: Map[String, String]) : Boolean = {
    generateBoolean()
  }

  /**
    *
    * @return
    */
  private def generateBoolean() = Random.nextBoolean


  /**
    *
    * @param params
    * @return
    */
  private def generateGeometry(params: Map[String, String]) : GeometryWrapper = {
    generateGeometry()
  }

  /**
    *
    * @return
    */
  private def generateGeometry() = {
    new GeometryWrapper("POINT(" + generateFloat(-100, 100).toString + " " +  generateFloat(-100, 100).toString + ")")
  }

  /**
    *
    * @param params
    * @return
    */
  private def generateGeography(params: Map[String, String]) : GeographyWrapper = {
    generateGeography()
  }

  /**
    *
    * @return
    */
  private def generateGeography() = {
    new GeographyWrapper("POINT(" + generateFloat(-100, 100).toString + " " +  generateFloat(-100, 100).toString + ")")
  }
}


