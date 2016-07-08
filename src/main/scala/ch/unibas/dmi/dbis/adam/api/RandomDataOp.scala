package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.datatypes.FieldTypes
import ch.unibas.dmi.dbis.adam.datatypes.FieldTypes.FieldType
import ch.unibas.dmi.dbis.adam.datatypes.feature.FeatureVectorWrapper
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.exception.GeneralAdamException
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.utils.Logging
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
object RandomDataOp extends Logging {

  case class VectorDataGenerationDetails(ndims: Int, sparsity: Float, min: Float, max: Float, sparse: Boolean)

  /**
    * Generates random data.
    *
    * @param entityname name of entity
    * @param ntuples    size of collection to generate
    * @param vdetail    details for vector data generation
    */
  def apply(entityname: EntityName, ntuples: Int, vdetail: Option[VectorDataGenerationDetails])(implicit ac: AdamContext): Try[Void] = {
    try {
      log.debug("perform generate data operation")

      if(ntuples == 0){
        throw new GeneralAdamException("please choose to create more than zero tuples")
      }

      if(vdetail.isDefined) {
        if(vdetail.get.ndims == 0){
          throw new GeneralAdamException("please choose to create vectors with more than zero dimensions")
        }

        if(!(vdetail.get.sparsity >= 0 && vdetail.get.sparsity <= 1.0)){
          throw new GeneralAdamException("sparsity should be between 0 and 1")
        }
      }

      val entity = Entity.load(entityname)
      if (entity.isFailure) {
        Failure(entity.failed.get)
      }

      //schema of random data dataframe to insert
      val schema = entity.get.schema()

      //generator
      def randomGenerator(fieldtype: FieldType): () => Any = {
        fieldtype match {
          case FieldTypes.INTTYPE => () => Random.nextInt
          case FieldTypes.LONGTYPE => () => Random.nextLong
          case FieldTypes.FLOATTYPE => () => Random.nextFloat
          case FieldTypes.DOUBLETYPE => () => Random.nextDouble
          case FieldTypes.STRINGTYPE => () => generateRandomWord(10)
          case FieldTypes.TEXTTYPE => () => generateRandomText(1000)
          case FieldTypes.BOOLEANTYPE => () => Random.nextBoolean
          case FieldTypes.FEATURETYPE => () => generateFeatureVector(vdetail.get)
          case _ => log.error("unkown datatype"); null
        }
      }

      //data
      val limit = math.min(ntuples, 100000)
      (0 until ntuples).sliding(limit, limit).foreach { seq =>
        val rdd = ac.sc.parallelize(
          seq.map(idx => {
            var data = schema.filterNot(_.fieldtype == FieldTypes.AUTOTYPE).map(field => randomGenerator(field.fieldtype)())
            Row(data: _*)
          })
        )
        val data = ac.sqlContext.createDataFrame(rdd, StructType(schema.filterNot(_.fieldtype == FieldTypes.AUTOTYPE).map(field => StructField(field.name, field.fieldtype.datatype))))

        log.debug("inserting data batch")
        val status = entity.get.insert(data, true)

        if (status.isFailure) {
          throw status.failed.get
        }
      }
      log.debug("finished inserting")
      Success(null)
    } catch {
      case e: Exception =>
        log.error("error when generating random data", e)
        Failure(e)
    }
  }

  /**
    *
    * @param vdetail
    * @return
    */
  private def generateFeatureVector(vdetail: VectorDataGenerationDetails): FeatureVectorWrapper = {
    var fv : Array[Float] = (0 until vdetail.ndims).map(i => {
      var rval = Random.nextFloat()
      //ensure that we do not have any zeros in vector, sparsify later
      while (math.abs(rval) < 10E6) {
        rval = Random.nextFloat() * (vdetail.max - vdetail.min) + vdetail.min //adjust range
      }

      rval
    }).toArray

    //zero the elements in the vector
    val nzeros = math.floor(vdetail.ndims * vdetail.sparsity).toInt
    (0 until nzeros).map(i => Random.nextInt(vdetail.ndims)).foreach { i =>
      fv(i) = 0.toFloat
    }

    if (vdetail.sparse) {
      //sparsify vector
      val sfv = sparsify(fv)
      new FeatureVectorWrapper(sfv._2, sfv._1, sfv._3)
    } else {
      new FeatureVectorWrapper(fv.toSeq)
    }
  }

  /**
    *
    * @param words
    * @return
    */
  private def generateRandomText(words: Int) = {
    (0 until words).map(x => generateRandomWord((Random.nextGaussian() * 10).toInt)).mkString(" ")
  }

  /**
    *
    * @param letters
    * @return
    */
  private def generateRandomWord(letters: Int) = (0 until letters).map(x => Random.nextInt(26)).map(x => ('a' + x).toChar).mkString

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

}


