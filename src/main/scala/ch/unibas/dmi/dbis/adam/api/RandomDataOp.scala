package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.datatypes.FieldTypes
import ch.unibas.dmi.dbis.adam.datatypes.FieldTypes.FieldType
import ch.unibas.dmi.dbis.adam.datatypes.feature.FeatureVectorWrapper
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.utils.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, StructType}

import scala.util.{Failure, Random, Success, Try}

/**
  * adampro
  *
  *
  * Ivan Giangreco
  * March 2016
  */
object RandomDataOp extends Logging {
  /**
    * Generates random data.
    *
    * @param entityname     name of entity
    * @param collectionSize size of collection
    * @param vectorSize     size of feature vectors
    */
  def apply(entityname: EntityName, collectionSize: Int, vectorSize: Int)(implicit ac: AdamContext): Try[Void] = {
    try {
      log.debug("perform generate data operation")

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
          case FieldTypes.FEATURETYPE => () => new FeatureVectorWrapper(Seq.fill(vectorSize)(Random.nextFloat()))
        }
      }

      //data
      val limit = math.min(collectionSize, 100000)
      (0 until collectionSize).sliding(limit, limit).foreach { seq =>
        val rdd = ac.sc.parallelize(
          seq.map(idx => {
            var data = schema.map(field => randomGenerator(field.fieldtype)())
            Row(data: _*)
          })
        )
        val data = ac.sqlContext.createDataFrame(rdd, StructType(schema.map(field => StructField(field.name, field.fieldtype.datatype))))

        log.debug("inserting data batch")
        val status = entity.get.insert(data, true)

        if(status.isFailure){
          throw status.failed.get
        }
      }
      log.debug("finished inserting")
      Success(null)
    } catch {
      case e: Exception => Failure(e)
    }
  }

  private def generateRandomText(words : Int) = {
    (0 until words).map(x => generateRandomWord((Random.nextGaussian() * 10).toInt)).mkString(" ")
  }

  private def generateRandomWord(letters : Int) = (0 until letters).map(x => Random.nextInt(26)).map(x => ('a' + x).toChar).mkString

}


