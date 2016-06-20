package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.datatypes.feature.FeatureVectorWrapper
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.utils.Logging
import org.apache.spark.sql.types.{DataType, StructField, StructType, UserDefinedType}
import org.apache.spark.sql.{Row, types}

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
      val schema = entity.get.schema

      //generator
      def randomGenerator(datatype: DataType): () => Any = {
        datatype match {
          case _: types.IntegerType => () => Random.nextInt
          case _: types.LongType => () => Random.nextLong
          case _: types.FloatType => () => Random.nextFloat
          case _: types.DoubleType => () => Random.nextDouble
          case _: types.StringType => () => Random.nextString(10)
          case _: types.BooleanType => () => Random.nextBoolean
          case _: UserDefinedType[_] => () => new FeatureVectorWrapper(Seq.fill(vectorSize)(Random.nextFloat()))
        }
      }

      //data
      val limit = math.min(collectionSize, 100000)
      (0 until collectionSize).sliding(limit, limit).foreach { seq =>
        val rdd = ac.sc.parallelize(
          seq.map(idx => {
            var data = schema.map(field => randomGenerator(field.fieldtype.datatype)())
            Row(data: _*)
          })
        )
        val data = ac.sqlContext.createDataFrame(rdd, StructType(schema.map(field => StructField(field.name, field.fieldtype.datatype))))

        log.debug("inserting data batch")
        entity.get.insert(data, true)
      }
      log.debug("finished inserting")
      Success(null)
    } catch {
      case e: Exception => Failure(e)
    }
  }
}


