package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.datatypes.feature.{FeatureVectorWrapper, FeatureVectorWrapperUDT}
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.entity.{EntityHandler, FieldDefinition}
import ch.unibas.dmi.dbis.adam.entity.FieldTypes._
import ch.unibas.dmi.dbis.adam.main.SparkStartup.Implicits._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, StructType}

import scala.util.Random

/**
  * adampro
  *
  * Ivan Giangreco
  * March 2016
  */
object RandomDataOp {
  def apply(entityname: EntityName, collectionSize: Int, vectorSize: Int, fields: Option[Seq[FieldDefinition]] = None): Unit = {
    val limit = math.min(collectionSize, 100000)

    val entity = CreateEntityOp(entityname, fields)
    if (entity.isFailure) {
      throw entity.failed.get
    }

    try {
      //schema of random data dataframe to insert
      var schemaFields = Seq(StructField(FieldNames.featureColumnName, new FeatureVectorWrapperUDT, false))
      if (fields.isDefined) {
        schemaFields.++=(fields.get.map(field => StructField(field.name, field.fieldtype.datatype)))
      }
      val schema = StructType(schemaFields)


      //data
      (0 until collectionSize).sliding(limit, limit).foreach { seq =>
        val rdd = ac.sc.parallelize(
          seq.map(idx => {
            var data: Seq[Any] = Seq(new FeatureVectorWrapper(Seq.fill(vectorSize)(Random.nextFloat())))
            if (fields.isDefined) {
              data.++=(fields.get.map(field => randomGenerator(field.fieldtype)()))
            }

            Row(data: _*)
          })
        )
        val data = sqlContext.createDataFrame(rdd, schema)

        EntityHandler.insertData(entityname, data, true)
      }

      //entity with data created
    } catch {
      case e : Throwable =>
        DropEntityOp(entity.get.entityname)
        throw e
    }
  }

  private def randomGenerator(fieldtype: FieldType): () => Any = {
    fieldtype match {
      case INTTYPE => () => (Random.nextInt)
      case LONGTYPE => () => (Random.nextLong)
      case FLOATTYPE => () => (Random.nextFloat)
      case DOUBLETYPE => () => (Random.nextDouble)
      case STRINGTYPE => () => (Random.nextString(10))
      case BOOLEANTYPE => () => (Random.nextBoolean)
      case UNRECOGNIZEDTYPE => () => (null)
    }
  }
}


