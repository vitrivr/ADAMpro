package ch.unibas.dmi.dbis.adam.helpers.sparsify

import breeze.linalg.SparseVector
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature.{SparseFeatureVector, VectorBase}
import ch.unibas.dmi.dbis.adam.datatypes.feature.FeatureVectorWrapper
import ch.unibas.dmi.dbis.adam.entity.{Entity, EntityLRUCache}
import ch.unibas.dmi.dbis.adam.exception.GeneralAdamException
import ch.unibas.dmi.dbis.adam.main.AdamContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

import scala.util.{Failure, Success, Try}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  */
object SparsifyHelper {
  /**
    * Compresses the entity by turning vectors to sparse.
    *
    * @param entity        entity
    * @param attributename name of attribute
    * @return
    */
  def apply(entity: Entity, attributename: String)(implicit ac: AdamContext): Try[Entity] = {
    try {
      if (entity.getFeatureData.isEmpty) {
        return Failure(new GeneralAdamException("no feature data available for performing sparsifying"))
      }

      var data = entity.getData().get
      val convertToSparse = udf((c: FeatureVectorWrapper) => {
        val vec = c.vector

        if (vec.isInstanceOf[SparseFeatureVector]) {
          c
        } else {
          var numNonZeros = 0
          var k = 0
          numNonZeros = {
            var nnz = 0
            vec.foreach { v =>
              if (math.abs(v) > 1E-10) {
                nnz += 1
              }
            }
            nnz
          }

          //TODO: possibly check here if nnz > 0.5 length then do not translate to sparse (or possibly allow mixed)

          val ii = new Array[Int](numNonZeros)
          val vv = new Array[VectorBase](numNonZeros)
          k = 0

          vec.foreachPair { (i, v) =>
            if (math.abs(v) > 1E-10) {
              ii(k) = i
              vv(k) = v
              k += 1
            }
          }

          if (ii.nonEmpty) {
            new FeatureVectorWrapper(ii, vv, vec.size)
          } else {
            FeatureVectorWrapper(SparseVector.zeros(vec.size))
          }
        }
      })

      data = data.withColumn("conv-" + attributename, convertToSparse(data(attributename)))
      data = data.drop(attributename).withColumnRenamed("conv-" + attributename, attributename)

      val handler = entity.schema(Some(Seq(attributename))).head.storagehandler.get

      //select data which is available in the one handler
      val attributes = entity.schema().filterNot(_.pk).filter(_.storagehandler.get == handler).+:(entity.pk)
      data = data.select(attributes.map(attribute => col(attribute.name)).toArray: _*)

      val status = handler.write(entity.entityname, data, SaveMode.Overwrite)
      if (status.isFailure) {
        throw status.failed.get
      }

      entity.markStale()
      EntityLRUCache.invalidate(entity.entityname)

      Success(entity)

    } catch {
      case e: Exception => Failure(e)
    }
  }
}
