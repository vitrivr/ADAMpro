package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.datatypes.feature.FeatureVectorWrapper
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.entity.{Entity, WrappingTuple}
import ch.unibas.dmi.dbis.adam.main.SparkStartup

import scala.util.Random

/**
 * adamtwo
 *
 * Ivan Giangreco
 * November 2015
 */
object GenerateDataOp {
  def apply(entityname: EntityName, numberOfElements : Int, numberOfDimensions : Int, createIfNotExists : Boolean = false) : Boolean = {
    if(createIfNotExists && !Entity.existsEntity(entityname)){
      Entity.createEntity(entityname)
    } else if(!Entity.existsEntity(entityname)){
      return false
    }

    import SparkStartup.sqlContext.implicits._
    val data = SparkStartup.sc.parallelize((0 until numberOfElements)).map( id =>
        WrappingTuple(id, new FeatureVectorWrapper(Seq.fill(numberOfDimensions)(Random.nextFloat())))
    ).toDF()

    Entity.insertData(entityname, data)
  }
}
