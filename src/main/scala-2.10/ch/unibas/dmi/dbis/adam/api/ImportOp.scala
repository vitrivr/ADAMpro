package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.datatypes.feature.FeatureVectorWrapper
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.main.SparkStartup

/**
 * adamtwo
 *
 * Ivan Giangreco
 * December 2015
 */
object ImportOp {
  case class ImportRow(id : Long, featureVectorWrapper: FeatureVectorWrapper)

  def apply(entityname: EntityName, path : String, createIfNotExists : Boolean = false): Boolean = {
    if(createIfNotExists && !Entity.existsEntity(entityname)){
      Entity.createEntity(entityname)
    } else if(!Entity.existsEntity(entityname)){
      return false
    }

    val data = SparkStartup.sqlContext.read.json(path)
      .select("id", "feature")
      .map(r => ImportRow(r.getLong(0), new FeatureVectorWrapper(r.getSeq[Double](1).map(_.toFloat))))

    import SparkStartup.sqlContext.implicits._
    Entity.insertData(entityname, data.toDF)

    true
  }
}
