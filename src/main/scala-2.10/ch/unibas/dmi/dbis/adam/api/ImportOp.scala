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
@deprecated("Should be refactored, possibly no longer works correctly","2016-03")
object ImportOp {
  case class ImportRow(id : Long, featureVectorWrapper: FeatureVectorWrapper)

  def apply(entityname: EntityName, path : String, createIfNotExists : Boolean = false): Boolean = {
    if(createIfNotExists && !Entity.exists(entityname)){
      Entity.create(entityname)
    } else if(!Entity.exists(entityname)){
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
