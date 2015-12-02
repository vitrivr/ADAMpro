package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.main.SparkStartup

import scala.concurrent.Future

/**
 * adamtwo
 *
 * Ivan Giangreco
 * December 2015
 */
object ImportOp {
  def apply(entityname: EntityName, path : String, createIfNotExists : Boolean = false): Boolean = {
    if(createIfNotExists && !Entity.existsEntity(entityname)){
      Entity.createEntity(entityname)
    } else if(!Entity.existsEntity(entityname)){
      return false
    }

    import scala.concurrent.ExecutionContext.Implicits.global
    Future {
      val data = SparkStartup.sqlContext.read.parquet(path)
      Entity.insertData(entityname, data)
    }

    true
  }
}
