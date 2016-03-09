package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName

/**
 * adamtwo
 *
 * Ivan Giangreco
 * November 2015
 */
@deprecated("Should be rewritten to new evaluation framework","2016-03")
object GenerateDataOp {
  val limit = 500000

  def apply(entityname: EntityName, numberOfElements : Int, numberOfDimensions : Int, createIfNotExists : Boolean = false) : Boolean = ???
    /*if(createIfNotExists && !Entity.existsEntity(entityname)){
      Entity.createEntity(entityname)
    } else if(!Entity.existsEntity(entityname)){
      return false
    }

    import SparkStartup.sqlContext.implicits._

    (0 until math.max(1, numberOfElements / limit)).foreach{ i =>
      val data = SparkStartup.sc.parallelize((0 until math.min(limit, numberOfElements))).map( id =>
        WrappingTuple(i * limit + id, new FeatureVectorWrapper(Seq.fill(numberOfDimensions)(Random.nextFloat())))
      ).toDF()
      Entity.insertData(entityname, data)
    }

    true*/

}
