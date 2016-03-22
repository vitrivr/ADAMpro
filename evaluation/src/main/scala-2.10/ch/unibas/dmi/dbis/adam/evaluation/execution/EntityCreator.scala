package ch.unibas.dmi.dbis.adam.evaluation.execution

import ch.unibas.dmi.dbis.adam.evaluation.client.EvaluationClient
import ch.unibas.dmi.dbis.adam.evaluation.config.IndexTypes

/**
  * adampro
  *
  * Ivan Giangreco
  * March 2016
  */
object EntityCreator {
  /**
    *
    * @param client
    * @param collectionSize
    * @param vectorSize
    * @param indexes
    * @param norm
    */
  def apply(client : EvaluationClient, collectionSize : Int, vectorSize : Int, indexes: Seq[IndexTypes.IndexType], norm : Int) : String = {
    val entityname = client.createEntity()
    client.generateRandomData(entityname, collectionSize, vectorSize)

    indexes.foreach {
      index =>
        client.createIndex(entityname, index, norm)
    }

    entityname
  }
}
