package org.vitrivr.adampro.rpc.datastructures

import org.vitrivr.adampro.grpc.grpc.{Optimizer, QueryMessage, QuerySimulationMessage}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * May 2017
  */
case class RPCSimulationQueryObject(qo : RPCGenericQueryObject, optimizer : Optimizer) {

  /**
    *
    * @return
    */
  def buildQueryMessage: QuerySimulationMessage = {
    QuerySimulationMessage(qo.getOption("entityname").get, qo.nnq, optimizer)
  }
}
