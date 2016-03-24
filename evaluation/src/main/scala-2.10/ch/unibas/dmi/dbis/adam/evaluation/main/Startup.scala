package ch.unibas.dmi.dbis.adam.evaluation.main

import ch.unibas.dmi.dbis.adam.evaluation.client.EvaluationClient
import ch.unibas.dmi.dbis.adam.evaluation.config.EvaluationConfig
import ch.unibas.dmi.dbis.adam.evaluation.execution.{ProgressiveQueryExperiment, EntityCreator, Evaluator, SimpleQueryExperiment}
import ch.unibas.dmi.dbis.adam.http.grpc.{AdamSearchGrpc, AdamDefinitionGrpc}
import io.grpc.ManagedChannelBuilder

/**
  * adampro
  *
  * Ivan Giangreco
  * March 2016
  */
object Startup {
  def main(args: Array[String]) {
    val client = Startup("localhost", 5890)

    try {
      EvaluationConfig.collectionSizes.foreach { collectionSize =>
        EvaluationConfig.vectorSizes.foreach { vectorSize =>
          val entityname = EntityCreator(client, collectionSize, vectorSize, EvaluationConfig.indexes, EvaluationConfig.norm)
          Evaluator.enqueue(new SimpleQueryExperiment(client, entityname, collectionSize, vectorSize, EvaluationConfig.indexes, EvaluationConfig.k, EvaluationConfig.numExperiments))
          Evaluator.enqueue(new ProgressiveQueryExperiment(client, entityname, collectionSize, vectorSize, EvaluationConfig.indexes, EvaluationConfig.k, EvaluationConfig.numExperiments))
        }
      }
    } finally {
      client.shutdown()
    }
  }

  def apply(host: String, port: Int): EvaluationClient = {
    val channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext(true).asInstanceOf[ManagedChannelBuilder[_]].build()

    new EvaluationClient(
      channel,
      AdamDefinitionGrpc.blockingStub(channel),
      AdamSearchGrpc.blockingStub(channel),
      AdamSearchGrpc.stub(channel)
    )
  }
}