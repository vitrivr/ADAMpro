package ch.unibas.dmi.dbis.adam.evaluation.grpc


import ch.unibas.dmi.dbis.adam.evaluation.AdamParUtils
import ch.unibas.dmi.dbis.adam.http.grpc.AdamDefinitionGrpc.AdamDefinitionBlockingStub
import ch.unibas.dmi.dbis.adam.http.grpc.AdamSearchGrpc.{AdamSearchBlockingStub, AdamSearchStub}
import ch.unibas.dmi.dbis.adam.http.grpc._
import io.grpc.okhttp.OkHttpChannelBuilder
import io.grpc.{ManagedChannel, ManagedChannelBuilder}

import scala.util.Random

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  */
class RPCClient(channel: ManagedChannel, definer: AdamDefinitionBlockingStub, searcherBlocking: AdamSearchBlockingStub, searcher: AdamSearchStub) extends AdamParUtils{

  val eName = "silvan"
  val nTuples = 1e5.toInt
  val nDims = 128
  val noPart = 3
  val k = 100

  definer.listEntities(EmptyMessage())

  //Generate Entity
  val entityRes = time("Creating Entity")(definer.createEntity(CreateEntityMessage.apply(eName, Seq(FieldDefinitionMessage.apply("id", FieldDefinitionMessage.FieldType.LONG, true, true, true), FieldDefinitionMessage.apply("feature", FieldDefinitionMessage.FieldType.FEATURE, false, false, true)))))
  verifyRes(entityRes)

  //Generate Data
  val dataRes= definer.generateRandomData(GenerateRandomDataMessage(eName, nTuples, nDims))
  verifyRes(dataRes)

  //Generate index
  val indexMsg = IndexMessage(eName,"feature",IndexType.ecp,None,Map[String,String]())
  val indexRes = definer.index(indexMsg)
  verifyRes(indexRes)

  definer.repartitionEntityData(RepartitionMessage(eName,noPart,Seq("feature"),RepartitionMessage.PartitionOptions.REPLACE_EXISTING))

  val featureVector = FeatureVectorMessage().withDenseVector(DenseVectorMessage(Seq.fill(nDims)(Random.nextFloat())))
  val queryMsg = NearestNeighbourQueryMessage("feature",Some(featureVector),None,None,k,Map[String,String](),true,1 until noPart)
  val res = searcherBlocking.doQuery(QueryMessage(nnq = Some(queryMsg)))

  System.out.println(res.serializedSize+" Results!")

  def verifyRes (res: AckMessage) {
    if (!(res.code == AckMessage.Code.OK) ) {
      System.err.println ("Error during entity creation")
      System.err.println (res.message)
      System.exit (1)
    }
  }


}

object RPCClient {
  def apply(host: String, port: Int): RPCClient = {
    val channel = OkHttpChannelBuilder.forAddress(host, port).usePlaintext(true).asInstanceOf[ManagedChannelBuilder[_]].build()

    new RPCClient(
      channel,
      AdamDefinitionGrpc.blockingStub(channel),
      AdamSearchGrpc.blockingStub(channel),
      AdamSearchGrpc.stub(channel)
    )
  }
}