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

  var eName = "silvan"
  var nTuples = 1e5.toInt
  var nDims = 20
  var noPart = 3
  val k = 100

  val entityList = definer.listEntities(EmptyMessage())
  System.out.println(entityList.entities.toString())

  definer.dropEntity(EntityNameMessage(eName))
  generateEntity()
  definer.generateRandomData(GenerateRandomDataMessage(eName, nTuples, nDims))
  System.out.println(definer.count(EntityNameMessage(eName)).message)

  definer.dropEntity(EntityNameMessage(eName))
  generateEntity()
  nTuples = 1e6.toInt
  definer.generateRandomData(GenerateRandomDataMessage(eName, nTuples, nDims))
  System.out.println(definer.count(EntityNameMessage(eName)).message)

  nTuples = 1e4.toInt
  var count = 0
  while(count<5){
    count+=1
    definer.generateRandomData(GenerateRandomDataMessage(eName, nTuples, nDims))
  }

  generateIndex(IndexType.ecp)

  val repMessage = RepartitionMessage(eName,noPart,Seq("feature"),RepartitionMessage.PartitionOptions.REPLACE_EXISTING)
  time("Repartitioning Entity")(verifyRes(definer.repartitionEntityData(repMessage)))

  /**
    * Query-Testing
    */
  val featureVector = FeatureVectorMessage().withDenseVector(DenseVectorMessage(Seq.fill(nDims)(Random.nextFloat())))
  val queryMsg = NearestNeighbourQueryMessage("feature",Some(featureVector),None,None,k,Map[String,String](),true,1 until noPart)

  val resIndex = time("Performing nnQuery")(searcherBlocking.doQuery(QueryMessage(nnq = Some(queryMsg),from = Some(FromMessage(FromMessage.Source.Index("silvan_feature_ecp_0"))))))

  System.out.println("\n" + resIndex.serializedSize + " Results!\n")

  System.out.println(definer.count(EntityNameMessage(eName)).message)


  def generateIndex(indexType: IndexType): String = {
    val indexMsg = IndexMessage(eName,"feature",indexType,None,Map[String,String]())
    val indexRes = time("Building " + indexType.toString + " Index")(definer.index(indexMsg))
    verifyRes(indexRes)
    System.out.println(indexRes.message)
    indexRes.message
  }

  def generateEntity(): Unit = {
    val entityRes = time("Creating Entity")(definer.createEntity(CreateEntityMessage.apply(eName, Seq(FieldDefinitionMessage.apply("id", FieldDefinitionMessage.FieldType.LONG, true, true, true), FieldDefinitionMessage.apply("feature", FieldDefinitionMessage.FieldType.FEATURE, false, false, true)))))
    verifyRes(entityRes)
  }

  def getDistanceMsg : Option[DistanceMessage] = Some(DistanceMessage(DistanceMessage.DistanceType.minkowski,Map[String,String](("norm","2"))))


  def verifyRes (res: AckMessage) {
    if (!(res.code == AckMessage.Code.OK) ) {
      System.err.println ("Error during entity creation")
      System.err.println (res.message)
      System.exit (1)
    }
  }


  def dropAllEntities() = {
    val entityList = definer.listEntities(EmptyMessage())

    for(entity <- entityList.entities) {
      System.out.println("Dropping " + entity)
      val dropEnt = definer.dropEntity(EntityNameMessage(entity))
      verifyRes(dropEnt)
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