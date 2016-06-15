package ch.unibas.dmi.dbis.adam.evaluation.grpc


import java.io.File

import ch.unibas.dmi.dbis.adam.evaluation.AdamParEvalUtils
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
class RPCClient(channel: ManagedChannel, definer: AdamDefinitionBlockingStub, searcherBlocking: AdamSearchBlockingStub, searcher: AdamSearchStub) extends AdamParEvalUtils{

  val eName = "silvan"+Math.abs(Random.nextInt())
  val nTuples = 1e5.toInt
  val nDims = 10
  val noPart = 3
  val k = 100

  generateNewEntity()

  if(definer.count(EntityNameMessage(eName)).message.toInt!=nTuples){
    System.err.println("Data generation failed")
    System.exit(1)
  }

  val ecpName = generateIndex(IndexType.ecp)
  val vafName = generateIndex(IndexType.vaf)
  if(!ecpName.equals(eName+"_feature_ecp_0")){
    System.err.println(ecpName)
    System.err.println(eName+"_feature_ecp_0")
  }

  System.out.println(ecpName + " | "+vafName)

  val repMessage = RepartitionMessage(eName,noPart,Seq("feature"),RepartitionMessage.PartitionOptions.REPLACE_EXISTING)
  time("Repartitioning Entity")(verifyRes(definer.repartitionEntityData(repMessage)))

  /**
    * Query-Testing
    */
  val ecpScan = time("Performing nnQuery")(searcherBlocking.doQuery(QueryMessage(nnq = Some(randomQueryMessage),from = Some(FromMessage(FromMessage.Source.Index(ecpName))))))
  System.out.println("\n" + ecpScan.responses.head.results.size + " ECP - Results! \n")


  val vafScan = time("Performing nnQuery")(searcherBlocking.doQuery(QueryMessage(nnq = Some(randomQueryMessage),from = Some(FromMessage(FromMessage.Source.Index(vafName))))))
  System.out.println("\n" + vafScan.responses.head.results.size + " VAF - Results! \n")


  def randomQueryMessage = NearestNeighbourQueryMessage("feature",Some(FeatureVectorMessage().withDenseVector(DenseVectorMessage(Seq.fill(nDims)(Random.nextFloat())))),None,getDistanceMsg,k,Map[String,String](),true,1 until noPart)

  def generateNewEntity(): Unit ={
    val entities = definer.listEntities(EmptyMessage())
    System.out.println("Current entities: "+entities.entities.toString())
    //verifyRes(definer.dropEntity(EntityNameMessage(eName)))
    dropAllEntities()
    generateEntity()
    val newEntities = definer.listEntities(EmptyMessage())
    System.out.println("New entities: "+newEntities.entities.toString())

    time("Generating Random Data")(verifyRes(definer.generateRandomData(GenerateRandomDataMessage(eName, nTuples, nDims))))
  }

  def generateEntity(): Unit = {
    val entityRes = time("Creating Entity")(definer.createEntity(CreateEntityMessage.apply(eName, Seq(FieldDefinitionMessage.apply("id", FieldDefinitionMessage.FieldType.LONG, true, true, true), FieldDefinitionMessage("feature", FieldDefinitionMessage.FieldType.FEATURE,false,false,true)))))
    verifyRes(entityRes)
  }

  def verifyRes (res: AckMessage) {
    if (!(res.code == AckMessage.Code.OK) ) {
      System.err.println ("Error during entity creation")
      System.err.println (res.message)
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

  def getDistanceMsg : Option[DistanceMessage] = Some(DistanceMessage(DistanceMessage.DistanceType.minkowski,Map[String,String](("norm","2"))))

  def generateIndex(indexType: IndexType): String = {
    val indexMsg = IndexMessage(eName,"feature",indexType,getDistanceMsg,Map[String,String]())
    val indexRes = time("Building " + indexType.toString + " Index")(definer.index(indexMsg))
    verifyRes(indexRes)
    indexRes.message
  }

  def appendToResults(tuples:Int, dimensions:Int, partitions: Int, index: String, time: Long, k: Int): Unit ={
    val pw = new java.io.PrintWriter(new File("results.txt"))
    try pw.println(index+","+tuples+","+dimensions+","+partitions+","+time+","+k) finally pw.close()
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