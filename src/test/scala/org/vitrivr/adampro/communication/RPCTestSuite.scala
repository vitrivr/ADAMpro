package org.vitrivr.adampro.communication

import org.vitrivr.adampro.AdamTestBase
import org.vitrivr.adampro.data.datatypes.AttributeTypes
import org.vitrivr.adampro.data.entity.{AttributeDefinition, Entity}
import org.vitrivr.adampro.grpc.grpc.{AdamDefinitionGrpc, AdamSearchGrpc}
import io.grpc.ManagedChannelBuilder
import io.grpc.stub.StreamObserver
import org.scalatest.concurrent.ScalaFutures
import org.vitrivr.adampro.grpc.grpc.BooleanQueryMessage.WhereMessage
import org.vitrivr.adampro.grpc.grpc.InsertMessage.TupleInsertMessage
import org.vitrivr.adampro.grpc.grpc._
import org.vitrivr.adampro.process.RPCStartup

import scala.concurrent.duration._
import scala.util.Random

/**
  * adampro
  *
  * Ivan Giangreco
  * May 2016
  */
class RPCTestSuite extends AdamTestBase with ScalaFutures {
  private val MAX_WAITING_TIME: Duration = 100.seconds
  new Thread(new RPCStartup(ac.config.grpcPort)).start
  Thread.sleep(5000)
  //wait for RPC server to startup

  val channel = ManagedChannelBuilder.forAddress("localhost", ac.config.grpcPort).usePlaintext(true).asInstanceOf[ManagedChannelBuilder[_]].build()
  val definition = AdamDefinitionGrpc.blockingStub(channel)
  val definitionNb = AdamDefinitionGrpc.stub(channel)
  val search = AdamSearchGrpc.blockingStub(channel)
  val searchNb = AdamSearchGrpc.stub(channel)

  def ntuples() = Random.nextInt(5)
  def ndims() = 20 + Random.nextInt(80)


  feature("data definition") {
    /**
      *
      */
    scenario("create entity") {
      withEntityName { entityname =>
        When("a new random entity (without any metadata) is created")
        val createEntityFuture =
          definition.createEntity(
            CreateEntityMessage(entityname,
              Seq(
                AttributeDefinitionMessage("tid", AttributeType.LONG),
                AttributeDefinitionMessage("feature", AttributeType.VECTOR)
              )))
        Then("the entity should exist")
        assert(Entity.exists(entityname))
      }
    }

    /**
      *
      */
    scenario("insert feature data into entity") {
      withEntityName { entityname =>
        Given("an entity")
        val entity = Entity.create(entityname, Seq(new AttributeDefinition("tid", AttributeTypes.LONGTYPE), new AttributeDefinition("feature", AttributeTypes.VECTORTYPE)))
        assert(entity.isSuccess)

        val requestObserver: StreamObserver[InsertMessage] = definitionNb.streamInsert(new StreamObserver[AckMessage]() {
          def onNext(ack: AckMessage) {}

          def onError(t: Throwable): Unit = {
            assert(false) //should never happen
          }

          def onCompleted() {}
        })

        val tuplesInsert = ntuples()
        val dimsInsert = ndims()

        When("tuples are inserted")
        val tuples = (0 until tuplesInsert)
          .map(i => Map[String, DataMessage](
            "tid" -> DataMessage().withLongData(Random.nextLong()),
            "feature" -> DataMessage().withVectorData(VectorMessage().withDenseVector(DenseVectorMessage(Seq.fill(dimsInsert)(Random.nextFloat()))))
          ))

        requestObserver.onNext(InsertMessage(entityname, tuples.map(tuple => TupleInsertMessage(tuple))))
        requestObserver.onCompleted()

        Then("the tuples should eventually be available")
        eventually {
          Thread.sleep(500)
          assert(definition.count(EntityNameMessage(entityname)).message.toInt == tuplesInsert)
        }
      }
    }


    /**
      *
      */
    scenario("insert data (feature and metadata) into entity") {
      withEntityName { entityname =>
        Given("an entity")
        val entity = Entity.create(entityname, Seq(
          new AttributeDefinition("tid", AttributeTypes.LONGTYPE), new AttributeDefinition("feature", AttributeTypes.VECTORTYPE),
          new AttributeDefinition("stringfield", AttributeTypes.STRINGTYPE), new AttributeDefinition("intfield", AttributeTypes.INTTYPE)
        ))

        assert(entity.isSuccess)

        val requestObserver: StreamObserver[InsertMessage] = definitionNb.streamInsert(new StreamObserver[AckMessage]() {
          def onNext(ack: AckMessage) {}

          def onError(t: Throwable): Unit = {
            assert(false) //should never happen
          }

          def onCompleted() {}
        })

        val tuplesInsert = ntuples()
        val dimsInsert = ndims()

        When("tuples are inserted")
        val tuples = (0 until tuplesInsert)
          .map(i => Map[String, DataMessage](
            "tid" -> DataMessage().withLongData(Random.nextLong()),
            "vectorfield" -> DataMessage().withVectorData(VectorMessage().withDenseVector(DenseVectorMessage(Seq.fill(dimsInsert)(Random.nextFloat())))),
            "intfield" -> DataMessage().withIntData(Random.nextInt(10)),
            "stringfield" -> DataMessage().withStringData(getRandomName(10))
          ))
        requestObserver.onNext(InsertMessage(entityname, tuples.map(tuple => TupleInsertMessage(tuple))))
        requestObserver.onCompleted()

        Then("the tuples should eventually be available")
        eventually {
          Thread.sleep(500)
          val response = definition.count(EntityNameMessage(entityname))
          assert(response.code == AckMessage.Code.OK)
          assert(response.message.toInt == tuplesInsert)
        }
      }
    }


    scenario("insert data into entity and query") {
      withEntityName { entityname =>
        Given("an entity")
        val createEntityFuture = definition.createEntity(CreateEntityMessage(entityname,
          Seq(
            AttributeDefinitionMessage("tid", AttributeType.LONG),
            AttributeDefinitionMessage("stringfield", AttributeType.STRING),
            AttributeDefinitionMessage("intfield", AttributeType.INT),
            AttributeDefinitionMessage("vectorfield", AttributeType.VECTOR)
          )))

        val requestObserver: StreamObserver[InsertMessage] = definitionNb.streamInsert(new StreamObserver[AckMessage]() {
          def onNext(ack: AckMessage) {}

          def onError(t: Throwable): Unit = {
            assert(false) //should never happen
          }

          def onCompleted() {}
        })

        val tuplesInsert = ntuples()
        val dimsInsert = ndims()

        When("tuples are inserted")
        val tuples = (0 until tuplesInsert)
          .map(i => Map[String, DataMessage](
            "tid" -> DataMessage().withLongData(Random.nextLong()),
            "vectorfield" -> DataMessage().withVectorData(VectorMessage().withDenseVector(DenseVectorMessage(Seq.fill(dimsInsert)(Random.nextFloat())))),
            "intfield" -> DataMessage().withIntData(Random.nextInt(10)),
            "stringfield" -> DataMessage().withStringData(getRandomName(10))
          ))
        requestObserver.onNext(InsertMessage(entityname, tuples.map(tuple => TupleInsertMessage(tuple))))
        requestObserver.onCompleted()

        Then("the tuples should eventually be available")
        eventually {
          Thread.sleep(500)
          val inserted = definition.count(EntityNameMessage(entityname)).message.toInt
          assert(inserted == tuplesInsert)

          val results = search.doQuery(QueryMessage(queryid = "", from = Some(FromMessage().withEntity(entityname)), bq = Some(BooleanQueryMessage(Seq(WhereMessage("tid", Seq(DataMessage().withLongData(tuples.head("tid").getLongData))))))))
          assert(results.ack.get.code == AckMessage.Code.OK)
        }

      }
    }
  }
}