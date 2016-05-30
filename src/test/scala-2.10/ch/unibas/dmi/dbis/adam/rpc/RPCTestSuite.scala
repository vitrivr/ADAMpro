package ch.unibas.dmi.dbis.adam.rpc

import ch.unibas.dmi.dbis.adam.AdamTestBase
import ch.unibas.dmi.dbis.adam.config.AdamConfig
import ch.unibas.dmi.dbis.adam.datatypes.FieldTypes
import ch.unibas.dmi.dbis.adam.entity.{Entity, AttributeDefinition}
import ch.unibas.dmi.dbis.adam.http.grpc.BooleanQueryMessage.WhereMessage
import ch.unibas.dmi.dbis.adam.http.grpc.InsertMessage.TupleInsertMessage
import ch.unibas.dmi.dbis.adam.http.grpc._
import ch.unibas.dmi.dbis.adam.main.RPCStartup
import io.grpc.ManagedChannelBuilder
import io.grpc.stub.StreamObserver
import org.scalatest.concurrent.ScalaFutures
import ch.unibas.dmi.dbis.adam.main.SparkStartup.Implicits._

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
  new Thread(new RPCStartup()).start
  Thread.sleep(5000)
  //wait for RPC server to startup

  val channel = ManagedChannelBuilder.forAddress("localhost", AdamConfig.grpcPort).usePlaintext(true).asInstanceOf[ManagedChannelBuilder[_]].build()
  val definition = AdamDefinitionGrpc.blockingStub(channel)
  val definitionNb = AdamDefinitionGrpc.stub(channel)
  val search = AdamSearchGrpc.blockingStub(channel)
  val searchNb = AdamSearchGrpc.stub(channel)


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
                FieldDefinitionMessage("tid", FieldDefinitionMessage.FieldType.LONG, true),
                FieldDefinitionMessage("feature", FieldDefinitionMessage.FieldType.FEATURE)
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
        val entity = Entity.create(entityname, Seq(AttributeDefinition("tid", FieldTypes.LONGTYPE, true), AttributeDefinition("feature", FieldTypes.FEATURETYPE)))
        assert(entity.isSuccess)

        val requestObserver: StreamObserver[InsertMessage] = definitionNb.insert(new StreamObserver[AckMessage]() {
          def onNext(ack: AckMessage) {}

          def onError(t: Throwable): Unit = {
            assert(false) //should never happen
          }

          def onCompleted() {}
        })

        val ntuples = 10

        When("tuples are inserted")
        val tuples = (0 until ntuples)
          .map(i => Map[String, DataMessage](
            "tid" -> DataMessage().withLongData(Random.nextLong()),
            "feature" -> DataMessage().withFeatureData(FeatureVectorMessage().withDenseVector(DenseVectorMessage(Seq.fill(10)(Random.nextFloat()))))
          ))

        requestObserver.onNext(InsertMessage(entityname, tuples.map(tuple => TupleInsertMessage(tuple))))
        requestObserver.onCompleted()

        Then("the tuples should eventually be available")
        eventually {
          assert(definition.count(EntityNameMessage(entityname)).message.toInt == ntuples)
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
          AttributeDefinition("tid", FieldTypes.LONGTYPE, true), AttributeDefinition("feature", FieldTypes.FEATURETYPE),
          AttributeDefinition("stringfield", FieldTypes.STRINGTYPE), AttributeDefinition("intfield", FieldTypes.INTTYPE)
        ))

        assert(entity.isSuccess)

        val requestObserver: StreamObserver[InsertMessage] = definitionNb.insert(new StreamObserver[AckMessage]() {
          def onNext(ack: AckMessage) {}

          def onError(t: Throwable): Unit = {
            assert(false) //should never happen
          }

          def onCompleted() {}
        })

        val ntuples = 10

        When("tuples are inserted")
        val tuples = (0 until ntuples)
          .map(i => Map[String, DataMessage](
            "tid" -> DataMessage().withLongData(Random.nextLong()),
            "featurefield" -> DataMessage().withFeatureData(FeatureVectorMessage().withDenseVector(DenseVectorMessage(Seq.fill(10)(Random.nextFloat())))),
            "intfield" -> DataMessage().withIntData(Random.nextInt(10)),
            "stringfield" -> DataMessage().withStringData(getRandomName(10))
          ))
        requestObserver.onNext(InsertMessage(entityname, tuples.map(tuple => TupleInsertMessage(tuple))))
        requestObserver.onCompleted()

        Then("the tuples should eventually be available")
        eventually {
          assert(definition.count(EntityNameMessage(entityname)).message.toInt == ntuples)
        }
      }
    }


    scenario("insert data into entity and query") {
      withEntityName { entityname =>
        Given("an entity")
        val createEntityFuture = definition.createEntity(CreateEntityMessage(entityname,
          Seq(
            FieldDefinitionMessage("tid", FieldDefinitionMessage.FieldType.LONG, true),
            FieldDefinitionMessage("stringfield", FieldDefinitionMessage.FieldType.STRING),
            FieldDefinitionMessage("intfield", FieldDefinitionMessage.FieldType.INT),
            FieldDefinitionMessage("featurefield", FieldDefinitionMessage.FieldType.FEATURE)
          )))

        val requestObserver: StreamObserver[InsertMessage] = definitionNb.insert(new StreamObserver[AckMessage]() {
          def onNext(ack: AckMessage) {}

          def onError(t: Throwable): Unit = {
            assert(false) //should never happen
          }

          def onCompleted() {}
        })

        val ntuples = 10

        When("tuples are inserted")
        val tuples = (0 until ntuples)
          .map(i => Map[String, DataMessage](
            "tid" -> DataMessage().withLongData(Random.nextLong()),
            "featurefield" -> DataMessage().withFeatureData(FeatureVectorMessage().withDenseVector(DenseVectorMessage(Seq.fill(10)(Random.nextFloat())))),
            "intfield" -> DataMessage().withIntData(Random.nextInt(10)),
            "stringfield" -> DataMessage().withStringData(getRandomName(10))
          ))
        requestObserver.onNext(InsertMessage(entityname, tuples.map(tuple => TupleInsertMessage(tuple))))
        requestObserver.onCompleted()

        Then("the tuples should eventually be available")
        eventually {
          val inserted = definition.count(EntityNameMessage(entityname)).message.toInt == ntuples
          assert(inserted)

          if(inserted){
            val results = search.doQuery(QueryMessage(queryid = "", from = Some(FromMessage().withEntity(entityname)), bq = Some(BooleanQueryMessage(Seq(WhereMessage("tid", tuples.head("tid").getLongData.toString))))))
            assert(results.ack.get.code == AckMessage.Code.OK)
          }
        }

      }
    }

  }
}