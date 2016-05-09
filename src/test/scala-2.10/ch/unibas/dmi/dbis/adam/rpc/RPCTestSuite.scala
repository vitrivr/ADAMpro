package ch.unibas.dmi.dbis.adam.rpc

import ch.unibas.dmi.dbis.adam.AdamTestBase
import ch.unibas.dmi.dbis.adam.config.AdamConfig
import ch.unibas.dmi.dbis.adam.entity.EntityHandler
import ch.unibas.dmi.dbis.adam.http.grpc.InsertMessage.TupleInsertMessage
import ch.unibas.dmi.dbis.adam.http.grpc._
import ch.unibas.dmi.dbis.adam.main.RPCStartup
import io.grpc.ManagedChannelBuilder
import io.grpc.stub.StreamObserver
import org.apache.log4j.Logger
import org.scalatest.concurrent.ScalaFutures

import scala.concurrent.duration._
import scala.util.Random

/**
  * adampro
  *
  * Ivan Giangreco
  * May 2016
  */
class RPCTestSuite extends AdamTestBase with ScalaFutures {
  val log = Logger.getLogger(getClass.getName)
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
        assert(EntityHandler.exists(entityname))
      }
    }

    /**
      *
      */
    scenario("insert feature data into entity") {
      withEntityName { entityname =>
        Given("an entity")
        val createEntityFuture =
          definition.createEntity(
            CreateEntityMessage(entityname,
              Seq(
                FieldDefinitionMessage("tid", FieldDefinitionMessage.FieldType.LONG, true),
                FieldDefinitionMessage("feature", FieldDefinitionMessage.FieldType.FEATURE)
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
        val tuples = (0 until ntuples).map(i => Map[String, String]("tid" -> Random.nextLong().toString,"feature" -> Seq.fill(10)(Random.nextFloat()).mkString(",")))
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
        val tuples = (0 until ntuples).map(i =>
          Map[String, String](
            "tid" -> Random.nextLong.toString,
            "featurefield" -> Seq.fill(10)(Random.nextFloat()).mkString(","),
            "intfield" -> Random.nextInt(10).toString,
            "stringfield" -> getRandomName(10)
          )
        )
        requestObserver.onNext(InsertMessage(entityname, tuples.map(tuple => TupleInsertMessage(tuple))))
        requestObserver.onCompleted()

        Then("the tuples should eventually be available")
        eventually {
          assert(definition.count(EntityNameMessage(entityname)).message.toInt == ntuples)
        }
      }
    }
  }
}