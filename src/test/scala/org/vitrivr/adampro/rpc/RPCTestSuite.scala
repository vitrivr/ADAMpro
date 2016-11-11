package org.vitrivr.adampro.rpc

import org.vitrivr.adampro.AdamTestBase
import org.vitrivr.adampro.config.AdamConfig
import org.vitrivr.adampro.datatypes.FieldTypes
import org.vitrivr.adampro.entity.{AttributeDefinition, Entity}
import org.vitrivr.adampro.grpc.grpc.{AdamSearchGrpc, AdamDefinitionGrpc}
import org.vitrivr.adampro.main.RPCStartup
import io.grpc.ManagedChannelBuilder
import io.grpc.stub.StreamObserver
import org.scalatest.concurrent.ScalaFutures
import org.vitrivr.adampro.grpc.grpc.BooleanQueryMessage.WhereMessage
import org.vitrivr.adampro.grpc.grpc.InsertMessage.TupleInsertMessage
import org.vitrivr.adampro.grpc.grpc._

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
                AttributeDefinitionMessage("tid", AttributeType.LONG, true),
                AttributeDefinitionMessage("feature", AttributeType.FEATURE)
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

        val requestObserver: StreamObserver[InsertMessage] = definitionNb.streamInsert(new StreamObserver[AckMessage]() {
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
          Thread.sleep(500)
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

        val requestObserver: StreamObserver[InsertMessage] = definitionNb.streamInsert(new StreamObserver[AckMessage]() {
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
          Thread.sleep(500)
          val response = definition.count(EntityNameMessage(entityname))
          assert(response.code == AckMessage.Code.OK)
          assert(response.message.toInt == ntuples)
        }
      }
    }


    scenario("insert data into entity and query") {
      withEntityName { entityname =>
        Given("an entity")
        val createEntityFuture = definition.createEntity(CreateEntityMessage(entityname,
          Seq(
            AttributeDefinitionMessage("tid", AttributeType.LONG, true),
            AttributeDefinitionMessage("stringfield", AttributeType.STRING),
            AttributeDefinitionMessage("intfield", AttributeType.INT),
            AttributeDefinitionMessage("featurefield", AttributeType.FEATURE)
          )))

        val requestObserver: StreamObserver[InsertMessage] = definitionNb.streamInsert(new StreamObserver[AckMessage]() {
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
          Thread.sleep(500)
          val inserted = definition.count(EntityNameMessage(entityname)).message.toInt == ntuples
          assert(inserted)

          if (inserted) {
            val results = search.doQuery(QueryMessage(queryid = "", from = Some(FromMessage().withEntity(entityname)), bq = Some(BooleanQueryMessage(Seq(WhereMessage("tid", Seq(DataMessage().withLongData(tuples.head("tid").getLongData))))))))
            assert(results.ack.get.code == AckMessage.Code.OK)
          }
        }

      }
    }




    /**
      *
      */
    scenario("insert data with serial pk") {
      withEntityName { entityname =>
        Given("an entity with a serial pk")
        val res = Entity.create(entityname, Seq(AttributeDefinition("idfield", FieldTypes.SERIALTYPE, true), AttributeDefinition("featurefield", FieldTypes.FEATURETYPE, false)))

        if(res.isFailure){
          throw res.failed.get
        }


        (0 to Random.nextInt(100)).foreach{ i =>
          val nextPkValue = definition.getNextPkValue(EntityNameMessage(entityname)).message.toLong

          assert(nextPkValue == i.toLong)
        }
      }
    }
  }

  feature("data import") {
    /**
      *
      */
    scenario("export and import proto-based file") {
      withEntityName { entityname =>
        val NTUPLES = 1000

        definition.createEntity(CreateEntityMessage(entityname, Seq(AttributeDefinitionMessage("id", AttributeType.LONG, true), AttributeDefinitionMessage("feature", AttributeType.FEATURE))))
        definition.generateRandomData(GenerateRandomDataMessage(entityname, NTUPLES, Map("fv-dimensions" -> 100.toString)))

        val count = definition.count(EntityNameMessage(entityname)).message.toInt
        assert(count == NTUPLES)

        val exported = definition.exportDataFile(EntityNameMessage(entityname))

        definition.dropEntity(EntityNameMessage(entityname))

        definition.importDataFile(ImportDataFileMessage().withDefinitionfile(exported.definitionfile).withDatafile(exported.datafile))

        val countAfterImport = definition.count(EntityNameMessage(entityname)).message.toInt

        log.info("found " + countAfterImport + " tuples")

        assert(countAfterImport == NTUPLES)
      }
    }

  }
}