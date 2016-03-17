package ch.unibas.dmi.dbis.adam.index

import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.datatypes.feature.{FeatureVectorWrapper, FeatureVectorWrapperUDT}
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.index.structures.ecp.ECPIndexer
import ch.unibas.dmi.dbis.adam.index.structures.lsh.LSHIndexer
import ch.unibas.dmi.dbis.adam.index.structures.sh.SHIndexer
import ch.unibas.dmi.dbis.adam.index.structures.va.{VAVIndexer, VAFIndexer}
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.query.distance.EuclideanDistance
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructField, StructType}
import org.scalatest.concurrent.{Eventually, IntegrationPatience}
import org.scalatest.{FeatureSpec, GivenWhenThen}

import scala.util.Random

/**
  * adamtwo
  *
  * Ivan Giangreco
  * March 2016
  */
class IndexTestSuite extends FeatureSpec with GivenWhenThen with Eventually with IntegrationPatience {
  SparkStartup

  val ntuples = Random.nextInt(1000)
  val ndims = 100

  def getRandomName(len: Int = 10) = {
    val sb = new StringBuilder(len)
    val ab = "abcdefghijklmnopqrstuvwxyz"
    for (i <- 0 until len) {
      sb.append(ab(Random.nextInt(ab.length)))
    }
    sb.toString
  }

  def createSimpleEntity(): String = {
    val entityname = getRandomName()
    Entity.create(entityname)

    val schema = StructType(Seq(
      StructField(FieldNames.featureColumnName, new FeatureVectorWrapperUDT, false)
    ))

    val rdd = SparkStartup.sc.parallelize((0 until ntuples).map(id =>
      Row(new FeatureVectorWrapper(Seq.fill(ndims)(Random.nextFloat())))
    ))

    val data = SparkStartup.sqlContext.createDataFrame(rdd, schema)

    Entity.insertData(entityname, data)

    entityname
  }


  feature("index creation") {
    scenario("create and drop indexes") {
      Given("an entity without metadata and an index")
      val entityname = createSimpleEntity()
      val entity = Entity.load(entityname)
      val index = Index.createIndex(entity, ECPIndexer(EuclideanDistance))

      assert(Index.exists(index.indexname))

      When("dropping the index")
      Index.drop(index.indexname)

      Then("the index should be dropped")
      assert(!Index.exists(index.indexname))

      //clean up
      Entity.drop(entityname)
    }

    scenario("create and drop indexes by dropping entity") {
      Given("an entity without metadata and an index")
      val entityname = createSimpleEntity()
      val entity = Entity.load(entityname)
      val index = Index.createIndex(entity, ECPIndexer(EuclideanDistance))

      assert(Index.exists(index.indexname))

      When("dropping the index")
      Entity.drop(entityname)

      Then("the index should be dropped")
      assert(!Index.exists(index.indexname))
    }


    scenario("create eCP index") {
      Given("an entity without metadata")
      val entityname = createSimpleEntity()
      val entity = Entity.load(entityname)

      When("an eCP index is created")
      val index = Index.createIndex(entity, ECPIndexer(EuclideanDistance))

      Then("the index has been created")
      assert(Index.exists(index.indexname))
      assert(index.confidence <= 1)
      assert(index.count == entity.count)
      //TODO: better checks of eCP index

      //clean up
      Entity.drop(entityname)
    }


    scenario("create LSH index") {
      Given("an entity without metadata")
      val entityname = createSimpleEntity()
      val entity = Entity.load(entityname)

      When("an LSH index is created")
      val index = Index.createIndex(entity, LSHIndexer(EuclideanDistance))

      Then("the index has been created")
      assert(Index.exists(index.indexname))
      assert(index.confidence <= 1)
      assert(index.count == entity.count)
      //TODO: better checks of LSH index

      //clean up
      Entity.drop(entityname)
    }


    scenario("create SH index") {
      Given("an entity without metadata")
      val entityname = createSimpleEntity()
      val entity = Entity.load(entityname)

      When("an SH index is created")
      val index = Index.createIndex(entity, SHIndexer(ndims))

      Then("the index has been created")
      assert(Index.exists(index.indexname))
      assert(index.confidence <= 1)
      assert(index.count == entity.count)
      //TODO: better checks of SH index

      //clean up
      Entity.drop(entityname)
    }


    scenario("create VA-File (fixed) index") {
      Given("an entity without metadata")
      val entityname = createSimpleEntity()
      val entity = Entity.load(entityname)

      When("an VA-File index is created")
      val index = Index.createIndex(entity, VAFIndexer(EuclideanDistance))

      Then("the index has been created")
      assert(Index.exists(index.indexname))
      assert(index.confidence == 1)
      assert(index.count == entity.count)
      //TODO: better checks of VA-File (fixed) index

      //clean up
      Entity.drop(entityname)
    }

    scenario("create VA-File (variable) index") {
      Given("an entity without metadata")
      val entityname = createSimpleEntity()
      val entity = Entity.load(entityname)

      When("an VA-File index is created")
      val index = Index.createIndex(entity, VAVIndexer(ndims, EuclideanDistance))

      Then("the index has been created")
      assert(Index.exists(index.indexname))
      assert(index.confidence == 1)
      assert(index.count == entity.count)
      //TODO: better checks of VA-File (variable) index

      //clean up
      Entity.drop(entityname)
    }
  }
}
