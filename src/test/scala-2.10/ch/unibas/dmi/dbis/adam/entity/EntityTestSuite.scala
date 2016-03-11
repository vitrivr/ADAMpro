package ch.unibas.dmi.dbis.adam.entity

import java.sql.DriverManager
import java.util.UUID

import ch.unibas.dmi.dbis.adam.config.AdamConfig
import ch.unibas.dmi.dbis.adam.entity.FieldTypes.FieldType
import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.{FeatureSpec, GivenWhenThen}

import scala.collection.mutable.ListBuffer

/**
  * adamtwo
  *
  * Ivan Giangreco
  * March 2016
  */
class EntityTestSuite extends FeatureSpec with GivenWhenThen with SharedSparkContext {

  def fixture =
    new {
      val connection = {
        Class.forName("org.postgresql.Driver").newInstance
        DriverManager.getConnection(AdamConfig.jdbcUrl, AdamConfig.jdbcUser, AdamConfig.jdbcPassword)
      }
    }


  feature("data definition") {
    scenario("create an entity") {
      Given("no entities have been created so far")
      assert(Entity.list().isEmpty)

      When("a new random entity (without any metadata) is created")
      val entityname = UUID.randomUUID().toString.substring(0, 8)
      Entity.create(entityname)

      Then("the entity, and only the entity should exist")
      val entities = Entity.list()
      assert(entities.length == 1)
      assert(entities.head == entityname)
    }

    scenario("drop an existing entity") {
      Given("there exists one entity")
      val entityname = UUID.randomUUID().toString.substring(0, 8)
      Entity.create(entityname)
      assert(Entity.list() == 1)

      When("the enetity is dropped")
      Entity.drop(entityname)

      Then("no entity should exist")
      assert(Entity.list().length == 0)
    }

    scenario("create an entity with metadata") {
      Given("no entities have been created so far")
      assert(Entity.list().isEmpty)

      val fieldTemplate = Seq(
        ("stringfield", FieldTypes.STRINGTYPE, "character varying"),
        ("floatfield", FieldTypes.FLOATTYPE, "real"),
        ("doublefield", FieldTypes.DOUBLETYPE, "double precision"),
        ("intfield", FieldTypes.INTTYPE, "integer"),
        ("longfield", FieldTypes.LONGTYPE, "bigint"),
        ("booleanfield", FieldTypes.BOOLEANTYPE, "boolean")
      )

      When("a new random entity with metadata is created")
      val entityname = UUID.randomUUID().toString.substring(0, 8)
      val fields: Map[String, FieldType] = fieldTemplate.map(ft => (ft._1, ft._2)).toMap
      Entity.create(entityname, Option(fields))

      Then("the entity, and only the entity should exist")
      val entities = Entity.list()
      assert(entities.length == 1)
      assert(entities.head == entityname)

      And("The metadata table should have been created")
      val connection = fixture.connection
      val result = connection.createStatement().executeQuery("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = " + entityname)

      val lb = new ListBuffer[(String, String)]()
      while (result.next) {
        lb.+=((result.getString(1), result.getString(2)))
      }

      val dbNames = lb.toList.toMap
      val templateNames = fieldTemplate.map(ft => (ft._1, ft._3)).toMap

      assert(dbNames.size == templateNames.size)

      val keys = dbNames.keySet

      And("The metadata table should contain the same columns (with the corresponding data type)")
      assert(keys.forall({ key => dbNames(key) == templateNames(key) }))
    }


    scenario("drop an entity with metadata") {
      Given("an entity with metadata")
      val entityname = UUID.randomUUID().toString.substring(0, 8)
      val fields: Map[String, FieldType] = Map[String, FieldType](("stringfield" -> FieldTypes.STRINGTYPE))
      Entity.create(entityname, Option(fields))

      val connection = fixture.connection
      val preResult = connection.createStatement().executeQuery("SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = " + entityname)
      var tableCount = 0
      while (preResult.next) {
        tableCount += 1
      }

      When("the entity is dropped")
      Entity.drop(entityname)

      Then("the metadata entity is dropped as well")
      val postResult = connection.createStatement().executeQuery("SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = " + entityname)
      while (postResult.next) {
        tableCount -= 1
      }

      assert(tableCount == 0)
    }
  }


  /*feature("data manipulation") {
    scenario("insert data in an entity without metadata") {
      Given("an entity without metadata")
      val entityname = UUID.randomUUID().toString.substring(0, 8)
      Entity.create(entityname)

      When("data is inserted")

      Entity.insertData(entityname)

      Then("the data should be available")
      assert(tableCount == 0)

      When("data with metadata is inserted")


      Then("the data is available without metadata")
    }

    scenario("insert data in an entity with metadata") {
      Given("an entity without metadata")
      val entityname = UUID.randomUUID().toString.substring(0, 8)
      Entity.create(entityname, Option(fields))
      val fieldTemplate = Seq(
        ("stringfield", FieldTypes.STRINGTYPE, "character varying"),
        ("floatfield", FieldTypes.FLOATTYPE, "real"),
        ("doublefield", FieldTypes.DOUBLETYPE, "double precision"),
        ("intfield", FieldTypes.INTTYPE, "integer"),
        ("longfield", FieldTypes.LONGTYPE, "bigint"),
        ("booleanfield", FieldTypes.BOOLEANTYPE, "boolean")
      )

      When("the entity is dropped")
      Entity.drop(entityname)

      Then("the metadata entity is dropped as well")
      val postResult = connection.createStatement().executeQuery("SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = " + entityname)
      while (postResult.next) {
        tableCount -= 1
      }

      assert(tableCount == 0)
    }


  }*/
}