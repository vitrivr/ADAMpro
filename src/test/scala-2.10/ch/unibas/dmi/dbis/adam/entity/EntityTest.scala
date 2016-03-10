package ch.unibas.dmi.dbis.adam.entity

import java.util.UUID

import ch.unibas.dmi.dbis.adam.entity.FieldTypes.FieldType
import com.holdenkarau.spark.testing.SharedSparkContext
import org.scalatest.FlatSpec

/**
  * adamtwo
  *
  * Ivan Giangreco
  * March 2016
  */
class EntityTest extends FlatSpec with SharedSparkContext {
  def fixture =
    new {
      def entityname = UUID.randomUUID().toString.subSequence(0, 8).toString
    }


  "entities" should "not exist" in {
    assert(Entity.list().isEmpty)
  }

  "a random test entity" should "not exist" in {
    assert(!Entity.exists(fixture.entityname))
  }

  "a random test entity without metadata" should "be creatable and dropped" in {
    val entityname = fixture.entityname
    Entity.create(entityname)
    assert(Entity.exists(entityname))
    Entity.drop(entityname)
    assert(!Entity.exists(entityname))
  }

  it should "produce an Exception when trying to drop a non-existing entity" in {
    intercept[Exception] {
      Entity.drop(fixture.entityname)
    }
  }

  it should "not produce an Exception when trying to drop a non-existing entity but the ifExists option is set" in {
      Entity.drop(fixture.entityname, true)
  }

  "a random test entity with metadata" should "be creatable and dropped" in {
    val entityname = fixture.entityname
    val fields: Map[String, FieldType] = Seq(("tmp", FieldTypes.STRINGTYPE), ("tmp2", FieldTypes.DOUBLETYPE)).toMap
    Entity.create(entityname, Option(fields))
    assert(Entity.exists(entityname))
    Entity.drop(entityname)
    assert(!Entity.exists(entityname))
  }

}