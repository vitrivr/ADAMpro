package org.vitrivr.adampro.entity

import org.vitrivr.adampro.AdamTestBase
import org.vitrivr.adampro.api.EntityOp
import org.vitrivr.adampro.datatypes.FieldTypes
import org.vitrivr.adampro.query.query.Predicate
import org.apache.spark.sql.{Row, types}
import org.apache.spark.sql.types._
import org.scalatest.Matchers._
import org.vitrivr.adampro.config.FieldNames
import org.vitrivr.adampro.datatypes.vector.Vector

import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
  * adamtwo
  *
  * Ivan Giangreco
  * March 2016
  */
class EntityTestSuite extends AdamTestBase {

  case class TemplateFieldDefinition(name: String, fieldType: FieldTypes.FieldType, sqlType: String)

  def ntuples() = Random.nextInt(1000)
  def ndims() = 20 + Random.nextInt(80)

  feature("data definition") {
    /**
      *
      */
    scenario("create an entity") {
      withEntityName { entityname =>
        Given("a database with a few elements already")
        val givenEntities = Entity.list

        When("a new random entity (without any metadata) is created")
        val entity = Entity.create(entityname, Seq(new AttributeDefinition("idfield", FieldTypes.LONGTYPE), new AttributeDefinition("feature", FieldTypes.VECTORTYPE)))
        assert(entity.isSuccess)

        Then("one entity should be created")
        val finalEntities = Entity.list
        eventually {
          finalEntities.size shouldBe givenEntities.size + 1
        }
        eventually {
          finalEntities.contains(entityname)
        }
      }
    }

    /**
      *
      */
    scenario("drop an existing entity") {
      withEntityName { entityname =>
        Given("there exists one entity")
        val entity = Entity.create(entityname, Seq(new AttributeDefinition("idfield", FieldTypes.LONGTYPE), new AttributeDefinition("feature", FieldTypes.VECTORTYPE)))
        assert(entity.isSuccess)
        assert(Entity.list.contains(entityname.toLowerCase()))

        When("the entity is dropped")
        EntityOp.drop(entityname)

        Then("the entity should no longer exist")
        assert(!Entity.list.contains(entityname.toLowerCase()))
      }
    }

    /**
      *
      */
    scenario("create an entity with multiple feature fields") {
      withEntityName { entityname =>
        Given("a database with a few elements already")
        val givenEntities = Entity.list

        When("a new random entity (without any metadata) is created")
        val entity = Entity.create(entityname, Seq(new AttributeDefinition("idfield", FieldTypes.LONGTYPE), new AttributeDefinition("feature1", FieldTypes.VECTORTYPE), new AttributeDefinition("feature2", FieldTypes.VECTORTYPE)))
        assert(entity.isSuccess)

        Then("one entity should be created")
        val finalEntities = Entity.list
        eventually {
          finalEntities.size shouldBe givenEntities.size + 1
        }
        eventually {
          finalEntities.contains(entityname)
        }
      }
    }

    /**
      *
      */
    scenario("create an entity with metadata") {
      withEntityName { entityname =>
        Given("a database with a few elements already")
        val givenEntities = Entity.list

        When("a new random entity with metadata is created")
        val fieldTemplate = Seq(
          TemplateFieldDefinition("idfield", FieldTypes.LONGTYPE, "bigint"),
          TemplateFieldDefinition("vectorfield", FieldTypes.VECTORTYPE, ""),
          TemplateFieldDefinition("stringfield", FieldTypes.STRINGTYPE, "text"),
          TemplateFieldDefinition("floatfield", FieldTypes.FLOATTYPE, "real"),
          TemplateFieldDefinition("doublefield", FieldTypes.DOUBLETYPE, "double precision"),
          TemplateFieldDefinition("intfield", FieldTypes.INTTYPE, "integer"),
          TemplateFieldDefinition("longfield", FieldTypes.LONGTYPE, "bigint"),
          TemplateFieldDefinition("booleanfield", FieldTypes.BOOLEANTYPE, "boolean")
        )

        val entity = Entity.create(entityname, fieldTemplate.map(ft => new AttributeDefinition(ft.name, ft.fieldType)))
        assert(entity.isSuccess)

        Then("the entity should be created")
        val entities = Entity.list
        val finalEntities = Entity.list
        assert(finalEntities.size == givenEntities.size + 1)
        assert(finalEntities.contains(entityname.toLowerCase()))

        And("The metadata table should have been created")
        val result = getMetadataConnection.createStatement().executeQuery("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '" + entityname.toLowerCase() + "' AND column_name != '" + FieldNames.internalIdColumnName + "'")

        val lb = new ListBuffer[(String, String)]()
        while (result.next) {
          lb.+=((result.getString(1), result.getString(2)))
        }

        val dbNames = lb.toList.toMap //fields from relational database
      val templateNames = fieldTemplate.filter(_.sqlType.length > 0).map(ft => ft.name -> ft.sqlType).toMap //fields that should be stored in relational database


        assert(dbNames.size == templateNames.size)

        And("the metadata table should contain the same columns (with the corresponding data type)")
        assert(dbNames.keySet.forall({ key => dbNames(key) == templateNames(key) }))

        And("the index on the id field is created")
        val indexesResult = getMetadataConnection.createStatement().executeQuery("SELECT t.relname AS table, i.relname AS index, a.attname AS column FROM pg_class t, pg_class i, pg_index ix, pg_attribute a WHERE t.oid = ix.indrelid AND i.oid = ix.indexrelid AND a.attrelid = t.oid AND a.attnum = ANY(ix.indkey) AND t.relkind = 'r' AND t.relname = '" + entityname.toLowerCase() + "' AND a.attname = '" + FieldNames.internalIdColumnName + "'")
        indexesResult.next()
        assert(indexesResult.getString(3) == FieldNames.internalIdColumnName)
      }
    }

    /**
      *
      */
    scenario("drop an entity with metadata") {
      withEntityName { entityname =>
        Given("an entity with metadata")
        val fields = Seq[AttributeDefinition](new AttributeDefinition("idfield", FieldTypes.LONGTYPE), new AttributeDefinition("feature", FieldTypes.VECTORTYPE), new AttributeDefinition("stringfield", FieldTypes.STRINGTYPE))
        EntityOp.create(entityname, fields)

        val preResult = getMetadataConnection.createStatement().executeQuery("SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '" + entityname.toLowerCase() + "'")
        var tableCount = 0
        while (preResult.next) {
          tableCount += 1
        }

        When("the entity is dropped")
        EntityOp.drop(entityname)

        Then("the metadata entity is dropped as well")
        val postResult = getMetadataConnection.createStatement().executeQuery("SELECT 1 FROM information_schema.tables WHERE table_schema = 'relational' AND table_name = '" + entityname.toLowerCase() + "'")
        while (postResult.next) {
          tableCount -= 1
        }

        assert(tableCount == 1)
      }
    }


    /**
      *
      */
    scenario("create an entity with very specified metadata (indexed, unique, primary key)") {
      withEntityName { entityname =>
        Given("an entity with metadata")
        val fields = Seq[AttributeDefinition](
          new AttributeDefinition("pkfield", FieldTypes.LONGTYPE),
          new AttributeDefinition("uniquefield", FieldTypes.INTTYPE, params = Map("unique" -> "true")),
          new AttributeDefinition("indexedfield", FieldTypes.INTTYPE, params = Map("indexed" -> "true")),
          new AttributeDefinition("feature", FieldTypes.VECTORTYPE)
        )

        When("the entity is created")
        EntityOp.create(entityname, fields)

        Then("the PK should be correctly")
        val pkResult = getMetadataConnection.createStatement().executeQuery(
          "SELECT a.attname FROM pg_index i JOIN   pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) WHERE  i.indrelid = '" + entityname.toLowerCase() + "'::regclass AND i.indisprimary;")
        pkResult.next()
        val pk = pkResult.getString(1)
        assert(pk == FieldNames.internalIdColumnName)

        And("the unique and indexed fields should be set correctly")
        val indexesResult = getMetadataConnection.createStatement().executeQuery("SELECT t.relname AS table, i.relname AS index, a.attname AS column FROM pg_class t, pg_class i, pg_index ix, pg_attribute a WHERE t.oid = ix.indrelid AND i.oid = ix.indexrelid AND a.attrelid = t.oid AND a.attnum = ANY(ix.indkey) AND t.relkind = 'r' AND t.relname = '" + entityname.toLowerCase() + "'")
        val indexesLb = ListBuffer.empty[String]
        while (indexesResult.next()) {
          indexesLb += indexesResult.getString("column")
        }
        val indexes = indexesLb.toList
        assert(indexes.contains("uniquefield"))
        assert(indexes.contains("indexedfield"))
      }
    }
  }


  feature("data manipulation") {
    /**
      *
      */
    scenario("insert data in an entity without metadata") {
      withEntityName { entityname =>
        Given("an entity without metadata")
        val creationAttributes = Seq(new AttributeDefinition("idfield", FieldTypes.LONGTYPE), new AttributeDefinition("vectorfield", FieldTypes.VECTORTYPE))
        Entity.create(entityname, creationAttributes)

        val schema = StructType(creationAttributes.map(a => StructField(a.name, a.fieldtype.datatype, false)))

        val tuplesInsert = ntuples()
        val dimsInsert = ndims()

        val rdd = ac.sc.parallelize((0 until tuplesInsert).map(id =>
          Row(Random.nextLong(), Seq.fill(dimsInsert)(Vector.nextRandom()))
        ))

        val data = ac.sqlContext.createDataFrame(rdd, schema)

        When("data without metadata is inserted")
        EntityOp.insert(entityname, data)

        Then("the data is available without metadata")
        val counted = EntityOp.count(entityname).get
        assert(counted == tuplesInsert)
      }
    }

    /**
      *
      */
    scenario("insert data in an entity with multiple feature fields without metadata") {
      withEntityName { entityname =>
        Given("an entity without metadata")

        val creationAttributes = Seq(new AttributeDefinition("idfield", FieldTypes.LONGTYPE), new AttributeDefinition("featurefield1", FieldTypes.VECTORTYPE), new AttributeDefinition("featurefield2", FieldTypes.VECTORTYPE))
        Entity.create(entityname, creationAttributes)

        val schema = StructType(creationAttributes.map(a => StructField(a.name, a.fieldtype.datatype, false)))

        val tuplesInsert = ntuples()
        val dimsInsert = ndims()

        val rdd = ac.sc.parallelize((0 until tuplesInsert).map(id =>
          Row(Random.nextLong(), Seq.fill(dimsInsert)(Vector.nextRandom()), Seq.fill(dimsInsert)(Vector.nextRandom()))
        ))

        val data = ac.sqlContext.createDataFrame(rdd, schema)

        When("data without metadata is inserted")
        EntityOp.insert(entityname, data)

        Then("the data is available without metadata")
        val counted = EntityOp.count(entityname).get
        assert(counted == tuplesInsert)
      }
    }


    /**
      *
      */
    scenario("insert data in an entity with metadata") {
      withEntityName { entityname =>
        Given("an entity with metadata")
        //every field has been created twice, one is not filled to check whether this works too
        val fieldTemplate = Seq(
          TemplateFieldDefinition("idfield", FieldTypes.LONGTYPE, "bigint"),
          TemplateFieldDefinition("vectorfield", FieldTypes.VECTORTYPE, ""),
          TemplateFieldDefinition("stringfield", FieldTypes.STRINGTYPE, "text"),
          TemplateFieldDefinition("stringfieldunfilled", FieldTypes.STRINGTYPE, "text"),
          TemplateFieldDefinition("floatfield", FieldTypes.FLOATTYPE, "real"),
          TemplateFieldDefinition("floatfieldunfilled", FieldTypes.FLOATTYPE, "real"),
          TemplateFieldDefinition("doublefield", FieldTypes.DOUBLETYPE, "double precision"),
          TemplateFieldDefinition("doublefieldunfilled", FieldTypes.DOUBLETYPE, "double precision"),
          TemplateFieldDefinition("intfield", FieldTypes.INTTYPE, "integer"),
          TemplateFieldDefinition("intfieldunfilled", FieldTypes.LONGTYPE, "integer"),
          TemplateFieldDefinition("longfield", FieldTypes.LONGTYPE, "bigint"),
          TemplateFieldDefinition("longfieldunfilled", FieldTypes.LONGTYPE, "bigint"),
          TemplateFieldDefinition("booleanfield", FieldTypes.BOOLEANTYPE, "boolean"),
          TemplateFieldDefinition("booleanfieldunfilled", FieldTypes.BOOLEANTYPE, "boolean")
        )

        val entity = Entity.create(entityname, fieldTemplate.map(x => new AttributeDefinition(x.name, x.fieldType)))

        val stringLength = 10
        val maxInt = 50000

        val schema = StructType(
          fieldTemplate.filterNot(_.name.endsWith("unfilled"))
            .map(field => StructField(field.name, field.fieldType.datatype, false))
        )

        val tuplesInsert = ntuples()
        val dimsInsert = ndims()

        val rdd = ac.sc.parallelize((0 until tuplesInsert).map(id =>
          Row(
            Random.nextLong(),
            Seq.fill(dimsInsert)(Vector.nextRandom()),
            Random.nextString(stringLength),
            math.abs(Random.nextFloat()),
            math.abs(Random.nextDouble()),
            math.abs(Random.nextInt(maxInt)),
            math.abs(Random.nextLong()),
            Random.nextBoolean()
          )))

        val data = ac.sqlContext.createDataFrame(rdd, schema)

        When("data with metadata is inserted")
        EntityOp.insert(entityname, data)

        Then("the data is available with metadata")
        val counted = EntityOp.count(entityname).get.toInt
        assert(counted == tuplesInsert)

        And("all tuples are inserted")
        val countResult = getMetadataConnection.createStatement().executeQuery("SELECT COUNT(*) AS count FROM " + entityname)
        countResult.next() //go to first result

        val tableCount = countResult.getInt("count")
        assert(tableCount == tuplesInsert)

        And("all filled fields should be filled")
        val randomRowResult = getMetadataConnection.createStatement().executeQuery("SELECT * FROM " + entityname + " ORDER BY RANDOM() LIMIT 1")
        randomRowResult.next() //go to first result
        assert(randomRowResult.getString("stringfield").length == stringLength)
        assert(randomRowResult.getFloat("floatField") >= 0)
        assert(randomRowResult.getDouble("doubleField") >= 0)
        assert(randomRowResult.getInt("intfield") < maxInt)
        assert(randomRowResult.getLong("longfield") >= 0)
        assert((randomRowResult.getBoolean("booleanfield")) || (!randomRowResult.getBoolean("booleanfield")))

        And("all unfilled fields should be empty")
        assert(randomRowResult.getString("stringfieldunfilled") == null)
        assert(randomRowResult.getFloat("floatFieldunfilled") < 10e-8)
        assert(randomRowResult.getDouble("doubleFieldunfilled") < 10e-8)
        assert(randomRowResult.getInt("intfieldunfilled") == 0)
        assert(randomRowResult.getLong("longfieldunfilled") == 0)
        assert(randomRowResult.getBoolean("booleanfieldunfilled") == false)
      }
    }


    /**
      *
      */
    scenario("delete data from an entity with metadata") {
      withQueryEvaluationSet { es =>
        val entity = es.entity
        import org.apache.spark.sql.functions._
        val newCount = es.fullData.filter(col("booleanfield") === true).count()
        entity.delete(Seq(Predicate("booleanfield", None, Seq(false))))
        val count = entity.count
        assert(newCount === count)
      }
    }
  }
}