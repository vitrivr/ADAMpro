package ch.unibas.dmi.dbis.adam.entity

import ch.unibas.dmi.dbis.adam.AdamTestBase
import ch.unibas.dmi.dbis.adam.api.EntityOp
import ch.unibas.dmi.dbis.adam.datatypes.feature.{FeatureVectorWrapper, FeatureVectorWrapperUDT}
import ch.unibas.dmi.dbis.adam.main.SparkStartup.Implicits._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.Matchers._

import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
  * adamtwo
  *
  * Ivan Giangreco
  * March 2016
  */
class EntityTestSuite extends AdamTestBase {

  case class TemplateFieldDefinition(name: String, fieldType: FieldTypes.FieldType, pk: Boolean, sqlType: String)


  feature("data definition") {
    /**
      *
      */
    scenario("create an entity") {
      withEntityName { entityname =>
        Given("a database with a few elements already")
        val givenEntities = EntityHandler.list()

        When("a new random entity (without any metadata) is created")
        EntityHandler.create(entityname, Seq(FieldDefinition("idfield", FieldTypes.LONGTYPE, true), FieldDefinition("feature", FieldTypes.FEATURETYPE)))

        Then("one entity should be created")
        val finalEntities = EntityHandler.list()
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
        EntityHandler.create(entityname, Seq(FieldDefinition("idfield", FieldTypes.LONGTYPE, true), FieldDefinition("feature", FieldTypes.FEATURETYPE)))
        assert(EntityHandler.list().contains(entityname.toLowerCase()))

        When("the entity is dropped")
        EntityOp.drop(entityname)

        Then("the entity should no longer exist")
        assert(!EntityHandler.list().contains(entityname.toLowerCase()))
      }
    }

    /**
      *
      */
    scenario("create an entity with multiple feature fields") {
      withEntityName { entityname =>
        Given("a database with a few elements already")
        val givenEntities = EntityHandler.list()

        When("a new random entity (without any metadata) is created")
        EntityHandler.create(entityname, Seq(FieldDefinition("idfield", FieldTypes.LONGTYPE, true), FieldDefinition("feature1", FieldTypes.FEATURETYPE), FieldDefinition("feature2", FieldTypes.FEATURETYPE)))

        Then("one entity should be created")
        val finalEntities = EntityHandler.list()
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
        val givenEntities = EntityHandler.list()

        When("a new random entity with metadata is created")
        val fieldTemplate = Seq(
          TemplateFieldDefinition("idfield", FieldTypes.LONGTYPE, true, "bigint"),
          TemplateFieldDefinition("featurefield", FieldTypes.FEATURETYPE, false, ""),
          TemplateFieldDefinition("stringfield", FieldTypes.STRINGTYPE, false, "text"),
          TemplateFieldDefinition("floatfield", FieldTypes.FLOATTYPE, false, "real"),
          TemplateFieldDefinition("doublefield", FieldTypes.DOUBLETYPE, false, "double precision"),
          TemplateFieldDefinition("intfield", FieldTypes.INTTYPE, false, "integer"),
          TemplateFieldDefinition("longfield", FieldTypes.LONGTYPE, false, "bigint"),
          TemplateFieldDefinition("booleanfield", FieldTypes.BOOLEANTYPE, false, "boolean")
        )

        val entity = EntityHandler.create(entityname, fieldTemplate.map(ft => FieldDefinition(ft.name, ft.fieldType, ft.pk)))

        Then("the entity should be created")
        val entities = EntityHandler.list()
        val finalEntities = EntityHandler.list()
        assert(finalEntities.size == givenEntities.size + 1)
        assert(finalEntities.contains(entityname.toLowerCase()))

        And("The metadata table should have been created")
        val result = getJDBCConnection.createStatement().executeQuery("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '" + entityname.toLowerCase() + "'")

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
        val indexesResult = getJDBCConnection.createStatement().executeQuery("SELECT t.relname AS table, i.relname AS index, a.attname AS column FROM pg_class t, pg_class i, pg_index ix, pg_attribute a WHERE t.oid = ix.indrelid AND i.oid = ix.indexrelid AND a.attrelid = t.oid AND a.attnum = ANY(ix.indkey) AND t.relkind = 'r' AND t.relname = '" + entityname.toLowerCase() + "' AND a.attname = '" + "idfield" + "'")
        indexesResult.next()
        assert(indexesResult.getString(3) == "idfield")
      }
    }

    /**
      *
      */
    scenario("drop an entity with metadata") {
      withEntityName { entityname =>
        Given("an entity with metadata")
        val fields = Seq[FieldDefinition](FieldDefinition("idfield", FieldTypes.LONGTYPE, true), FieldDefinition("feature", FieldTypes.FEATURETYPE), FieldDefinition("stringfield", FieldTypes.STRINGTYPE))
        EntityOp(entityname, fields)

        val preResult = getJDBCConnection.createStatement().executeQuery("SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '" + entityname.toLowerCase() + "'")
        var tableCount = 0
        while (preResult.next) {
          tableCount += 1
        }

        When("the entity is dropped")
        EntityOp.drop(entityname)

        Then("the metadata entity is dropped as well")
        val postResult = getJDBCConnection.createStatement().executeQuery("SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '" + entityname.toLowerCase() + "'")
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
        val fields = Seq[FieldDefinition](
          FieldDefinition("pkfield", FieldTypes.LONGTYPE, true),
          FieldDefinition("uniquefield", FieldTypes.INTTYPE, false, true),
          FieldDefinition("indexedfield", FieldTypes.INTTYPE, false, false, true),
          FieldDefinition("feature", FieldTypes.FEATURETYPE)
        )

        When("the entity is created")
        EntityOp(entityname, fields)

        Then("the PK should be correctly")
        val pkResult = getJDBCConnection.createStatement().executeQuery(
          "SELECT a.attname FROM pg_index i JOIN   pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) WHERE  i.indrelid = '" + entityname.toLowerCase() + "'::regclass AND i.indisprimary;")
        pkResult.next()
        assert(pkResult.getString(1) == "pkfield")

        And("the unique and indexed fields should be set correctly")
        val indexesResult = getJDBCConnection.createStatement().executeQuery("SELECT t.relname AS table, i.relname AS index, a.attname AS column FROM pg_class t, pg_class i, pg_index ix, pg_attribute a WHERE t.oid = ix.indrelid AND i.oid = ix.indexrelid AND a.attrelid = t.oid AND a.attnum = ANY(ix.indkey) AND t.relkind = 'r' AND t.relname = '" + entityname.toLowerCase() + "'")
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
    scenario("insert data in an entity with auto-increment metadata") {
      withEntityName { entityname =>
        EntityHandler.create(entityname, Seq(FieldDefinition("idfield", FieldTypes.AUTOTYPE, true), FieldDefinition("featurefield", FieldTypes.FEATURETYPE, false, false, false)))

        val ntuples = Random.nextInt(1000)
        val ndims = 100

        When("data is inserted by specifying id")
        val wrongschema = StructType(Seq(
          StructField("idfield", LongType, false),
          StructField("featurefield", new FeatureVectorWrapperUDT, false)
        ))

        val wrongrdd = ac.sc.parallelize((0 until ntuples).map(id =>
          Row(Random.nextLong(), new FeatureVectorWrapper(Seq.fill(ndims)(Random.nextFloat())))
        ))

        val wrongdata = ac.sqlContext.createDataFrame(wrongrdd, wrongschema)

        val wronginsert = EntityOp.insert(entityname, wrongdata)

        Then("the data is not inserted")
        assert(wronginsert.isFailure)


        val schema = StructType(Seq(
          StructField("featurefield", new FeatureVectorWrapperUDT, false)
        ))

        val rdd = ac.sc.parallelize((0 until ntuples).map(id =>
          Row(new FeatureVectorWrapper(Seq.fill(ndims)(Random.nextFloat())))
        ))

        val data = ac.sqlContext.createDataFrame(rdd, schema)

        When("data is inserted without specifying id")
        val insert = EntityOp.insert(entityname, data)
        assert(insert.isSuccess)

        Then("the data is available")
        val counted = EntityOp.count(entityname).get
        assert(counted - ntuples == 0)
      }
    }

    /**
      *
      */
    scenario("insert data in an entity without metadata") {
      withEntityName { entityname =>
        Given("an entity without metadata")
        EntityHandler.create(entityname, Seq(FieldDefinition("idfield", FieldTypes.LONGTYPE, true), FieldDefinition("featurefield", FieldTypes.FEATURETYPE, false, false, false)))

        val ntuples = Random.nextInt(1000)
        val ndims = 100

        val schema = StructType(Seq(
          StructField("idfield", LongType, false),
          StructField("featurefield", new FeatureVectorWrapperUDT, false)
        ))

        val rdd = ac.sc.parallelize((0 until ntuples).map(id =>
          Row(Random.nextLong(), new FeatureVectorWrapper(Seq.fill(ndims)(Random.nextFloat())))
        ))

        val data = ac.sqlContext.createDataFrame(rdd, schema)

        When("data without metadata is inserted")
        EntityOp.insert(entityname, data)

        Then("the data is available without metadata")
        val counted = EntityOp.count(entityname).get
        assert(counted - ntuples == 0)
      }
    }

    /**
      *
      */
    scenario("insert data in an entity with multiple feature fields without metadata") {
      withEntityName { entityname =>
        Given("an entity without metadata")
        EntityHandler.create(entityname, Seq(FieldDefinition("idfield", FieldTypes.LONGTYPE, true), FieldDefinition("featurefield1", FieldTypes.FEATURETYPE), FieldDefinition("featurefield2", FieldTypes.FEATURETYPE)))

        val ntuples = Random.nextInt(1000)
        val ndims = 100

        val schema = StructType(Seq(
          StructField("idfield", LongType, false),
          StructField("featurefield1", new FeatureVectorWrapperUDT, false),
          StructField("featurefield2", new FeatureVectorWrapperUDT, false)
        ))

        val rdd = ac.sc.parallelize((0 until ntuples).map(id =>
          Row(Random.nextLong(), new FeatureVectorWrapper(Seq.fill(ndims)(Random.nextFloat())), new FeatureVectorWrapper(Seq.fill(ndims)(Random.nextFloat())))
        ))

        val data = ac.sqlContext.createDataFrame(rdd, schema)

        When("data without metadata is inserted")
        EntityOp.insert(entityname, data)

        Then("the data is available without metadata")
        val counted = EntityOp.count(entityname).get
        assert(counted - ntuples == 0)
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
          TemplateFieldDefinition("idfield", FieldTypes.LONGTYPE, true, "bigint"),
          TemplateFieldDefinition("featurefield", FieldTypes.FEATURETYPE, false, ""),
          TemplateFieldDefinition("stringfield", FieldTypes.STRINGTYPE, false, "text"),
          TemplateFieldDefinition("stringfieldunfilled", FieldTypes.STRINGTYPE, false, "text"),
          TemplateFieldDefinition("floatfield", FieldTypes.FLOATTYPE, false, "real"),
          TemplateFieldDefinition("floatfieldunfilled", FieldTypes.FLOATTYPE, false, "real"),
          TemplateFieldDefinition("doublefield", FieldTypes.DOUBLETYPE, false, "double precision"),
          TemplateFieldDefinition("doublefieldunfilled", FieldTypes.DOUBLETYPE, false, "double precision"),
          TemplateFieldDefinition("intfield", FieldTypes.INTTYPE, false, "integer"),
          TemplateFieldDefinition("intfieldunfilled", FieldTypes.LONGTYPE, false, "integer"),
          TemplateFieldDefinition("longfield", FieldTypes.LONGTYPE, false, "bigint"),
          TemplateFieldDefinition("longfieldunfilled", FieldTypes.LONGTYPE, false, "bigint"),
          TemplateFieldDefinition("booleanfield", FieldTypes.BOOLEANTYPE, false, "boolean"),
          TemplateFieldDefinition("booleanfieldunfilled", FieldTypes.BOOLEANTYPE, false, "boolean")
        )

        val entity = EntityHandler.create(entityname, fieldTemplate.map(x => FieldDefinition(x.name, x.fieldType, x.pk)))

        val ntuples = Random.nextInt(1000)
        val ndims = 100
        val stringLength = 10
        val maxInt = 50000

        val schema = StructType(
          fieldTemplate.filterNot(_.name.endsWith("unfilled"))
            .map(field => StructField(field.name, field.fieldType.datatype, false))
        )

        val rdd = ac.sc.parallelize((0 until ntuples).map(id =>
          Row(
            Random.nextLong(),
            new FeatureVectorWrapper(Seq.fill(ndims)(Random.nextFloat())),
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
        assert(EntityOp.count(entityname).get.toInt == ntuples)

        And("all tuples are inserted")
        val countResult = getJDBCConnection.createStatement().executeQuery("SELECT COUNT(*) AS count FROM " + entityname)
        countResult.next() //go to first result

        val tableCount = countResult.getInt("count")
        assert(tableCount == ntuples)

        And("all filled fields should be filled")
        val randomRowResult = getJDBCConnection.createStatement().executeQuery("SELECT * FROM " + entityname + " ORDER BY RANDOM() LIMIT 1")
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
  }
}