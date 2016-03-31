package ch.unibas.dmi.dbis.adam.entity

import ch.unibas.dmi.dbis.adam.api.{DropEntityOp, CreateEntityOp}
import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.datatypes.feature.{FeatureVectorWrapper, FeatureVectorWrapperUDT}
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.test.AdamTestBase
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
  feature("data definition") {
    /**
      *
      */
    scenario("create an entity") {
      Given("a database with a few elements already")
      val givenEntities = Entity.list()

      When("a new random entity (without any metadata) is created")
      val entityname = getRandomName()
      CreateEntityOp(entityname)

      Then("one entity should be created")
      val finalEntities = Entity.list()
      eventually {
        finalEntities.size shouldBe givenEntities.size + 1
      }
      eventually {
        finalEntities.contains(entityname)
      }

      //clean up
      DropEntityOp(entityname)
    }

    /**
      *
      */
    scenario("drop an existing entity") {
      Given("there exists one entity")
      val entityname = getRandomName()
      CreateEntityOp(entityname)
      assert(Entity.list().contains(entityname))

      When("the entity is dropped")
      DropEntityOp(entityname)

      Then("the entity should no longer exist")
      assert(!Entity.list().contains(entityname))
    }

    /**
      *
      */
    scenario("create an entity with metadata") {
      Given("a database with a few elements already")
      val givenEntities = Entity.list()

      When("a new random entity with metadata is created")
      val fieldTemplate = Seq(
        ("stringfield", FieldTypes.STRINGTYPE, "text"),
        ("floatfield", FieldTypes.FLOATTYPE, "real"),
        ("doublefield", FieldTypes.DOUBLETYPE, "double precision"),
        ("intfield", FieldTypes.INTTYPE, "integer"),
        ("longfield", FieldTypes.LONGTYPE, "bigint"),
        ("booleanfield", FieldTypes.BOOLEANTYPE, "boolean")
      )

      val entityname = getRandomName()
      CreateEntityOp(entityname, Some(fieldTemplate.map(ft => (ft._1, FieldDefinition(ft._2))).toMap))

      Then("the entity should be created")
      val entities = Entity.list()
      val finalEntities = Entity.list()
      assert(finalEntities.size == givenEntities.size + 1)
      assert(finalEntities.contains(entityname))

      And("The metadata table should have been created")
      val result = getJDBCConnection.createStatement().executeQuery("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '" + entityname + "'")

      val lb = new ListBuffer[(String, String)]()
      while (result.next) {
        lb.+=((result.getString(1), result.getString(2)))
      }

      val dbNames = lb.toList.toMap
      val templateNames = fieldTemplate.map(ft => (ft._1, ft._3)).toMap + (FieldNames.idColumnName -> "bigint")

      assert(dbNames.size == templateNames.size)

      And("the metadata table should contain the same columns (with the corresponding data type)")
      assert(dbNames.keySet.forall({ key => dbNames(key) == templateNames(key) }))

      And("the index on the id field is created")
      val indexesResult = getJDBCConnection.createStatement().executeQuery("SELECT t.relname AS table, i.relname AS index, a.attname AS column FROM pg_class t, pg_class i, pg_index ix, pg_attribute a WHERE t.oid = ix.indrelid AND i.oid = ix.indexrelid AND a.attrelid = t.oid AND a.attnum = ANY(ix.indkey) AND t.relkind = 'r' AND t.relname = '" + entityname + "' AND a.attname = '" + FieldNames.idColumnName + "'")
      indexesResult.next()
      assert(indexesResult.getString(3) == FieldNames.idColumnName)

      //clean up
      DropEntityOp(entityname)
    }

    /**
      *
      */
    scenario("drop an entity with metadata") {
      Given("an entity with metadata")
      val entityname = getRandomName()
      val fields = Map[String, FieldDefinition](("stringfield" -> FieldDefinition(FieldTypes.STRINGTYPE)))
      CreateEntityOp(entityname, Option(fields))

      val preResult = getJDBCConnection.createStatement().executeQuery("SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '" + entityname + "'")
      var tableCount = 0
      while (preResult.next) {
        tableCount += 1
      }

      When("the entity is dropped")
      DropEntityOp(entityname)

      Then("the metadata entity is dropped as well")
      val postResult = getJDBCConnection.createStatement().executeQuery("SELECT 1 FROM information_schema.tables WHERE table_schema = 'public' AND table_name = '" + entityname + "'")
      while (postResult.next) {
        tableCount -= 1
      }

      assert(tableCount == 1)
    }


    /**
      *
      */
    scenario("create an entity with very specified metadata (indexed, unique, primary key)") {
      Given("an entity with metadata")
      val entityname = getRandomName()
      val fields = Map[String, FieldDefinition](
        ("pkfield" -> FieldDefinition(FieldTypes.INTTYPE, true)),
        ("uniquefield" -> FieldDefinition(FieldTypes.INTTYPE, false, true)),
        ("indexedfield" -> FieldDefinition(FieldTypes.INTTYPE, false, false, true))
      )

      When("the entity is created")
      CreateEntityOp(entityname, Option(fields))

      Then("the PK should be correctly")
      val pkResult = getJDBCConnection.createStatement().executeQuery(
        "SELECT a.attname FROM pg_index i JOIN   pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) WHERE  i.indrelid = '" + entityname + "'::regclass AND i.indisprimary;")
      pkResult.next()
      assert(pkResult.getString(1) == "pkfield")

      And("the unique and indexed fields should be set correctly")
      val indexesResult = getJDBCConnection.createStatement().executeQuery("SELECT t.relname AS table, i.relname AS index, a.attname AS column FROM pg_class t, pg_class i, pg_index ix, pg_attribute a WHERE t.oid = ix.indrelid AND i.oid = ix.indexrelid AND a.attrelid = t.oid AND a.attnum = ANY(ix.indkey) AND t.relkind = 'r' AND t.relname = '" + entityname + "'")
      val indexesLb = ListBuffer.empty[String]
      while(indexesResult.next()){
        indexesLb += indexesResult.getString("column")
      }
      val indexes = indexesLb.toList
      assert(indexes.contains("uniquefield"))
      assert(indexes.contains("indexedfield"))

      DropEntityOp(entityname)
    }
  }


  feature("data manipulation") {

    /**
      *
      */
    scenario("insert data in an entity without metadata") {
      Given("an entity without metadata")
      val entityname = getRandomName()
      CreateEntityOp(entityname)

      val ntuples = Random.nextInt(1000)
      val ndims = 100

      val schema = StructType(Seq(
        StructField(FieldNames.featureColumnName, new FeatureVectorWrapperUDT, false)
      ))

      val rdd = SparkStartup.sc.parallelize((0 until ntuples).map(id =>
        Row(new FeatureVectorWrapper(Seq.fill(ndims)(Random.nextFloat())))
      ))

      val data = SparkStartup.sqlContext.createDataFrame(rdd, schema)

      When("data without metadata is inserted")
      Entity.insertData(entityname, data)

      Then("the data is available without metadata")
      assert(Entity.countTuples(entityname) == ntuples)

      //clean up
      DropEntityOp(entityname)
    }


    /**
      *
      */
    scenario("insert data in an entity with metadata") {
      Given("an entity with metadata")
      val entityname = getRandomName()
      //every field has been created twice, one is not filled to check whether this works too
      val fieldTemplate = Seq(
        ("stringfield", FieldTypes.STRINGTYPE, "text"),
        ("stringfieldunfilled", FieldTypes.STRINGTYPE, "text"),
        ("floatfield", FieldTypes.FLOATTYPE, "real"),
        ("floatfieldunfilled", FieldTypes.FLOATTYPE, "real"),
        ("doublefield", FieldTypes.DOUBLETYPE, "double precision"),
        ("doublefieldunfilled", FieldTypes.DOUBLETYPE, "double precision"),
        ("intfield", FieldTypes.INTTYPE, "integer"),
        ("intfieldunfilled", FieldTypes.LONGTYPE, "bigint"),
        ("longfield", FieldTypes.LONGTYPE, "bigint"),
        ("longfieldunfilled", FieldTypes.LONGTYPE, "bigint"),
        ("booleanfield", FieldTypes.BOOLEANTYPE, "boolean"),
        ("booleanfieldunfilled", FieldTypes.BOOLEANTYPE, "boolean")
      )
      val fields = fieldTemplate.map(ft => (ft._1, FieldDefinition(ft._2))).toMap
      CreateEntityOp(entityname, Option(fields))

      val ntuples = Random.nextInt(1000)
      val ndims = 100
      val stringLength = 10
      val maxInt = 50000

      val schema = StructType(fieldTemplate.filterNot(_._1.endsWith("unfilled"))
        .map(field => StructField(field._1, field._2.datatype, false)).+:(StructField(FieldNames.featureColumnName, new FeatureVectorWrapperUDT, false)))

      val rdd = SparkStartup.sc.parallelize((0 until ntuples).map(id =>
        Row(new FeatureVectorWrapper(Seq.fill(ndims)(Random.nextFloat())),
          Random.nextString(stringLength),
          math.abs(Random.nextFloat()),
          math.abs(Random.nextDouble()),
          math.abs(Random.nextInt(maxInt)),
          math.abs(Random.nextLong()),
          Random.nextBoolean()
        )))

      val data = SparkStartup.sqlContext.createDataFrame(rdd, schema)

      When("data with metadata is inserted")
      Entity.insertData(entityname, data)

      Then("the data is available with metadata")
      assert(Entity.countTuples(entityname) == ntuples)

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

      //clean up
      DropEntityOp(entityname)
    }
  }
}