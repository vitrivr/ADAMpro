package ch.unibas.dmi.dbis.adam.storage.engine

import ch.unibas.dmi.dbis.adam.datatypes.feature.FeatureVectorWrapper
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.entity.Tuple.TupleID
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.storage.components.FeatureStorage
import com.datastax.driver.core.Session
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.sql.types.{BinaryType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}


/**
 * adamtwo
 *
 * Ivan Giangreco
 * December 2015
 */
object CassandraDataStorage extends FeatureStorage with Serializable {
  private val defaultKeyspace = "adamtwo"

  val conn = CassandraConnector(SparkStartup.sparkConfig)
  val adamtwoKeyspace = conn.withClusterDo(_.getMetadata).getKeyspace(defaultKeyspace)

  if (adamtwoKeyspace == null) {
    conn.withSessionDo { session =>
      createKeyspace(session)
    }
  }

  private def createKeyspaceCql(keyspacename: String = defaultKeyspace) =
    s"""
       |CREATE KEYSPACE IF NOT EXISTS $keyspacename
       |WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }
       |AND durable_writes = false
       |""".stripMargin

  private def dropKeyspaceCql(keyspacename: String) =
    s"""
       |DROP KEYSPACE IF EXISTS $keyspacename
       |""".stripMargin

  private def createTableCql(tablename: EntityName) =
    s"""
       |CREATE TABLE IF NOT EXISTS $tablename
       | (id bigint PRIMARY KEY, feature LIST<FLOAT>)
       |""".stripMargin

  private def dropTableCql(tablename: EntityName) =
    s"""
       |DROP TABLE IF EXISTS $tablename
       |""".stripMargin


  private def createKeyspace(session: Session, name: String = defaultKeyspace): Unit = {
    session.execute(dropKeyspaceCql(name))
    session.execute(createKeyspaceCql(name))
  }


  case class InternalCassandraRowFormat(id: Long, feature: Seq[Float])

  /**
   *
   * @param entityname
   * @param filter
   * @return
   */
  override def read(entityname: EntityName, filter: Option[scala.collection.Set[TupleID]]): DataFrame = {
    val rowRDD = if (filter.isDefined) {
      val subresults = filter.get.grouped(500).map(subfilter =>
        SparkStartup.sc.cassandraTable(defaultKeyspace, entityname).where("id IN ?", subfilter).map(crow => Row(crow.getLong(0), asWorkingVectorWrapper(crow.getList[Float](1))))
      ).toSeq

      var base = subresults(0)

      subresults.slice(1, subresults.length).foreach { sr =>
        base = base.union(sr)
      }

      base
    } else {
      val cassandraScan = SparkStartup.sc.cassandraTable(defaultKeyspace, entityname)
      cassandraScan.map(crow => Row(crow.getLong(0), asWorkingVectorWrapper(crow.getList[Float](1))))
    }


    val schema = StructType(
      List(
        StructField("id", LongType, false),
        StructField("feature", BinaryType, false)
      )
    )
    SparkStartup.sqlContext.createDataFrame(rowRDD, schema)
  }

  /**
   *
   * @param entityname
   */
  override def drop(entityname: EntityName): Unit = {
    conn.withSessionDo { session =>
      session.execute("use " + defaultKeyspace)
      session.execute(dropTableCql(entityname))
    }
  }

  /**
   *
   * @param entityname
   * @param df
   */
  override def create(entityname: EntityName, df: DataFrame): Unit = {
    conn.withSessionDo { session =>
      session.execute("use " + defaultKeyspace)
      session.execute(createTableCql(entityname))
    }
  }

  /**
   *
   * @param entityname
   * @param df
   * @param mode
   */
  override def write(entityname: EntityName, df: DataFrame, mode: SaveMode): Unit = {
    if (mode == SaveMode.Overwrite) {
      conn.withSessionDo { session =>
        session.execute("use " + defaultKeyspace)
        session.execute(dropTableCql(entityname))
        session.execute(createTableCql(entityname))
      }
    }

    df.rdd.map(r => InternalCassandraRowFormat(r.getLong(0).toInt, r.getAs[FeatureVectorWrapper](1).getSeq())).saveToCassandra("adamtwo", entityname)
  }

  override def count(entityname: EntityName): Int = {
    SparkStartup.sc.cassandraTable("adamtwo", entityname).cassandraCount().toInt
  }

  private def asWorkingVectorWrapper(value: Vector[Float]): FeatureVectorWrapper = {
    new FeatureVectorWrapper(value.toSeq)
  }
}


