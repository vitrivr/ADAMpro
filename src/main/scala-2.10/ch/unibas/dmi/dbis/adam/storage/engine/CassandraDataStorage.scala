package ch.unibas.dmi.dbis.adam.storage.engine

import ch.unibas.dmi.dbis.adam.datatypes.feature.FeatureVectorWrapper
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.entity.Tuple.TupleID
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.storage.components.FeatureStorage
import com.datastax.spark.connector._
import org.apache.spark.sql.types.{BinaryType, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}


/**
 * adamtwo
 *
 * Ivan Giangreco
 * December 2015
 */
object CassandraDataStorage extends FeatureStorage with Serializable {
  case class InternalCassandraRowFormat(id: Long, f : Seq[Float])

  override def read(entityname: EntityName, filter: Option[scala.collection.Set[TupleID]]): DataFrame = {
    val rowRDD = if(filter.isDefined){
      val subresults = filter.get.grouped(500).map( subfilter =>
        SparkStartup.sc.cassandraTable("adamtwo", entityname).where("id IN ?", subfilter).map(crow => Row(crow.getLong(0), asWorkingVectorWrapper(crow.getList[Float](1))))
      ).toSeq

      var base = subresults(0)

      subresults.slice(1, subresults.length).foreach{ sr =>
        base = base.union(sr)
      }

      base
    } else {
      val cassandraScan = SparkStartup.sc.cassandraTable("adamtwo", entityname)
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

  override def drop(entityname: EntityName): Unit = ??? //TODO

  override def write(entityname: EntityName, df: DataFrame, mode: SaveMode): Unit = {
    //TODO: save as cassandra, save to cassandra
    df.rdd.map(r => InternalCassandraRowFormat(r.getLong(0).toInt, r.getAs[FeatureVectorWrapper](1).getSeq())).saveToCassandra("adamtwo", entityname)
  }

  override def count(entityname: EntityName): Int = {
    SparkStartup.sc.cassandraTable("adamtwo", entityname).cassandraCount().toInt
  }

  private def asWorkingVectorWrapper(value: Vector[Float]): FeatureVectorWrapper = {
    new FeatureVectorWrapper(value.toSeq)
  }
}


