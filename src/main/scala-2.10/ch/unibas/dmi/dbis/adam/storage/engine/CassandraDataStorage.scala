package ch.unibas.dmi.dbis.adam.storage.engine

import ch.unibas.dmi.dbis.adam.datatypes.feature.FeatureVectorWrapper
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.entity.Tuple.TupleID
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.storage.components.FeatureStorage
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.sql.{DataFrame, SaveMode}


/**
 * adamtwo
 *
 * Ivan Giangreco
 * December 2015
 */
object CassandraDataStorage extends FeatureStorage {
  private case class InternalCassandraRowFormat(id: Long, f : Seq[Float])

  override def read(entityname: EntityName, filter: Option[scala.collection.Set[TupleID]]): DataFrame = {
    new CassandraSQLContext(SparkStartup.sc).table(entityname)
  }

  override def drop(entityname: EntityName): Unit = ???

  override def write(entityname: EntityName, df: DataFrame, mode: SaveMode): Unit = {
    df.rdd.map(r => InternalCassandraRowFormat(r.getLong(0), r.getAs[FeatureVectorWrapper](1).getSeq())).saveAsCassandraTable("adamtwo", entityname)
  }
}

