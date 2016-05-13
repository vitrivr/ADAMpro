package ch.unibas.dmi.dbis.adam.index.structures

import java.io.Serializable

import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.http._
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.index.structures.ecp.{ECPIndex, ECPIndexer}
import ch.unibas.dmi.dbis.adam.index.structures.lsh.{LSHIndex, LSHIndexer}
import ch.unibas.dmi.dbis.adam.index.structures.pq.{PQIndex, PQIndexer}
import ch.unibas.dmi.dbis.adam.index.structures.sh.{SHIndex, SHIndexer}
import ch.unibas.dmi.dbis.adam.index.structures.va.{VAFIndexer, VAIndex, VAVIndexer}
import ch.unibas.dmi.dbis.adam.index.{Index, IndexGenerator}
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.query.distance.DistanceFunction
import org.apache.spark.sql.DataFrame

import SparkStartup.Implicits._

/**
  * adamtwo
  *
  * Ivan Giangreco
  * October 2015
  */
object IndexTypes {

  sealed abstract class IndexType(val name: String,
                                  val indextype: grpc.IndexType,
                                  val index: (IndexName, EntityName, DataFrame, Any) => Index,
                                 val indexer : (DistanceFunction, Map[String, String]) => IndexGenerator
                                 ) extends Serializable

  case object ECPINDEX extends IndexType("ecp", grpc.IndexType.ecp, ECPIndex.apply, ECPIndexer.apply)

  case object LSHINDEX extends IndexType("lsh", grpc.IndexType.lsh, LSHIndex.apply, LSHIndexer.apply)

  case object PQINDEX extends IndexType("pq", grpc.IndexType.pq, PQIndex.apply, PQIndexer.apply)

  case object SHINDEX extends IndexType("sh", grpc.IndexType.sh, SHIndex.apply, SHIndexer.apply)

  case object VAFINDEX extends IndexType("vaf", grpc.IndexType.vaf, VAIndex.apply, VAFIndexer.apply)

  case object VAVINDEX extends IndexType("vav", grpc.IndexType.vav, VAIndex.apply, VAVIndexer.apply)

  /**
    *
    */
  val values = Seq(ECPINDEX, LSHINDEX, PQINDEX, SHINDEX, VAFINDEX, VAVINDEX)

  /**
    *
    * @param s
    * @return
    */
  //TODO: checks
  def withName(s: String): Option[IndexType] = values.map(value => value.name -> value).toMap.get(s)

  /**
    *
    * @param indextype
    * @return
    */
  //TODO: checks
  def withIndextype(indextype: grpc.IndexType): Option[IndexType] = values.map(value => value.indextype -> value).toMap.get(indextype)
}