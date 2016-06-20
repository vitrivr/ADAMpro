package ch.unibas.dmi.dbis.adam.index.structures

import java.io.Serializable

import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.http._
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.index.structures.ecp.{ECPIndex, ECPIndexer}
import ch.unibas.dmi.dbis.adam.index.structures.lsh.{LSHIndex, LSHIndexer}
import ch.unibas.dmi.dbis.adam.index.structures.mi.{MIIndex, MIIndexer}
import ch.unibas.dmi.dbis.adam.index.structures.pq.{PQIndex, PQIndexer}
import ch.unibas.dmi.dbis.adam.index.structures.sh.{SHIndex, SHIndexer}
import ch.unibas.dmi.dbis.adam.index.structures.va.{VAIndex, VAFIndexer, VAVIndexer}
import ch.unibas.dmi.dbis.adam.index.{Index, IndexGenerator}
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.distance.DistanceFunction
import org.apache.spark.sql.DataFrame

/**
  * adamtwo
  *
  * Ivan Giangreco
  * October 2015
  */
object IndexTypes {

  sealed abstract class IndexType(val name: String, val indextype: grpc.IndexType,
                                  val index: (IndexName, EntityName, DataFrame, Any, AdamContext) => Index,
                                  val indexer: (DistanceFunction, Map[String, String], AdamContext) => IndexGenerator
                                 ) extends Serializable

  case object ECPINDEX extends IndexType("ecp", grpc.IndexType.ecp,
    (indexname, entityname, df, meta, ac) => ECPIndex.apply(indexname, entityname, df, meta)(ac),
    (df, options, ac) => ECPIndexer.apply(df, options)(ac))

  case object LSHINDEX extends IndexType("lsh", grpc.IndexType.lsh,
    (indexname, entityname, df, meta, ac) => LSHIndex.apply(indexname, entityname, df, meta)(ac),
    (df, options, ac) => LSHIndexer.apply(df, options)(ac))

  case object MIINDEX extends IndexType("mi", grpc.IndexType.mi,
    (indexname, entityname, df, meta, ac) => MIIndex.apply(indexname, entityname, df, meta)(ac),
    (df, options, ac) => MIIndexer.apply(df, options)(ac))

  case object PQINDEX extends IndexType("pq", grpc.IndexType.pq,
    (indexname, entityname, df, meta, ac) => PQIndex.apply(indexname, entityname, df, meta)(ac),
    (df, options, ac) => PQIndexer.apply(df, options)(ac))

  case object SHINDEX extends IndexType("sh", grpc.IndexType.sh,
    (indexname, entityname, df, meta, ac) => SHIndex.apply(indexname, entityname, df, meta)(ac),
    (df, options, ac) => SHIndexer.apply(df, options)(ac))

  case object VAFINDEX extends IndexType("vaf", grpc.IndexType.vaf,
    (indexname, entityname, df, meta, ac) => VAIndex.apply(indexname, entityname, df, meta)(ac),
    (df, options, ac) => VAFIndexer.apply(df, options)(ac))

  case object VAVINDEX extends IndexType("vav", grpc.IndexType.vav,
    (indexname, entityname, df, meta, ac) => VAIndex.apply(indexname, entityname, df, meta)(ac),
    (df, options, ac) => VAVIndexer.apply(df, options)(ac))

  /**
    *
    */
  val values = Seq(ECPINDEX, LSHINDEX, MIINDEX, PQINDEX, SHINDEX, VAFINDEX, VAVINDEX)

  /**
    *
    * @param s
    * @return
    */
  //TODO: add checks
  def withName(s: String): Option[IndexType] = {
    values.map(value => value.name -> value).toMap.get(s)
  }

  /**
    *
    * @param indextype
    * @return
    */
  //TODO: add checks
  def withIndextype(indextype: grpc.IndexType): Option[IndexType] = {
    values.map(value => value.indextype -> value).toMap.get(indextype)
  }

  /**
    *
    * @param s
    * @return
    */
  implicit def fromString(s: String): IndexType = IndexTypes.withName(s).get
}