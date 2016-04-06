package ch.unibas.dmi.dbis.adam.index.structures

import ch.unibas.dmi.dbis.adam.http._

/**
  * adamtwo
  *
  * Ivan Giangreco
  * October 2015
  */
object IndexTypes {
  //TODO: clean other parts of code and integrate with this code as to have one single location where indexer, index, indextype, etc. is defined

  sealed abstract class IndexType(val name: String, val indextype: grpc.IndexType) extends Serializable

  case object ECPINDEX extends IndexType("ecp", grpc.IndexType.ecp)

  case object LSHINDEX extends IndexType("lsh", grpc.IndexType.lsh)

  case object PQINDEX extends IndexType("pq", null)

  case object SHINDEX extends IndexType("sh", grpc.IndexType.sh)

  case object VAFINDEX extends IndexType("vaf", grpc.IndexType.vaf)

  case object VAVINDEX extends IndexType("vav", grpc.IndexType.vav)

  /**
    *
    */
  val values = Seq(ECPINDEX, LSHINDEX, PQINDEX, SHINDEX, VAFINDEX, VAVINDEX)

  /**
    *
    * @param s
    * @return
    */
  def withName(s : String) : Option[IndexType] = values.map(value => value.name -> value).toMap.get(s)

  /**
    *
    * @param indextype
    * @return
    */
  def withIndextype(indextype: grpc.IndexType) : Option[IndexType] = values.map(value => value.indextype -> value).toMap.get(indextype)
}