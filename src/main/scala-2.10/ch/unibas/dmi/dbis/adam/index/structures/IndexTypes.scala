package ch.unibas.dmi.dbis.adam.index.structures

import ch.unibas.dmi.dbis.adam.http.grpc.adam.IndexMessage

/**
  * adamtwo
  *
  * Ivan Giangreco
  * October 2015
  */
object IndexTypes {
  sealed abstract class IndexType(val name: String, val indextype: IndexMessage.IndexType)

  case object ECPINDEX extends IndexType("ecp", IndexMessage.IndexType.ecp)

  case object LSHINDEX extends IndexType("lsh", IndexMessage.IndexType.lsh)

  case object SHINDEX extends IndexType("sh", IndexMessage.IndexType.sh)

  case object VAFINDEX extends IndexType("vaf", IndexMessage.IndexType.vaf)

  case object VAVINDEX extends IndexType("vav", IndexMessage.IndexType.vav)

  /**
    *
    */
  val values = Seq(ECPINDEX, LSHINDEX, SHINDEX, VAFINDEX, VAVINDEX)

  /**
    *
    * @param s
    * @return
    */
  def withName(s : String) : Option[IndexType] = values.map(value => value.name -> value).toMap.get(s)
}