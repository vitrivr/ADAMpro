package org.vitrivr.adampro.index.structures

import java.io.Serializable

import org.vitrivr.adampro.grpc._
import org.vitrivr.adampro.index.structures.ecp.{ECPIndex, ECPIndexGeneratorFactory}
import org.vitrivr.adampro.index.structures.lsh.{LSHIndex, LSHIndexGeneratorFactory}
import org.vitrivr.adampro.index.structures.mi.{MIIndex, MIIndexGeneratorFactory}
import org.vitrivr.adampro.index.structures.pq.{PQIndex, PQIndexGeneratorFactory}
import org.vitrivr.adampro.index.structures.sh.{SHIndex, SHIndexGeneratorFactory}
import org.vitrivr.adampro.index.structures.va._
import org.vitrivr.adampro.index.{Index, IndexGeneratorFactory}

/**
  * adamtwo
  *
  * Ivan Giangreco
  * October 2015
  */
object IndexTypes {

  sealed abstract class IndexType(val name: String, val indextype: grpc.IndexType, val indexClass: Class[_ <: Index], val indexGeneratorFactoryClass: Class[_ <: IndexGeneratorFactory]) extends Serializable


  case object ECPINDEX extends IndexType("ecp", grpc.IndexType.ecp, classOf[ECPIndex], classOf[ECPIndexGeneratorFactory])

  case object LSHINDEX extends IndexType("lsh", grpc.IndexType.lsh, classOf[LSHIndex], classOf[LSHIndexGeneratorFactory])

  case object MIINDEX extends IndexType("mi", grpc.IndexType.mi, classOf[MIIndex], classOf[MIIndexGeneratorFactory])

  case object PQINDEX extends IndexType("pq", grpc.IndexType.pq, classOf[PQIndex], classOf[PQIndexGeneratorFactory])

  case object SHINDEX extends IndexType("sh", grpc.IndexType.sh, classOf[SHIndex], classOf[SHIndexGeneratorFactory])

  case object VAFINDEX extends IndexType("vaf", grpc.IndexType.vaf, classOf[VAIndex], classOf[VAFIndexGeneratorFactory])

  case object VAVINDEX extends IndexType("vav", grpc.IndexType.vav, classOf[VAIndex], classOf[VAVIndexGeneratorFactory])

  case object VAPLUSINDEX extends IndexType("vap", grpc.IndexType.vap, classOf[VAPlusIndex], classOf[VAPlusIndexGeneratorFactory])


  /**
    *
    */
  val values = Seq(ECPINDEX, LSHINDEX, MIINDEX, PQINDEX, SHINDEX, VAFINDEX, VAVINDEX, VAPLUSINDEX)

  /**
    *
    * @param s
    * @return
    */
  def withName(s: String): Option[IndexType] = {
    values.map(value => value.name -> value).toMap.get(s)
  }

  /**
    *
    * @param indextype
    * @return
    */
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