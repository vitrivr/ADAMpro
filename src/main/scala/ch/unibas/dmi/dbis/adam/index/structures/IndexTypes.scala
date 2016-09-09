package ch.unibas.dmi.dbis.adam.index.structures

import java.io.Serializable

import ch.unibas.dmi.dbis.adam.index.structures.ecp.{ECPIndex, ECPIndexGeneratorFactory}
import ch.unibas.dmi.dbis.adam.index.structures.lsh.{LSHIndex, LSHIndexGeneratorFactory}
import ch.unibas.dmi.dbis.adam.index.structures.mi.{MIIndex, MIIndexGeneratorFactory}
import ch.unibas.dmi.dbis.adam.index.structures.pq.{PQIndex, PQIndexGeneratorFactory}
import ch.unibas.dmi.dbis.adam.index.structures.sh.{SHIndex, SHIndexGeneratorFactory}
import ch.unibas.dmi.dbis.adam.index.structures.va._
import ch.unibas.dmi.dbis.adam.index.{Index, IndexGeneratorFactory}
import org.vitrivr.adam.grpc._

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

  /**
    *
    */
  val values = Seq(ECPINDEX, LSHINDEX, MIINDEX, PQINDEX, SHINDEX, VAFINDEX, VAVINDEX)

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