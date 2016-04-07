package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.api.CountOp._
import ch.unibas.dmi.dbis.adam.datatypes.feature.{FeatureVectorWrapper, Feature}
import Feature.FeatureVector
import ch.unibas.dmi.dbis.adam.entity.Entity.apply
import ch.unibas.dmi.dbis.adam.index.Index.IndexTypeName
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.index.structures.ecp.ECPIndexer
import ch.unibas.dmi.dbis.adam.index.structures.lsh.LSHIndexer
import ch.unibas.dmi.dbis.adam.index.structures.pq.PQIndexer
import ch.unibas.dmi.dbis.adam.index.structures.sh.{SHIndexer, SHIndexer$}
import ch.unibas.dmi.dbis.adam.index.structures.va.{VAVIndexer, VAFIndexer, VAVIndexer$, VAFIndexer$}
import ch.unibas.dmi.dbis.adam.index.{Index, IndexGenerator, IndexingTaskTuple}
import ch.unibas.dmi.dbis.adam.query.distance.{DistanceFunction, MinkowskiDistance}
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity._
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD

import scala.util.Try

/**
  * adamtwo
  *
  * Index operation. Creates an index.
  *
  * Ivan Giangreco
  * August 2015
  */
object IndexOp {
  val log = Logger.getLogger(getClass.getName)

  /**
    * Creates an index.
    *
    * @param entityname
    * @param indextype  string representation of index type to use for indexing
    * @param distance   distance function to use
    * @param properties further index specific properties
    */
  def apply(entityname: EntityName, indextype: String, distance: DistanceFunction, properties: Map[String, String]): Try[Index] = {
    log.debug("perform create index operation")
    apply(entityname, IndexTypes.withName(indextype).get, distance, properties)
  }

  /**
    * Creates an index.
    *
    * @param entityname
    * @param indextypename index type to use for indexing
    * @param distance      distance function to use
    * @param properties    further index specific properties
    */
  def apply(entityname: EntityName, indextypename: IndexTypeName, distance: DistanceFunction, properties: Map[String, String] = Map()): Try[Index] = {
    log.debug("perform create index operation")

    val entity = Entity.load(entityname)

    val generator: IndexGenerator = indextypename match {
      case IndexTypes.ECPINDEX => ECPIndexer(distance)
      case IndexTypes.LSHINDEX => LSHIndexer(distance, properties)
      case IndexTypes.PQINDEX => PQIndexer(properties)
      case IndexTypes.SHINDEX => SHIndexer(entity.get.getFeaturedata.first().getAs[FeatureVectorWrapper](1).vector.length)
      case IndexTypes.VAFINDEX => VAFIndexer(distance.asInstanceOf[MinkowskiDistance], properties)
      case IndexTypes.VAVINDEX => VAVIndexer(entity.get.getFeaturedata.first().getAs[FeatureVectorWrapper](1).vector.length, distance.asInstanceOf[MinkowskiDistance], properties)
    }

    Index.createIndex(entity.get, generator)
  }

  /**
    * Creates indexes of all available types.
    *
    * @param entityname
    * @param distance   distance function to use
    * @param properties further index specific properties
    */
  def generateAll(entityname: EntityName, distance: DistanceFunction, properties: Map[String, String] = Map()): Boolean = {
    IndexTypes.values.foreach { indextypename =>
      apply(entityname, indextypename, distance, properties)
    }
    true
  }
}
