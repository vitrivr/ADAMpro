package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.datatypes.feature.{FeatureVectorWrapper, Feature}
import Feature.FeatureVector
import ch.unibas.dmi.dbis.adam.index.Index.IndexTypeName
import ch.unibas.dmi.dbis.adam.index.structures.IndexStructures
import ch.unibas.dmi.dbis.adam.index.structures.ecp.ECPIndexer
import ch.unibas.dmi.dbis.adam.index.structures.lsh.LSHIndexer
import ch.unibas.dmi.dbis.adam.index.structures.sh.{SHIndexer, SHIndexer$}
import ch.unibas.dmi.dbis.adam.index.structures.va.{VAVIndexer, VAFIndexer, VAVIndexer$, VAFIndexer$}
import ch.unibas.dmi.dbis.adam.index.{Index, IndexGenerator, IndexingTaskTuple}
import ch.unibas.dmi.dbis.adam.query.distance.{DistanceFunction, MinkowskiDistance}
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity._
import org.apache.spark.rdd.RDD

/**
  * adamtwo
  *
  * Index operation. Creates an index.
  *
  * Ivan Giangreco
  * August 2015
  */
object IndexOp {
  /**
    * Creates an index.
    *
    * @param entityname
    * @param indextype string representation of index type to use for indexing
    * @param distance distance function to use
    * @param properties further index specific properties
    */
  def apply(entityname: EntityName, indextype: String, distance: DistanceFunction, properties: Map[String, String]): Unit =
    apply(entityname, IndexStructures.withName(indextype), distance, properties)

  /**
    * Creates an index.
    *
    * @param entityname
    * @param indextypename index type to use for indexing
    * @param distance distance function to use
    * @param properties further index specific properties
    */
  def apply(entityname: EntityName, indextypename: IndexTypeName, distance: DistanceFunction, properties: Map[String, String]): Unit = {
    val entity = Entity.load(entityname)

    val generator: IndexGenerator = indextypename match {
      case IndexStructures.ECP => ECPIndexer(distance)
      case IndexStructures.LSH => LSHIndexer(distance, properties)
      case IndexStructures.SH => SHIndexer(entity.getFeaturedata.first().getAs[FeatureVectorWrapper](1).value.length)
      case IndexStructures.VAF => VAFIndexer(distance.asInstanceOf[MinkowskiDistance], properties)
      case IndexStructures.VAV => VAVIndexer(entity.getFeaturedata.first().getAs[FeatureVectorWrapper](1).value.length, distance.asInstanceOf[MinkowskiDistance], properties)
    }

    Index.createIndex(entity, generator)
  }
}
