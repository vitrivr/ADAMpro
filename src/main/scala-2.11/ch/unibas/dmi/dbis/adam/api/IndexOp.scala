package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature
import Feature.FeatureVector
import ch.unibas.dmi.dbis.adam.index.Index.IndexTypeName
import ch.unibas.dmi.dbis.adam.index.structures.IndexStructures
import ch.unibas.dmi.dbis.adam.index.structures.ecp.ECPIndexer
import ch.unibas.dmi.dbis.adam.index.structures.lsh.LSHIndexer
import ch.unibas.dmi.dbis.adam.index.structures.sh.{SHIndexer, SHIndexer$}
import ch.unibas.dmi.dbis.adam.index.structures.va.{VAVIndexer, VAFIndexer, VAVIndexer$, VAFIndexer$}
import ch.unibas.dmi.dbis.adam.index.{Index, IndexGenerator, IndexerTuple}
import ch.unibas.dmi.dbis.adam.query.distance.{DistanceFunction, MinkowskiDistance}
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity._
import org.apache.spark.rdd.RDD

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
object IndexOp {
  /**
   *
   * @param tablename
   * @param indextype
   * @param properties
   */
  def apply(tablename : EntityName, indextype : String, distance : DistanceFunction, properties : Map[String, String]): Unit = {
    val indextypename = IndexStructures.withName(indextype)
    apply(tablename, indextypename, distance, properties)
  }

  /**
   *
   * @param tablename
   * @param indextypename
   * @param properties
   */
  def apply(tablename : EntityName, indextypename : IndexTypeName, distance : DistanceFunction, properties : Map[String, String]): Unit = {
    val table = Entity.retrieveEntity(tablename)

    val data: RDD[IndexerTuple] = table.featuresRDD.map { x => IndexerTuple(x.getLong(0), x.getAs[FeatureVector](1)) }

    val generator : IndexGenerator = indextypename match {
      case IndexStructures.ECP => ECPIndexer(properties, distance, data)
      case IndexStructures.LSH => LSHIndexer(properties, distance, data)
      case IndexStructures.SH => SHIndexer(properties, data)
      case IndexStructures.VAF => VAFIndexer(properties, distance.asInstanceOf[MinkowskiDistance], data)
      case IndexStructures.VAV => VAVIndexer(properties, distance.asInstanceOf[MinkowskiDistance], data)
    }

    Index.createIndex(table, generator)
  }
}
