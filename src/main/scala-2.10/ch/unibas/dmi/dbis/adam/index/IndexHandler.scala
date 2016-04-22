package ch.unibas.dmi.dbis.adam.index

import ch.unibas.dmi.dbis.adam.config.{AdamConfig, FieldNames}
import ch.unibas.dmi.dbis.adam.datatypes.feature.FeatureVectorWrapper
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.exception.{GeneralAdamException, IndexNotExistingException}
import ch.unibas.dmi.dbis.adam.index.Index.{IndexName, IndexTypeName}
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.index.structures.ecp.ECPIndex
import ch.unibas.dmi.dbis.adam.index.structures.lsh.LSHIndex
import ch.unibas.dmi.dbis.adam.index.structures.pq.PQIndex
import ch.unibas.dmi.dbis.adam.index.structures.sh.SHIndex
import ch.unibas.dmi.dbis.adam.index.structures.va.VAIndex
import ch.unibas.dmi.dbis.adam.main.{AdamContext, SparkStartup}
import ch.unibas.dmi.dbis.adam.storage.engine.CatalogOperator
import ch.unibas.dmi.dbis.adam.storage.partitions.PartitionOptions
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

import scala.util.{Random, Failure, Success, Try}

/**
  * adampro
  *
  * Ivan Giangreco
  * April 2016
  */
object IndexHandler {
  val log = Logger.getLogger(getClass.getName)

  private val storage = SparkStartup.indexStorage

  private val MINIMUM_NUMBER_OF_TUPLE = 10

  private val lock = new Object()


  /**
    * Creates an index that is unique and which folows the naming rules of indexes.
    *
    * @param entityname
    * @param indextype
    * @return
    */
  private def createIndexName(entityname: EntityName, indextype: IndexTypeName): String = {
    val indexes = CatalogOperator.listIndexes(entityname, indextype).map(_._1)

    var indexname = ""

    var i = indexes.length
    do {
      indexname = entityname + "_" + indextype.name + "_" + i
      i += 1
    } while (indexes.contains(indexname))

    indexname
  }


  /**
    * Creates an index.
    *
    * @param entity
    * @param indexgenerator generator to create index
    * @return index
    */
  def createIndex(entity: Entity, indexgenerator: IndexGenerator)(implicit ac: AdamContext): Try[Index] = {
    lock.synchronized {
      val count = entity.count
      if (count < MINIMUM_NUMBER_OF_TUPLE) {
        log.error("not enough tuples for creating index, needs at least " + MINIMUM_NUMBER_OF_TUPLE + " but has only " + count)
        return Failure(new GeneralAdamException("not enough tuples for index"))
      }

      val indexname = createIndexName(entity.entityname, indexgenerator.indextypename)
      val rdd: RDD[IndexingTaskTuple] = entity.getFeaturedata.map { x => IndexingTaskTuple(x.getLong(0), x.getAs[FeatureVectorWrapper](1).vector) }
      val index = indexgenerator.index(indexname, entity.entityname, rdd)
      index.df = index
        .df
        .repartition(AdamConfig.defaultNumberOfPartitions)
        .withColumnRenamed("id", FieldNames.idColumnName)
        .withColumnRenamed("value", FieldNames.featureIndexColumnName)
      storage.write(indexname, index.df)
      CatalogOperator.createIndex(indexname, indexname, entity.entityname, indexgenerator.indextypename, index.metadata)
      Success(index)
    }
  }

  /**
    * Checks whether index exists.
    *
    * @param indexname
    * @return
    */
  def exists(indexname: IndexName)(implicit ac: AdamContext): Boolean = CatalogOperator.existsIndex(indexname)

  /**
    * Lists indexes.
    *
    * @param entityname
    * @param indextypename
    * @return
    */
  def list(entityname: EntityName = null, indextypename: IndexTypeName = null)(implicit ac: AdamContext): Seq[(IndexName, IndexTypeName, Float)] = {
    CatalogOperator.listIndexes(entityname, indextypename)
  }

  /**
    * Loads index into cache.
    *
    * @param indexname
    * @return
    */
  def load(indexname: IndexName, cache: Boolean = false)(implicit ac: AdamContext): Try[Index] = {
    if (!IndexLRUCache.contains(indexname) && !exists(indexname)) {
      Failure(new IndexNotExistingException())
    }

    val index = IndexLRUCache.get(indexname)

    if (cache) {
      index.get.df.rdd.setName(indexname).cache()
    }

    index
  }

  /**
    * Loads the index metadata without loading the data itself yet.
    *
    * @param indexname
    * @return
    */
  private[index] def loadIndexMetaData(indexname: IndexName)(implicit ac: AdamContext): Try[Index] = {
    if (!exists(indexname)) {
      Failure(IndexNotExistingException())
    }

    val df = storage.read(CatalogOperator.getIndexPath(indexname))
    val entityname = CatalogOperator.getEntitynameFromIndex(indexname)
    val meta = CatalogOperator.getIndexMeta(indexname)

    val indextypename = CatalogOperator.getIndexTypeName(indexname)

    val index = indextypename match {
      case IndexTypes.ECPINDEX => ECPIndex(indexname, entityname, df, meta)
      case IndexTypes.LSHINDEX => LSHIndex(indexname, entityname, df, meta)
      case IndexTypes.PQINDEX => PQIndex(indexname, entityname, df, meta)
      case IndexTypes.SHINDEX => SHIndex(indexname, entityname, df, meta)
      case IndexTypes.VAFINDEX => VAIndex(indexname, entityname, df, meta)
      case IndexTypes.VAVINDEX => VAIndex(indexname, entityname, df, meta)
    }

    Success(index)
  }

  /**
    * Drops an index.
    *
    * @param indexname
    * @return true if index was dropped
    */
  def drop(indexname: IndexName)(implicit ac: AdamContext): Try[Void] = {
    lock.synchronized {
      CatalogOperator.dropIndex(indexname)
      storage.drop(indexname)
      Success(null)
    }
  }

  /**
    * Drops all indexes for a given entity.
    *
    * @param entityname
    * @return
    */
  def dropAll(entityname: EntityName)(implicit ac: AdamContext): Try[Void] = {
    val indexes = CatalogOperator.dropAllIndexes(entityname)

    indexes.foreach {
      index => storage.drop(index)
    }

    Success(null)
  }


  /**
    *
    * @param index
    * @param n
    * @param join
    * @param cols
    * @param option
    * @return
    */
  def repartition(index: Index, n: Int, join: Option[DataFrame], cols: Option[Seq[String]], option: PartitionOptions.Value)(implicit ac: AdamContext): Try[Index] = {
    var data = index.df
    //TODO: possibly add own partitioner
    //data.map(r => (r.getAs(cols.get.head), r)).partitionBy(new HashPartitioner())

    if (join.isDefined) {
      data = data.join(join.get, FieldNames.idColumnName)
    }

    data = if (cols.isDefined) {
      //TODO: check that cols are existent in dataframe
      data.repartition(n, cols.get.map(data(_)): _*)
    } else {
      data.repartition(n)
    }

    if (join.isDefined) {
      data = data.select(FieldNames.idColumnName, FieldNames.featureIndexColumnName)
    }

    option match {
      case PartitionOptions.CREATE_NEW =>
        val newName = createIndexName(index.entityname, index.indextypename)
        storage.write(newName, data)
        CatalogOperator.createIndex(newName, newName, index.entityname, index.indextypename, index.metadata)
        IndexLRUCache.invalidate(newName)

        return Success(load(newName).get)

      case PartitionOptions.CREATE_TEMP =>
        val newName = createIndexName(index.entityname, index.indextypename)

        val newIndex = index.copy(Some(newName))
        newIndex.df = data

        IndexLRUCache.put(newName, newIndex)
        return Success(newIndex)

      case PartitionOptions.REPLACE_EXISTING =>
        val oldPath = index.path

        index.df = data
        var newPath = ""

        do {
          newPath = index.indexname + "-rep" + Random.nextInt
        } while (SparkStartup.indexStorage.exists(newPath))

        SparkStartup.indexStorage.write(newPath, data)
        CatalogOperator.updateIndexPath(index.indexname, newPath)
        SparkStartup.indexStorage.drop(oldPath)

        IndexLRUCache.invalidate(index.indexname)

        return Success(index)
    }
  }

  /**
    *
    * @param indexname
    * @param weight
    */
  def setWeight(indexname: IndexName, weight: Float): Boolean = {
    CatalogOperator.updateIndexWeight(indexname, weight)
  }

  /**
    * Resets all weights to default weight.
    *
    * @param entityname
    */
  def resetAllWeights(entityname: EntityName): Boolean = {
    CatalogOperator.resetIndexWeight(entityname)
  }
}

