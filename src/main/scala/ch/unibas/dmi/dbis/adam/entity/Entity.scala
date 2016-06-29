package ch.unibas.dmi.dbis.adam.entity

import breeze.linalg.SparseVector
import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.datatypes.FieldTypes
import ch.unibas.dmi.dbis.adam.datatypes.FieldTypes._
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature.{SparseFeatureVector, VectorBase}
import ch.unibas.dmi.dbis.adam.datatypes.feature.FeatureVectorWrapper
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.exception.{EntityExistingException, EntityNotExistingException, EntityNotProperlyDefinedException, GeneralAdamException}
import ch.unibas.dmi.dbis.adam.handler.FlatFileHandler
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
import ch.unibas.dmi.dbis.adam.storage.engine.CatalogOperator
import ch.unibas.dmi.dbis.adam.storage.partition.PartitionMode
import ch.unibas.dmi.dbis.adam.utils.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Random, Success, Try}

/**
  * adamtwo
  *
  * Ivan Giangreco
  * October 2015
  */
case class Entity(val entityname: EntityName)(@transient implicit val ac: AdamContext) extends Serializable with Logging {
  //TODO: make entities singleton? lock on entity?

  /**
    * Gets feature data.
    *
    * @return
    */
  def featureData: Option[DataFrame] = data(typeFilter = Some(Seq(FEATURETYPE)))

  /**
    * Gets feature column and pk column; use this for indexing purposes.
    *
    * @param attribute attribute that is indexed
    * @return
    */
  private[adam] def getIndexableFeature(attribute: String)(implicit ac: AdamContext): Option[DataFrame] = data(Some(Seq(pk.name, attribute)))


  private var _data: Option[DataFrame] = None

  /**
    * Gets the full entity data.
    *
    * @return
    */
  def data(nameFilter: Option[Seq[String]] = None, typeFilter: Option[Seq[FieldType]] = None): Option[DataFrame] = {
    if (_data.isEmpty) {
      val res = schema().filterNot(_.pk).groupBy(_.handler).mapValues(_.map(_.path).map(_.get).distinct).flatMap { case (handler, paths) =>
        paths.map(path => handler.read(entityname, Some(path)).get)
      }.reduce(_.join(_, pk.name))

      _data = Some(res)
    }

    if (nameFilter.isDefined || typeFilter.isDefined) {
      val tmpData = _data

      import org.apache.spark.sql.functions.col

      tmpData.map(_.select(schema(nameFilter, typeFilter).map(attribute => col(attribute.name)): _*))
    } else {
      _data
    }
  }

  /**
    * Marks the data stale (e.g., if new data has been inserted to entity).
    */
  private def markStale(): Unit = {
    _tupleCount = None
    _data = None
  }

  /**
    * Gets the primary key.
    *
    * @return
    */
  val pk = CatalogOperator.getEntityPK(entityname)


  /**
    *
    * @return
    */
  lazy val indexes: Seq[Try[Index]] = {
    CatalogOperator.listIndexes(entityname).map(index => Index.load(index._1))
  }


  /**
    * Returns the weight set to the entity scan. The weight is used at query time to choose which query path to choose.
    */
  def scanweight(attribute: String) = CatalogOperator.getEntityWeight(entityname, attribute)

  /**
    *
    * @param weight new weights to set to entity scan
    * @return
    */
  def setScanWeight(attribute: String, weight: Option[Float] = None): Boolean = {
    CatalogOperator.updateEntityWeight(entityname, attribute, weight)
  }

  private var _tupleCount: Option[Long] = None


  /**
    * Returns number of elements in the entity.
    *
    * @return
    */
  def count: Long = {
    if (_tupleCount.isEmpty) {
      if (data().isEmpty) {
        _tupleCount = Some(0)
      } else {
        _tupleCount = Some(data().get.count())
      }
    }

    _tupleCount.get
  }


  /**
    * Gives preview of entity.
    *
    * @param k number of elements to show in preview
    * @return
    */
  def show(k: Int): Option[DataFrame] = data().map(_.limit(k))

  /**
    * Inserts data into the entity.
    *
    * @param data         data to insert
    * @param ignoreChecks whether to ignore checks
    * @return
    */
  def insert(data: DataFrame, ignoreChecks: Boolean = false): Try[Void] = {
    log.debug("inserting data into entity")

    try {
      var insertion =
        if (pk.fieldtype.equals(FieldTypes.AUTOTYPE)) {
          if (data.schema.fieldNames.contains(pk.name)) {
            return Failure(new GeneralAdamException("the field " + pk.name + " has been specified as auto and should therefore not be provided"))
          }

          val rdd = data.rdd.zipWithUniqueId.map { case (r: Row, id: Long) => Row.fromSeq(id +: r.toSeq) }
          ac.sqlContext
            .createDataFrame(
              rdd, StructType(StructField(pk.name, pk.fieldtype.datatype, false) +: data.schema.fields))
        } else {
          data
        }

      //TODO: check that schema of data inserted and schema of entity are equal

      insertion.schema.fields
        .map(field => schema(Some(Seq(field.name))))
        .filterNot(_.isEmpty).map(_.head)
        .groupBy(_.handler)
        .foreach { case (handler, attributes) =>
          val a = attributes

          val fields = if (attributes.filter(_.name == pk.name).isEmpty) {
            attributes.+:(pk)
          } else {
            attributes
          }

          import org.apache.spark.sql.functions.col
          val df = insertion.select(fields.map(attribute => col(attribute.name)): _*)

          handler.write(entityname, None, df)
        }


      if (CatalogOperator.listIndexes(entityname).nonEmpty) {
        log.warn("new data inserted, but indexes are not updated; please re-create index")
        CatalogOperator.updateIndexesToStale(entityname)
      }

      Success(null)
    } catch {
      case e: Exception => Failure(e)
    } finally {
      //mark entity stale
      markStale()
    }
  }


  def drop(): Unit = {
    schema().groupBy(_.handler).mapValues(_.map(_.path).distinct).foreach { case (handler, paths) =>
      paths.foreach { path =>
        handler.drop(entityname, path)
      }
    }
  }

  //TODO: add delete operation?

  /**
    * Returns a map of properties to the entity. Useful for printing.
    */
  def properties: Map[String, String] = {
    val lb = ListBuffer[(String, String)]()

    lb.append("scanweight" -> schema().filter(_.fieldtype == FieldTypes.FEATURETYPE).map(field => field.name -> scanweight(field.name)).map { case (name, weight) => name + "(" + weight + ")" }.mkString(","))
    lb.append("schema" -> CatalogOperator.getFields(entityname).map(field => field.name + "(" + field.fieldtype.name + ")").mkString(","))
    lb.append("indexes" -> CatalogOperator.listIndexes(entityname).mkString(", "))

    lb.toMap
  }

  private var _schema: Option[Seq[AttributeDefinition]] = None

  /**
    * Schema of the entity.
    *
    * @return
    */
  def schema(nameFilter: Option[Seq[String]] = None, typeFilter: Option[Seq[FieldType]] = None): Seq[AttributeDefinition] = {
    if (_schema.isEmpty) {
      _schema = Some(CatalogOperator.getFields(entityname))
    }

    var tmpSchema = _schema.get

    if (nameFilter.isDefined) {
      tmpSchema = tmpSchema.filter(attribute => nameFilter.get.contains(attribute.name))
    }

    if (typeFilter.isDefined) {
      tmpSchema = tmpSchema.filter(attribute => typeFilter.get.map(_.name).contains(attribute.fieldtype.name))
    }

    tmpSchema
  }


  /**
    * Checks whether query is conform to entity.
    *
    * @param query the query to be performed
    * @return
    */
  def isQueryConform(query: NearestNeighbourQuery): Boolean = {
    if (featureData.isDefined) {
      try {
        val length = featureLength(query.column)
        if (length != query.q.length) {
          log.error("expected vector of length " + length + ", but received " + query.q.length)
        }

        length == query.q.length
      } catch {
        case e: Exception => log.error("query not conform with entity", e)
          false
      }
    } else {
      false
    }
  }

  lazy val featureLength = schema().filter(_.fieldtype == FEATURETYPE).map(attribute => attribute.name -> featureData.get.first().getAs[FeatureVectorWrapper](attribute.name).vector.length).toMap

  override def equals(that: Any): Boolean =
    that match {
      case that: Entity =>
        this.entityname.equals(that.entityname)
      case _ => false
    }

  override def hashCode: Int = entityname.hashCode
}

object Entity extends Logging {
  type EntityName = EntityNameHolder

  def exists(entityname: EntityName): Boolean = CatalogOperator.existsEntity(entityname)

  /**
    * Creates an entity.
    *
    * @param entityname  name of the entity
    * @param attributes  attributes of entity
    * @param ifNotExists if set to true and the entity exists, the entity is just returned; otherwise an error is thrown
    * @return
    */
  def create(entityname: EntityName, attributes: Seq[AttributeDefinition], ifNotExists: Boolean = false)(implicit ac: AdamContext): Try[Entity] = {
    try {
      //checks
      if (exists(entityname)) {
        if (!ifNotExists) {
          return Failure(EntityExistingException())
        } else {
          return load(entityname)
        }
      }

      if (attributes.isEmpty) {
        return Failure(EntityNotProperlyDefinedException("Entity " + entityname + " will have no attributes"))
      }

      FieldNames.reservedNames.foreach { reservedName =>
        if (attributes.map(_.name).contains(reservedName)) {
          return Failure(EntityNotProperlyDefinedException("Entity defined with field " + reservedName + ", but name is reserved"))
        }
      }

      if (attributes.map(_.name).distinct.length != attributes.length) {
        return Failure(EntityNotProperlyDefinedException("Entity defined with duplicate fields."))
      }

      val allowedPkTypes = FieldTypes.values.filter(_.pk)

      if (attributes.count(_.pk) > 1) {
        return Failure(EntityNotProperlyDefinedException("Entity defined with more than one primary key"))
      } else if (!attributes.exists(_.pk)) {
        return Failure(EntityNotProperlyDefinedException("Entity defined has no primary key."))
      } else if (!attributes.filter(_.pk).forall(field => allowedPkTypes.contains(field.fieldtype))) {
        return Failure(EntityNotProperlyDefinedException("Entity defined needs a " + allowedPkTypes.map(_.name).mkString(", ") + " primary key"))
      }

      if (attributes.count(_.fieldtype == AUTOTYPE) > 1) {
        return Failure(EntityNotProperlyDefinedException("Too many auto attributes defined."))
      } else if (attributes.count(_.fieldtype == AUTOTYPE) > 0 && !attributes.filter(_.pk).forall(_.fieldtype == AUTOTYPE)) {
        return Failure(EntityNotProperlyDefinedException("Auto type only allowed in primary key."))
      }

      val pk = attributes.filter(_.pk).head

      val handlers = (attributes.filterNot(_.pk)).groupBy(_.handler)

      handlers.foreach {
        case (handler, attributes) =>
          attributes.map(_.handlerName = Some(handler.name))

          val path = handler.create(entityname, None, attributes.+:(pk))

          if (path.isSuccess) {
            attributes.map(_.path = path.get)
          } else {
            throw path.failed.get
          }
      }

      //TODO: if attributes filterNot pk length == 0 we have to still store the pk

      CatalogOperator.createEntity(entityname, attributes)

      Success(Entity(entityname)(ac))
    } catch {
      case e: Exception => {
        //TODO: possibly drop what has been already created
        Failure(e)
      }
    }
  }

  /**
    * Drops an entity.
    *
    * @param entityname name of entity
    * @param ifExists   if set to true, no error is raised if entity does not exist
    * @return
    */
  def drop(entityname: EntityName, ifExists: Boolean = false)(implicit ac: AdamContext): Try[Void] = {
    if (!exists(entityname)) {
      if (!ifExists) {
        return Failure(EntityNotExistingException())
      } else {
        return Success(null)
      }
    }

    Index.dropAll(entityname)
    Entity(entityname)(ac).drop()

    CatalogOperator.dropEntity(entityname, ifExists)

    EntityLRUCache.invalidate(entityname)
    Success(null)
  }

  /**
    * Loads an entity.
    *
    * @param entityname name of entity
    * @return
    */
  def load(entityname: EntityName, cache: Boolean = false)(implicit ac: AdamContext): Try[Entity] = {
    if (!exists(entityname)) {
      return Failure(EntityNotExistingException("Entity " + entityname + " is not existing."))
    }

    val entity = EntityLRUCache.get(entityname)

    if (cache) {
      //TODO: cache data for each handler
    }

    entity
  }

  /**
    * Loads the entityname metadata without loading the data itself yet.
    *
    * @param entityname name of entity
    * @return
    */
  private[entity] def loadEntityMetaData(entityname: EntityName)(implicit ac: AdamContext): Try[Entity] = {
    if (!exists(entityname)) {
      return Failure(EntityNotExistingException())
    }

    try {
      val pk = CatalogOperator.getEntityPK(entityname)

      Success(Entity(entityname)(ac))
    } catch {
      case e: Exception => Failure(e)
    }
  }

  /**
    * Compresses the entity by turning vectors to sparse.
    *
    * @param entity        entity
    * @param attributename name of attribute
    * @return
    */
  def sparsify(entity: Entity, attributename: String)(implicit ac: AdamContext): Try[Entity] = {
    try {
      if (entity.featureData.isEmpty) {
        return Failure(new GeneralAdamException("no feature data available for performing sparsifying"))
      }

      var data = entity.data().get
      val convertToSparse = udf((c: FeatureVectorWrapper) => {
        val vec = c.vector

        if (vec.isInstanceOf[SparseFeatureVector]) {
          c
        } else {
          var numNonZeros = 0
          var k = 0
          numNonZeros = {
            var nnz = 0
            vec.foreach { v =>
              if (math.abs(v) > 1E-10) {
                nnz += 1
              }
            }
            nnz
          }

          //TODO: possibly check here if nnz > 0.5 length then do not translate to sparse

          val ii = new Array[Int](numNonZeros)
          val vv = new Array[VectorBase](numNonZeros)
          k = 0

          vec.foreachPair { (i, v) =>
            if (math.abs(v) > 1E-10) {
              ii(k) = i
              vv(k) = v
              k += 1
            }
          }

          if (ii.nonEmpty) {
            new FeatureVectorWrapper(ii, vv, vec.size)
          } else {
            FeatureVectorWrapper(SparseVector.zeros(vec.size))
          }
        }
      })

      data = data.withColumn("conv-" + attributename, convertToSparse(data(attributename)))
      data = data.drop(attributename).withColumnRenamed("conv-" + attributename, attributename)

      val attribute = entity.schema(Some(Seq(attributename))).head
      val oldPath = attribute.path.get

      data = data.select(entity.schema().filter(_.handler == FlatFileHandler).map(attribute => col(attribute.name)): _*)

      var newPath = ""

      do {
        newPath = oldPath + "-sp" + Random.nextInt(999)
      } while (attribute.handler.exists(entity.entityname, Some(newPath)).get)

      attribute.handler.write(entity.entityname, Some(newPath), data, SaveMode.ErrorIfExists)
      CatalogOperator.updateEntityFeaturePath(entity.entityname, attribute.handler.name, newPath)
      attribute.handler.drop(entity.entityname, Some(oldPath))

      entity.markStale()
      EntityLRUCache.invalidate(entity.entityname)

      Success(entity)
    }

    catch {
      case e: Exception => Failure(e)
    }
  }

  /**
    * Repartition data.
    *
    * @param entity      entity
    * @param nPartitions number of partitions
    * @param join        other dataframes to join on, on which the partitioning is performed
    * @param cols        columns to partition on, if not specified the primary key is used
    * @param mode        partition mode
    * @return
    */
  def repartition(entity: Entity, nPartitions: Int, join: Option[DataFrame], cols: Option[Seq[String]], mode: PartitionMode.Value)(implicit ac: AdamContext): Try[Entity] = {
    if (entity.featureData.isEmpty) {
      return Failure(new GeneralAdamException("no feature data available for performing sparsifying"))
    }

    val attributes = entity.schema(cols)
    if (!attributes.forall(_.handler != FlatFileHandler)) {
      return Failure(new GeneralAdamException("repartitioning is only possible for data in the feature database handler"))
    }

    var data = entity.data().get

    //TODO: possibly add own partitioner
    //data.map(r => (r.getAs[Any](cols.get.head), r)).partitionBy(new HashPartitioner())

    if (join.isDefined) {
      data = data.join(join.get, entity.pk.name)
    }

    data = if (cols.isDefined) {
      val entityColNames = data.schema.map(_.name)
      if (!cols.get.forall(name => entityColNames.contains(name))) {
        Failure(throw new GeneralAdamException("one of the columns " + cols.mkString(",") + " is not existing in entity " + entity.entityname + entityColNames.mkString("(", ",", ")")))
      }

      data.repartition(nPartitions, cols.get.map(data(_)): _*)
    } else {
      data.repartition(nPartitions, data(entity.pk.name))
    }

    data = data.select(entity.schema().filter(_.handler == FlatFileHandler).map(attribute => col(attribute.name)): _*)

    mode match {
      case PartitionMode.REPLACE_EXISTING =>
        if (attributes.length > 1 && attributes.map(_.path).map(_.get).distinct.length > 1) {
          return Failure(new GeneralAdamException("the repartitioner has received columns from more than one path"))
        }

        val oldPath = attributes.head.path.get

        var newPath = ""

        do {
          newPath = oldPath + "-rep" + Random.nextInt(999)
        } while (attributes.head.handler.exists(entity.entityname, Some(newPath)).get)

        attributes.head.handler.write(entity.entityname, Some(newPath), data, SaveMode.ErrorIfExists)
        CatalogOperator.updateEntityFeaturePath(entity.entityname, attributes.head.handler.name, newPath)
        attributes.head.handler.drop(entity.entityname, Some(oldPath))

        entity.markStale()
        EntityLRUCache.invalidate(entity.entityname)

        return Success(entity)

      case _ => Failure(new GeneralAdamException("partitioning mode unknown"))
    }
  }

  /**
    * Lists names of all entities.
    *
    * @return name of entities
    */
  def list: Seq[EntityName] = CatalogOperator.listEntities()
}