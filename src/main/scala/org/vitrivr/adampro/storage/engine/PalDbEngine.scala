package org.vitrivr.adampro.storage.engine

import java.io._
import java.util.Map.Entry
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.StampedLock

import com.linkedin.paldb.api.{PalDB, Serializer}
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.vitrivr.adampro.data.datatypes.AttributeTypes
import org.vitrivr.adampro.data.datatypes.TupleID.TupleID
import org.vitrivr.adampro.data.entity.AttributeDefinition
import org.vitrivr.adampro.process.SharedComponentContext
import org.vitrivr.adampro.query.query.Predicate
import org.xerial.snappy.Snappy

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * August 2017
  */
class PalDbEngine(private val path: String)(@transient override implicit val ac: SharedComponentContext) extends Engine()(ac) with Serializable {
  override val name = "paldb"

  override def supports = Seq(AttributeTypes.AUTOTYPE, AttributeTypes.INTTYPE, AttributeTypes.LONGTYPE, AttributeTypes.STRINGTYPE, AttributeTypes.VECTORTYPE)

  override def specializes = Seq(AttributeTypes.VECTORTYPE)

  override val repartitionable = false

  private case class DBStatus(lock: StampedLock)

  private val connections = new ConcurrentHashMap[String, DBStatus]()

  private lazy val configuration = {
    val configuration = PalDB.newConfiguration
    configuration.registerSerializer(new RowSerializer())
    configuration
  }


  /**
    *
    * @param props
    */
  def this(props: Map[String, String])(implicit ac: SharedComponentContext) {
    this(props.get("path").get)(ac)
  }

  /**
    *
    * @param storename
    * @return
    */
  private def getPath(storename: String) = ac.config.cleanPath(path) + "/" + storename + ".paldb"

  /**
    *
    * @param storename
    * @return
    */
  private def getLock(storename: String): StampedLock = {
    connections.synchronized({
      if (!connections.containsKey(storename)) {
        val lock = new StampedLock()
        connections.putIfAbsent(storename, DBStatus(lock))
        lock
      } else {
        val ret = connections.get(storename)
        ret.lock
      }
    })
  }

  /**
    *
    * @param storename
    */
  private def removeLock(storename: String): Unit = {
    connections.synchronized({
      if (connections.contains(storename)) {
        val ret = connections.get(storename)

        if (!ret.lock.isWriteLocked && !ret.lock.isReadLocked) {
          connections.remove(storename)
        }
      }
    })
  }

  /**
    * Create the entity.
    *
    * @param storename  adapted entityname to store feature to
    * @param attributes attributes of the entity (w.r.t. handler)
    * @param params     creation parameters
    * @return options to store
    */
  override def create(storename: String, attributes: Seq[AttributeDefinition], params: Map[String, String])(implicit ac: SharedComponentContext): Try[Map[String, String]] = {
    try {
      val lock = getLock(storename).asWriteLock()

      lock.lock()
      val writer = PalDB.createWriter(new File(getPath(storename)), configuration)
      writer.close()

      lock.unlock()

      Success(Map())
    } catch {
      case e: Exception =>
        log.error("fatal error when opening connection to paldb", e)
        Failure(e)
    } finally {
      removeLock(storename)
    }
  }


  /**
    * Check if entity exists.
    *
    * @param storename adapted entityname to store feature to
    * @return
    */
  override def exists(storename: String)(implicit ac: SharedComponentContext): Try[Boolean] = {
    try {
      Success(new File(getPath(storename)).exists())
    } catch {
      case e: Exception => Failure(e)
    }
  }


  /**
    * Read entity.
    *
    * @param storename  adapted entityname to store feature to
    * @param attributes the attributes to read
    * @param predicates filtering predicates (only applied if possible)
    * @param params     reading parameters
    * @return
    */
  def read(storename: String, attributes: Seq[AttributeDefinition], predicates: Seq[Predicate], params: Map[String, String])(implicit ac: SharedComponentContext): Try[DataFrame] = {
    try {
      val pk = attributes.filter(_.pk).head

      val rdd = if (predicates.nonEmpty) {
        val pkpredicates = predicates.filter(_.attribute == pk).headOption

        if (pkpredicates.isEmpty || predicates.size > 1) {
          log.warn("leveldb can only filter by primary key, but predicate contains other fields too")
        }

        if (pkpredicates.get.operator.isDefined && pkpredicates.get.operator.get != "=") {
          log.error("leveldb can only check for equality in predicates")
        }

        val lb = new ListBuffer[Row]()

        val lock = getLock(storename).asReadLock()
        lock.lock()

        val reader = PalDB.createReader(new File(getPath(storename)), configuration)

        pkpredicates.get.values.foreach { id =>
          lb += reader.get[Row](id.asInstanceOf[TupleID])
        }

        reader.close()
        lock.unlock()

        ac.sc.parallelize(lb)
      } else {
        val lock = getLock(storename).asReadLock()
        lock.lock()

        val reader = PalDB.createReader(new File(getPath(storename)), configuration)

        import scala.collection.JavaConverters._
        val iterable: java.lang.Iterable[Entry[TupleID, GenericRowWithSchema]] = reader.iterable()
        val it = iterable.asScala.iterator

        val lb = new ListBuffer[Row]()

        while (it.hasNext) {
          lb += it.next.getValue()
        }

        reader.close()
        lock.unlock()

        ac.sc.parallelize(lb)
      }

      val schema = StructType(attributes.map(a => StructField(a.name, a.attributeType.datatype)))
      Success(ac.sqlContext.createDataFrame(rdd, schema))
    } catch {
      case e: Exception => Failure(e)
    } finally {
      removeLock(storename)
    }
  }


  /**
    * Write entity.
    *
    * @param storename  adapted entityname to store feature to
    * @param df         data
    * @param attributes attributes to store
    * @param mode       save mode (append, overwrite, ...)
    * @param params     writing parameters
    * @return new options to store
    */
  override def write(storename: String, df: DataFrame, attributes: Seq[AttributeDefinition], mode: SaveMode = SaveMode.Append, params: Map[String, String])(implicit ac: SharedComponentContext): Try[Map[String, String]] = {
    try {
      val lock = getLock(storename).asWriteLock()

      lock.lock()

      val pk = attributes.filter(_.pk).head

      val rowsIt = df.select(attributes.map(a => col(a.name)): _*).rdd.map(row => row.getAs[TupleID](pk.name) -> row).toLocalIterator

      val writer = PalDB.createWriter(new File(getPath(storename)), configuration)

      while (rowsIt.hasNext) {
        val row = rowsIt.next()
        val pk = row._1
        val data = row._2

        writer.put(pk, data)
      }

      writer.close()
      lock.unlock()

      Success(Map())
    } catch {
      case e: Exception => Failure(e)
    } finally {
      removeLock(storename)
    }
  }

  /**
    * Drop the entity.
    *
    * @param storename adapted entityname to store feature to
    * @return
    */
  override def drop(storename: String)(implicit ac: SharedComponentContext): Try[Void] = {
    try {
      FileUtils.forceDelete(new File(getPath(storename)))
      Success(null)
    } catch {
      case e: Exception => Failure(e)
    }
  }
}

/**
  *
  */
class RowSerializer extends Serializer[GenericRowWithSchema] {
  override def read(in: DataInput): GenericRowWithSchema = {
    val length = in.readShort()

    val data = new Array[Byte](length)
    (0 to length - 1).foreach{ i =>
      data(i) = in.readByte()
    }

    val bis = new ByteArrayInputStream(Snappy.uncompress(data))
    val ois = new ObjectInputStream(bis)
    ois.readObject.asInstanceOf[GenericRowWithSchema]


  }

  override def getWeight(row: GenericRowWithSchema): Int = {
    row.schema.map(_.dataType.defaultSize).sum
  }

  override def write(out: DataOutput, row: GenericRowWithSchema): Unit = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(row)
    oos.close()
    val data = Snappy.compress(bos.toByteArray)

    out.writeShort(data.length)
    data.foreach{ datum =>
      out.writeByte(datum)
    }
  }
}