package org.vitrivr.adampro.storage.engine

import java.io.{ByteArrayInputStream, ObjectInputStream, ObjectOutputStream, _}
import java.nio.channels.OverlappingFileLockException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.StampedLock

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.iq80.leveldb.impl.Iq80DBFactory._
import org.iq80.leveldb.{DB, Options}
import org.vitrivr.adampro.data.datatypes.AttributeTypes
import org.vitrivr.adampro.data.datatypes.TupleID.TupleID
import org.vitrivr.adampro.data.entity.AttributeDefinition
import org.vitrivr.adampro.process.SharedComponentContext
import org.vitrivr.adampro.query.query.Predicate

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2017
  */
class LevelDbEngine(private val path: String)(@transient override implicit val ac: SharedComponentContext) extends Engine()(ac) with Serializable {
  override val name = "leveldb"

  override def supports = Seq(AttributeTypes.AUTOTYPE, AttributeTypes.INTTYPE, AttributeTypes.LONGTYPE, AttributeTypes.STRINGTYPE, AttributeTypes.VECTORTYPE)

  override def specializes = Seq(AttributeTypes.VECTORTYPE)

  override val repartitionable = false

  private case class DBStatus(db: DB, lock: StampedLock)

  private val connections = new ConcurrentHashMap[String, DBStatus]()


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
  private def getPath(storename: String) = ac.config.cleanPath(path) + "/" + storename + ".leveldb"

  /**
    *
    * @param storename
    * @param options
    * @return
    */
  private def openConnection(storename: String, options: Options = new Options()): (DB, StampedLock) = {
    connections.synchronized({
      if (!connections.containsKey(storename)) {
        val db = factory.open(new File(getPath(storename)), options)
        val lock = new StampedLock()

        connections.putIfAbsent(storename, DBStatus(db, lock))
        (db, lock)
      } else {
        val ret = connections.get(storename)
        (ret.db, ret.lock)
      }
    })
  }

  /**
    *
    * @param storename
    */
  private def closeConnection(storename: String): Unit = {
    connections.synchronized({
      if (connections.contains(storename)) {
        val ret = connections.get(storename)

        if (!ret.lock.isWriteLocked && !ret.lock.isReadLocked) {
          ret.db.close()
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
      val options = new Options()
      options.createIfMissing(true)

      val (db, lock) = openConnection(storename, options)

      Success(Map())
    } catch {
      case e: Exception =>
        log.error("fatal error when opening connection to leveldb", e)
        Failure(e)
    } finally {
      closeConnection(storename)
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

        val (db, lock) = openConnection(storename)
        val stamp = lock.readLock()
        pkpredicates.get.values.foreach { id =>
          lb += deserialize[Row](db.get(serialize(id))).get
        }
        lock.unlockRead(stamp)

        ac.sc.parallelize(lb)
      } else {
        val (db, lock) = openConnection(storename)
        val stamp = lock.readLock()

        val iterator = db.iterator()
        iterator.seekToFirst()

        val lb = new ListBuffer[Row]()

        while (iterator.hasNext()) {
          lb += deserialize[Row](iterator.next().getValue).get
        }
        lock.unlockRead(stamp)

        ac.sc.parallelize(lb)
      }

      val schema = StructType(attributes.map(a => StructField(a.name, a.attributeType.datatype)))
      Success(ac.sqlContext.createDataFrame(rdd, schema))
    } catch {
      case e: Exception => Failure(e)
    } finally {
      closeConnection(storename)
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
      val (db, lock) = openConnection(storename)
      val stamp = lock.writeLock()

      val pk = attributes.filter(_.pk).head

      val groupedIt = df.select(attributes.map(a => col(a.name)): _*).rdd.map(row => row.getAs[TupleID](pk.name) -> row).toLocalIterator.sliding(1000, 1000)

      groupedIt.foreach { group =>
        val rowsIt = group.iterator

        val batch = db.createWriteBatch()
        while (rowsIt.hasNext) {
          val row = rowsIt.next()
          val pk = serialize(row._1)
          val data = serialize(row._2)
          batch.put(pk, data)
        }
        db.write(batch)
      }

      lock.unlockWrite(stamp)

      Success(Map())
    } catch {
      case e: Exception => Failure(e)
    } finally {
      closeConnection(storename)
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
      factory.destroy(new File(getPath(storename)), new Options())
      Success(null)
    } catch {
      case e: Exception => Failure(e)
    }
  }


  /**
    *
    * @param storename
    * @return
    */
  private def count(storename: String): Int = {
    try {
      val (db, lock) = openConnection(storename)
      val stamp = lock.readLock()

      val iterator = db.iterator()
      iterator.seekToFirst()

      var counter = 0

      while (iterator.hasNext()) {
        counter += 1
        iterator.next()
      }

      lock.unlockRead(stamp)
      counter
    } finally {
      closeConnection(storename)
    }
  }


  /**
    *
    * @param o
    * @tparam T
    * @return
    */
  private def serialize[T](o: T): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(bos)
    oos.writeObject(o)
    oos.close()
    bos.toByteArray
  }

  /**
    *
    * @param bytes
    * @tparam T
    * @return
    */
  private def deserialize[T](bytes: Array[Byte]): Option[T] = {
    if (bytes != null) {
      val bis = new ByteArrayInputStream(bytes)
      val ois = new ObjectInputStream(bis)
      Some(ois.readObject.asInstanceOf[T])
    } else {
      None
    }
  }
}

