package ch.unibas.dmi.dbis.adam.storage.engine

import java.io._
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap

import ch.unibas.dmi.dbis.adam.config.AdamConfig
import ch.unibas.dmi.dbis.adam.datatypes.feature.FeatureVectorWrapper
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.entity.Tuple._
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.storage.components.FeatureStorage
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.iq80.leveldb._
import org.iq80.leveldb.impl.Iq80DBFactory._

import scala.collection._
import scala.collection.convert.decorateAsScala._
import scala.collection.mutable.ListBuffer


/**
 * adamtwo
 *
 * Ivan Giangreco
 * October 2015
 */
object LevelDBDataStorage extends FeatureStorage {

  protected case class DBStatus(db: DB, var locks: Int)

  val databases: concurrent.Map[EntityName, DBStatus] = new ConcurrentHashMap[EntityName, DBStatus]().asScala


  /**
   *
   * @param entityname
   * @param options
   * @return
   */
  private def openConnection(entityname: EntityName, options: Options): DB = {
    databases.synchronized({
      if (databases.contains(entityname)) {
        val dbStatus = databases.get(entityname)
        dbStatus.synchronized({
          dbStatus.get.locks += 1
        })
        return dbStatus.get.db
      } else {
        val db: DB = factory.open(new File(AdamConfig.dataPath + "/" + entityname + ".leveldb"), options)
        val dbStatus = DBStatus(db, 1)
        databases.putIfAbsent(entityname, dbStatus)
      }
      return databases.get(entityname).get.db
    }
    )
  }

  /**
   *
   * @param entityname
   */
  private def closeConnection(entityname: EntityName): Unit = {
    if (databases.contains(entityname)) {
      databases.synchronized({
        val status = databases.get(entityname).get
        status.synchronized({
          status.locks -= 1

          if (status.locks == 0) {
            status.db.close()
            databases.remove(entityname)
          }
        })

      })
    }
  }

  /**
   *
   * @param entityname
   * @param filter
   * @return
   */
  override def read(entityname: EntityName, filter: Option[scala.collection.Set[TupleID]]): DataFrame = {
    if(filter.isDefined){
      internalRead(entityname, filter.get)
    } else {
      internalRead(entityname)
    }
  }

    /**
   *
   * @param entityname
   * @return
   */
  private def internalRead(entityname: EntityName): DataFrame = {
    val options = new Options()
    options.createIfMissing(true)
    try {
      val db = openConnection(entityname, options)

      val iterator = db.iterator()
      iterator.seekToFirst()

      val results = ListBuffer[Row]()
      while (iterator.hasNext()) {
        val key = asLong(iterator.peekNext().getKey())
        val value = asWorkingVectorWrapper(iterator.peekNext().getValue())

        results += Row(key, value)

        iterator.next()
      }

      val rdd = SparkStartup.sc.parallelize(results)
      val schema = StructType(
        List(
          StructField("id", LongType, false),
          StructField("feature", BinaryType, false)
        )
      )

      SparkStartup.sqlContext.createDataFrame(rdd, schema)

    } finally {
      closeConnection(entityname)
    }
  }

  /**
   *
   * @param entityname
   * @param filter
   * @return
   */
  private def internalRead(entityname: EntityName, filter: Set[TupleID]): DataFrame = {
    val options = new Options()
    options.createIfMissing(true)

    val db = openConnection(entityname, options)

    try {
      val data = filter.par.map(f => {
        val values = db.get(bytes(f))

        if (values != null) {
          (f, values)
        } else {
          null
        }
      }).filter(x => x != null).map(r => Row(r._1, asWorkingVectorWrapper(r._2))).toSeq.seq

      val rdd = SparkStartup.sc.parallelize(data)

      val schema = StructType(
        List(
          StructField("id", LongType, false),
          StructField("feature", BinaryType, false)
        )
      )
      SparkStartup.sqlContext.createDataFrame(rdd, schema)

    } finally {
      closeConnection(entityname)
    }
  }


  /**
   *
   * @param entityname
   * @return
   */
  def count(entityname: EntityName): Int = {
    val options = new Options()
    options.createIfMissing(true)
    try {
      val db = openConnection(entityname, options)

      val iterator = db.iterator()
      iterator.seekToFirst()

      var counter = 0

      while (iterator.hasNext()) {
        counter += 1
        iterator.next()
      }

      counter

    } finally {
      closeConnection(entityname)
    }
  }


  /**
   *
   * @param entityname
   */
  override def drop(entityname: EntityName): Unit = factory.destroy(new File(AdamConfig.dataPath + "/" + entityname + ".leveldb"), new Options())


  /**
   *
   * @param entityname
   * @param df
   * @param mode
   */
  override def write(entityname: EntityName, df: DataFrame, mode: SaveMode): Unit = {
    val options = new Options()
    options.createIfMissing(true)
    val db = openConnection(entityname, options)

    try {
      val it = df.rdd.collect().iterator

      while (it.hasNext) {
        val data: List[Row] = List(it.next())

        val batch = db.createWriteBatch()
        var i = 0

        while (i < data.length) {
          val t = data(i)

          val tid = bytes(t.getLong(0))
          val value = bytes(t.getAs[FeatureVectorWrapper](1))

          batch.put(tid, value)

          i += 1
        }

        db.write(batch)
        batch.close()
      }

    } finally {
      closeConnection(entityname)
    }
  }

  /**
   *
   * @param value
   * @return
   */
  private def bytes(value: Long): Array[Byte] = ByteBuffer.allocate(8).putLong(value).array()

  /**
   *
   * @param v
   * @return
   */
  private def asLong(v: Array[Byte]): Long = {
    val buffer = ByteBuffer.allocate(8)
    buffer.put(v)
    buffer.flip()
    buffer.getLong()
  }

  /**
   *
   * @param value
   * @return
   */
  private def bytes(value: FeatureVectorWrapper): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(bos)
    out.writeObject(value)
    val res = bos.toByteArray
    bos.close()
    out.close()

    res
  }

  /**
   *
   * @param value
   * @return
   */
  private def asWorkingVectorWrapper(value: Array[Byte]): FeatureVectorWrapper = {
    try {
      val bis = new ByteArrayInputStream(value)
      val in = new ObjectInputStream(bis)
      in.readObject().asInstanceOf[FeatureVectorWrapper]
    } catch {
      case e : Exception => null
    }
  }
}
