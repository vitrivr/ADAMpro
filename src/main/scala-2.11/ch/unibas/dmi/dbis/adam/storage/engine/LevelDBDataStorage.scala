package ch.unibas.dmi.dbis.adam.storage.engine

import java.io._
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap

import ch.unibas.dmi.dbis.adam.datatypes.WorkingVectorWrapper
import ch.unibas.dmi.dbis.adam.main.{SparkStartup, Startup}
import ch.unibas.dmi.dbis.adam.storage.components.TableStorage
import ch.unibas.dmi.dbis.adam.table.Table.TableName
import ch.unibas.dmi.dbis.adam.table.Tuple._
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


object LevelDBDataStorage extends TableStorage {

  protected case class DBStatus(db: DB, var locks: Int)

  val config = Startup.config
  val databases: concurrent.Map[TableName, DBStatus] = new ConcurrentHashMap[TableName, DBStatus]().asScala


  /**
   *
   * @param tablename
   * @param options
   * @return
   */
  private def openConnection(tablename: TableName, options: Options): DB = {
    databases.synchronized({
      if (databases.contains(tablename)) {
        val dbStatus = databases.get(tablename)
        dbStatus.synchronized({
          dbStatus.get.locks += 1
        })
        return dbStatus.get.db
      } else {
        val db: DB = factory.open(new File(config.dataPath + "/" + tablename + ".leveldb"), options)
        val dbStatus = DBStatus(db, 1)
        databases.putIfAbsent(tablename, dbStatus)
      }
      return databases.get(tablename).get.db
    }
    )
  }

  /**
   *
   * @param tablename
   */
  private def closeConnection(tablename: TableName): Unit = {
    if (databases.contains(tablename)) {
      databases.synchronized({
        val status = databases.get(tablename).get
        status.synchronized({
          status.locks -= 1

          if (status.locks == 0) {
            status.db.close()
            databases.remove(tablename)
          }
        })

      })
    }
  }

  /**
   *
   * @param tablename
   * @return
   */
  override def readTable(tablename: TableName): DataFrame = {
    val options = new Options()
    options.createIfMissing(true)
    try {
      val db = openConnection(tablename, options)

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
      closeConnection(tablename)
    }
  }

  /**
   *
   * @param tablename
   * @param filter
   * @return
   */
  override def readFilteredTable(tablename: TableName, filter: Predef.Set[TupleID]): DataFrame = {
    val options = new Options()
    options.createIfMissing(true)

    val db = openConnection(tablename, options)

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
      closeConnection(tablename)
    }
  }


  /**
   *
   * @param tablename
   */
  override def dropTable(tablename: TableName): Unit = {
    factory.destroy(new File(config.dataPath + "/" + tablename + ".leveldb"), new Options())
  }

  /**
   *
   * @param tablename
   * @param df
   * @param mode
   */
  override def writeTable(tablename: TableName, df: DataFrame, mode: SaveMode): Unit = {
    val options = new Options()
    options.createIfMissing(true)
    val db = factory.open(new File(config.dataPath + "/" + tablename + ".leveldb"), options)

    try {
      val it = df.rdd.collect().iterator

      while (it.hasNext) {
        val data: List[Row] = List(it.next())

        val batch = db.createWriteBatch()
        var i = 0

        while (i < data.length) {
          val t = data(i)

          val tid = bytes(t.getLong(0))
          val value = bytes(t.getAs[WorkingVectorWrapper](1))

          batch.put(tid, value)

          i += 1
        }

        db.write(batch)
        batch.close()
      }

    } finally {
      db.close()
    }
  }

  /**
   *
   * @param value
   * @return
   */
  private def bytes(value: Long): Array[Byte] = {
    ByteBuffer.allocate(8).putLong(value).array()
  }

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
  private def bytes(value: WorkingVectorWrapper): Array[Byte] = {
    val bos = new ByteArrayOutputStream()
    val out = new ObjectOutputStream(bos)
    out.writeObject(value)
    bos.toByteArray
  }

  /**
   *
   * @param value
   * @return
   */
  private def asWorkingVectorWrapper(value: Array[Byte]): WorkingVectorWrapper = {
    try {
      val bis = new ByteArrayInputStream(value)
      val in = new ObjectInputStream(bis)
      in.readObject().asInstanceOf[WorkingVectorWrapper]
    } catch {
      case e : Exception => null
    }
  }
}
