package ch.unibas.dmi.dbis.adam.storage.engine

import java.io._
import java.util.concurrent.ConcurrentHashMap

import ch.unibas.dmi.dbis.adam.config.AdamConfig
import ch.unibas.dmi.dbis.adam.datatypes.FieldTypes
import ch.unibas.dmi.dbis.adam.entity.AttributeDefinition
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.query.Predicate
import ch.unibas.dmi.dbis.adam.utils.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.iq80.leveldb.impl.Iq80DBFactory._
import org.iq80.leveldb.{DB, Options}

import scala.collection.convert.decorateAsScala._
import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}


/**
  * ADAMpro
  *
  * Ivan Giangreco
  * September 2016
  */
class LevelDbEngine(private val path: String) extends Engine with Logging with Serializable {
  override val name = "leveldb"

  override def supports = Seq(FieldTypes.AUTOTYPE, FieldTypes.INTTYPE, FieldTypes.LONGTYPE, FieldTypes.FLOATTYPE, FieldTypes.DOUBLETYPE, FieldTypes.STRINGTYPE, FieldTypes.TEXTTYPE, FieldTypes.BOOLEANTYPE, FieldTypes.FEATURETYPE)

  override def specializes = Seq(FieldTypes.FEATURETYPE)


  protected case class DBStatus(db: DB, var locks: Int)

  //TODO: works only for one single machine!
  val databases = new ConcurrentHashMap[EntityName, DBStatus]().asScala


  /**
    *
    * @param props
    */
  def this(props: Map[String, String]) {
    this(props.get("path").get)
  }

  /**
    *
    * @param storename
    * @return
    */
  private def getPath(storename: String) = AdamConfig.cleanPath(path) + "/" + storename + ".leveldb"

  /**
    *
    * @param storename
    * @param options
    * @return
    */
  private def openConnection(storename: String, options: Options = new Options()): DB = {
    databases.synchronized({
      if (databases.contains(storename)) {
        val dbStatus = databases.get(storename)
        dbStatus.synchronized({
          dbStatus.get.locks += 1
        })
        return dbStatus.get.db
      } else {
        val db: DB = factory.open(new File(getPath(storename)), options)
        val dbStatus = DBStatus(db, 1)
        databases.putIfAbsent(storename, dbStatus)
      }
      return databases.get(storename).get.db
    }
    )
  }

  /**
    *
    * @param storename
    */
  private def closeConnection(storename: String): Unit = {
    if (databases.contains(storename)) {
      databases.synchronized({
        val status = databases.get(storename).get
        status.synchronized({
          status.locks -= 1

          if (status.locks == 0) {
            status.db.close()
            databases.remove(storename)
          }
        })

      })
    }
  }

  /**
    * Create the entity.
    *
    * @param storename  adapted entityname to store feature to
    * @param attributes attributes of the entity (w.r.t. handler)
    * @param params     creation parameters
    * @return options to store
    */
  override def create(storename: String, attributes: Seq[AttributeDefinition], params: Map[String, String])(implicit ac: AdamContext): Try[Map[String, String]] = {
    try {
      val options = new Options()
      options.createIfMissing(true)

      val db = openConnection(storename, options)

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
  override def exists(storename: String)(implicit ac: AdamContext): Try[Boolean] = {
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
  def read(storename: String, attributes: Seq[AttributeDefinition], predicates: Seq[Predicate], params: Map[String, String])(implicit ac: AdamContext): Try[DataFrame] = {
    try {
      val db = openConnection(storename)
      val pk = attributes.filter(_.pk).head

      val rdd = if (predicates.nonEmpty) {
        val pkpredicates = predicates.filter(_.attribute == pk).headOption

        if(pkpredicates.isEmpty || predicates.size > 1){
          log.warn("leveldb can only filter by primary key, but predicate contains other fields too")
        }

        if(pkpredicates.get.operator.isDefined && pkpredicates.get.operator.get != "="){
          log.error("leveldb can only check for equality in predicates")
        }

        val lb = new ListBuffer[Row]()

        pkpredicates.foreach{ id =>
          lb += deserialize[Row](db.get(serialize(id)))
        }

        ac.sc.parallelize(lb.toSeq)
      } else {
        val it = db.iterator()
        it.seekToFirst()

        val lb = new ListBuffer[Row]()

        while (it.hasNext) {
          lb += deserialize[Row](it.next().getValue)
        }

        ac.sc.parallelize(lb.toSeq)
      }

      val schema = StructType(attributes.map(a => StructField(a.name, a.fieldtype.datatype)))
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
  override def write(storename: String, df: DataFrame, attributes: Seq[AttributeDefinition], mode: SaveMode = SaveMode.Append, params: Map[String, String])(implicit ac: AdamContext): Try[Map[String, String]] = {
    try {
      val db = openConnection(storename)
      val pk = attributes.filter(_.pk).head

      val rowsIt = df.select(attributes.map(a => col(a.name)): _*).rdd.map(row => row.getAs[Any](pk.name) -> row).toLocalIterator

      val batch = db.createWriteBatch()
      while (rowsIt.hasNext) {
        val row = rowsIt.next()
        val pk = serialize(row._1)
        val data = serialize(row._2)
        batch.put(pk, data)
      }
      db.write(batch)

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
  override def drop(storename: String)(implicit ac: AdamContext): Try[Void] = {
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
      val db = openConnection(storename)

      val iterator = db.iterator()
      iterator.seekToFirst()

      var counter = 0

      while (iterator.hasNext()) {
        counter += 1
        iterator.next()
      }

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
  private def deserialize[T](bytes: Array[Byte]): T = {
    val bis = new ByteArrayInputStream(bytes)
    val ois = new ObjectInputStream(bis)
    ois.readObject.asInstanceOf[T]
  }
}
