package ch.unibas.dmi.dbis.adam.storage.engine

import java.io._

import ch.unibas.dmi.dbis.adam.exception.{IndexExistingException, IndexNotExistingException, EntityExistingException, EntityNotExistingException}
import ch.unibas.dmi.dbis.adam.index.Index.{IndexName, IndexTypeName}
import ch.unibas.dmi.dbis.adam.index.structures.IndexStructures
import ch.unibas.dmi.dbis.adam.main.Startup
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import slick.dbio.Effect.{Read, Write}
import slick.driver.H2Driver.api._
import slick.jdbc.meta.MTable
import slick.profile.{SqlAction, FixedSqlAction}

import scala.concurrent.Await
import scala.concurrent.duration._


/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
object CatalogOperator {
  private val MAX_WAITING_TIME : Duration = 5.seconds

  private val db = Database.forURL("jdbc:h2:" + (Startup.config.catalogPath / "catalog").toAbsolute.toString(), driver="org.h2.Driver")

  //generate catalog entities in the beginning if not already existent
  val entityList = Await.result(db.run(MTable.getTables), MAX_WAITING_TIME).toList.map(x => x.name.name)
  Catalog().filterNot(mdd => entityList.contains(mdd._1)).foreach(mdd => {
    db.run(mdd._2.schema.create)
  })

  private val entities = TableQuery[EntitiesCatalog]
  private val indexes = TableQuery[IndexesCatalog]


  /**
   *
   * @param entityname
   */
  def createEntity(entityname : EntityName): Unit ={
    if(existsEntity(entityname)){
      throw new EntityExistingException()
    }

    val setup = DBIO.seq(
      entities.+=(entityname)
    )
    db.run(setup)
  }

  /**
   *
   * @param entityname
   * @param ifExists
   */
  def dropEntity(entityname : EntityName, ifExists : Boolean = false) = {
    if(!existsEntity(entityname)){
      if(!ifExists){
        throw new EntityNotExistingException()
      }
    } else {
      val query = entities.filter(_.entityname === entityname).delete
      val count = Await.result(db.run(query), MAX_WAITING_TIME)
    }
  }

  /**
   *
   * @param entityname
   * @return
   */
  def existsEntity(entityname : EntityName): Boolean ={
    val query = entities.filter(_.entityname === entityname).length.result
    val count = Await.result(db.run(query), MAX_WAITING_TIME)

    (count > 0)
  }

  /**
   *
   * @return
   */
  def listEntities() : List[EntityName] = {
    val query = entities.map(_.entityname).result
    Await.result(db.run(query), MAX_WAITING_TIME).toList
  }

  /**
   *
   * @param indexname
   * @return
   */
  def existsIndex(indexname : IndexName): Boolean = {
    val query = indexes.filter(_.indexname === indexname).length.result
    val count = Await.result(db.run(query), MAX_WAITING_TIME)

    (count > 0)
  }

  /**
   *
   * @param indexname
   * @param entityname
   * @param indexmeta
   */
  def createIndex(indexname : IndexName, entityname : EntityName, indextypename : IndexTypeName, indexmeta : Serializable): Unit ={
    if(!existsEntity(entityname)){
      throw new EntityNotExistingException()
    }

    if(existsIndex(indexname)){
      throw new IndexExistingException()
    }

    val metaPath = Startup.config.indexPath + "/" + indexname + "/"
    val metaFilePath =  metaPath + "_adam_metadata"
    val oos = new ObjectOutputStream(new FileOutputStream(metaFilePath))
    oos.writeObject(indexmeta)
    oos.close

    val setup = DBIO.seq(
      indexes.+=((indexname, entityname, indextypename.toString, metaFilePath))
    )
    db.run(setup)
  }

  /**
   *
   * @param indexname
   * @return
   */
  def dropIndex(indexname : IndexName) : Unit = {
    if(!existsIndex(indexname)){
      throw new IndexNotExistingException()
    }

    val query = indexes.filter(_.indexname === indexname).delete
    Await.result(db.run(query), MAX_WAITING_TIME)
  }

  /**
   *
   * @param entityname
   * @return
   */
  def dropIndexesForEntity(entityname: EntityName) = {
    if(!existsEntity(entityname)){
      throw new EntityNotExistingException()
    }

    val existingIndexes = getIndexes(entityname)

    val query = indexes.filter(_.entityname === entityname).delete
    Await.result(db.run(query), MAX_WAITING_TIME)

    existingIndexes
  }

  /**
   *
   * @param entityname
   */
  def getIndexes(entityname : EntityName): Seq[IndexName] = {
    val query = indexes.filter(_.entityname === entityname).map(_.indexname).result
    Await.result(db.run(query), MAX_WAITING_TIME).toList
  }

  /**
   *
   * @return
   */
  def getIndexes(): Seq[IndexName] = {
    val query = indexes.map(_.indexname).result
    Await.result(db.run(query), MAX_WAITING_TIME).toList
  }


  /**
   *
   * @param indexname
   * @return
   */
  def getIndexMeta(indexname : IndexName) : Any = {
    val query = indexes.filter(_.indexname === indexname).map(_.indexmeta).result.head
    val path = Await.result(db.run(query), MAX_WAITING_TIME)
    val ois = new ObjectInputStream(new FileInputStream(path))
    ois.readObject()
  }

  /**
   *
   * @param indexname
   * @return
   */
  def getIndexTypeName(indexname : IndexName)  : IndexTypeName = {
    val query: SqlAction[String, NoStream, Read] = indexes.filter(_.indexname === indexname).map(_.indextypename).result.head
    val result = Await.result(db.run(query), MAX_WAITING_TIME)

    IndexStructures.withName(result)
  }

  /**
   *
   * @param indexname
   * @return
   */
  def getIndexEntity(indexname : IndexName) : EntityName = {
    val query = indexes.filter(_.indexname === indexname).map(_.entityname).result.head
    Await.result(db.run(query), MAX_WAITING_TIME)
  }

  /**
   *
   */
  def dropAllIndexes() : Unit = {
    val query: FixedSqlAction[Int, NoStream, Write] = indexes.delete
    Await.result(db.run(query), MAX_WAITING_TIME)
  }
}
