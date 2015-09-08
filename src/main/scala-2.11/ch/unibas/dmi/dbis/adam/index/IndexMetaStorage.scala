package ch.unibas.dmi.dbis.adam.index

import scala.collection.mutable.{Map => mMap}
/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
class IndexMetaStorage(val map: Map[String, Any]) {
  def contains(key: String): Boolean = map.contains(key)
  def get[T](key: String): T = map(key).asInstanceOf[T]
}

/**
 *
 */
class IndexMetaStorageBuilder {
  private val map: mMap[String, Any] = mMap.empty

  protected def getMap = map.toMap

  /**
   *
   * @param key
   * @param value
   * @return
   */
  def put(key: String, value: Any): this.type = {
    map.put(key, value)
    this
  }

  /**
   *
   * @return
   */
  def build(): IndexMetaStorage = {
    new IndexMetaStorage(map.toMap)
  }
}
