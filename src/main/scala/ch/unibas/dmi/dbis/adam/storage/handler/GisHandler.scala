package ch.unibas.dmi.dbis.adam.storage.handler

import ch.unibas.dmi.dbis.adam.datatypes.FieldTypes
import ch.unibas.dmi.dbis.adam.datatypes.FieldTypes.FieldType
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.storage.engine.GisDatabaseEngine
import org.apache.spark.sql.DataFrame

import scala.util.Try

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * September 2016
  */
class GisHandler(private val engine: GisDatabaseEngine) extends DatabaseHandler(engine) {
  override val name: String = "gis"

  override def supports: Seq[FieldType] = Seq(FieldTypes.AUTOTYPE, FieldTypes.INTTYPE, FieldTypes.LONGTYPE, FieldTypes.STRINGTYPE, FieldTypes.GEOMETRYTYPE, FieldTypes.GEOGRAPHYTYPE)

  override def specializes: Seq[FieldType] = Seq(FieldTypes.GEOMETRYTYPE, FieldTypes.GEOGRAPHYTYPE)

  override protected val ENTITY_OPTION_NAME = "storing-gis-tablename"


  /**
    *
    * @param entityname
    * @param params
    * @return
    */
  override def read(entityname: EntityName, params: Map[String, String] = Map())(implicit ac: AdamContext): Try[DataFrame] = {
    execute("read") {
      val query = params.getOrElse("query", "*")
      val limit = params.getOrElse("limit", "ALL")
      val tablename = getTablename(entityname)
      val table = s"(SELECT $query FROM $tablename LIMIT $limit) AS $tablename"
      engine.read(table, params.get("predicate").map(Seq(_)))
    }
  }
}