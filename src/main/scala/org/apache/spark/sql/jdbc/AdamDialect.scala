package org.apache.spark.sql.jdbc

import org.apache.spark.sql.types.{DataType, MetadataBuilder, StringType}

/**
  * adamtwo
  *
  * Provides functionalities to read from an old ADAM database.
  *
  * Ivan Giangreco
  * September 2015
  */
case class AdamDialect(url: String) extends JdbcDialect {
  override def canHandle(url: String): Boolean = url.equals(this.url)


  override def getCatalystType(sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
    if (sqlType == 1111 && typeName.equals("feature")) {
      Some(StringType)
    } else {
      None
    }
  }

  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case _ => None
  }
}

object AdamDialectRegistrar {
  def register(url: String) {
    JdbcDialects.registerDialect(new AdamDialect(url))
  }
}



