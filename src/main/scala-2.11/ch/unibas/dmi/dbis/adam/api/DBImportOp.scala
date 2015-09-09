package ch.unibas.dmi.dbis.adam.api

import ch.unibas.dmi.dbis.adam.main.{SparkStartup, Startup}
import ch.unibas.dmi.dbis.adam.table.Table
import ch.unibas.dmi.dbis.adam.table.Table._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.jdbc.AdamDialectRegistrar
import org.apache.spark.sql.types._

/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
object DBImportOp {
  //TODO: refactor, atm only for testing purposes
  def apply(url : String, port : Int, database : String, user : String, password : String, tablename: TableName, columns : String) : Unit = {
    val sqlContext = SparkStartup.sqlContext

    val props: java.util.Properties = new java.util.Properties()
    props.put("user", user.toString)
    props.put("password", password.toString)

    val fullUrl = "jdbc:postgresql://" + url + ":" + port + "/" + database

    //register adam dialect
    AdamDialectRegistrar.register(fullUrl)

    //create table
    val schema = StructType(
      List(
        StructField("id", LongType, false),
        StructField("feature", ArrayType(FloatType), false)
      )
    )
    Table.createTable(tablename, schema)

    //import data
    //val db = sqlContext.read.jdbc(fullUrl , tablename, props)
    val db = sqlContext.read.jdbc(fullUrl, tablename, "id", 0, 10000000, 1000, props)

    import sqlContext.implicits._
    val columnsArray = columns.split(",")
    val data = db.select(columnsArray(0), columnsArray(1)).map(x => (x.getInt(0).toLong, x(1).toString.substring(1, x(1).toString.length - 1).split(",").map(v => v.toFloat))).toDF
    data.write.mode(SaveMode.Overwrite).save(Startup.config.dataPath + "/" + tablename)
    //Table.insertData(tablename, data)
  }


}
