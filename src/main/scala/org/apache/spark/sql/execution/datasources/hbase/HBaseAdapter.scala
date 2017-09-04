package org.apache.spark.sql.execution.datasources.hbase

import org.apache.spark.metrics
import org.apache.spark.metrics.source
import org.apache.spark.sql.SQLContext

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * August 2017
  */
class HBaseAdapter(sqlContext : SQLContext) {
  val tmpCatalog =  s"""{
                        |"table":{"namespace":"default", "name":"tmp"},
                        |"rowkey":"tmp",
                        |"columns":{
                        |"tmp":{"cf":"rowkey", "col":"tmp", "type":"string"}
                        |}
                        |}""".stripMargin
  val relation = new HBaseRelation(Map(HBaseTableCatalog.tableCatalog -> tmpCatalog), None)(sqlContext)


  private lazy val connection =  HBaseConnectionCache.getConnection(relation.hbaseConf)
  lazy val admin = connection.getAdmin
}
