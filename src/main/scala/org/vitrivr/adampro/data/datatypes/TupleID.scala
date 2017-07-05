package org.vitrivr.adampro.data.datatypes

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{LeafExpression, Nondeterministic}
import org.apache.spark.sql.types.{DataType, LongType}

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * January 2017
  */
object TupleID {
  type TupleID = Long
  val AdamTupleID = AttributeTypes.LONGTYPE
  val SparkTupleID = AdamTupleID.datatype
}