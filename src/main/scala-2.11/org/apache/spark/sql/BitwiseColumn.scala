package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.{BitwiseAndCounting, Expression}
import org.apache.spark.sql.functions._

/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
class BitwiseColumn(c : Column){
  /** Creates a column based on the given expression. */
  implicit private def exprToColumn(newExpr: Expression): Column = new Column(newExpr)
  def bitwiseANDCounting(other: Any): Column = BitwiseAndCounting(c.expr, lit(other).expr)
}
