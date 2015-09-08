package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.analysis.UnresolvedException
import org.apache.spark.sql.types._

/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
case class BitwiseAndCounting(left: Expression, right: Expression) extends BinaryExpression {
  self: Product =>

  type EvaluatedType = Any

  override def symbol: String = "-c&c-"

  private lazy val and: (Any, Any) => Any = ((evalE1: Array[Byte], evalE2: Array[Byte]) => hammingDistance(evalE1.toArray, evalE2.toArray)).asInstanceOf[(Any, Any) => Any]

  override lazy val resolved =
    left.resolved && right.resolved &&
      left.dataType == right.dataType &&
      !DecimalType.isFixed(left.dataType)

  override def dataType: DataType = {
    if (!resolved) {
      throw new UnresolvedException(this,
        s"datatype. Can not resolve due to differing types ${left.dataType}, ${right.dataType}")
    }
    IntegerType
  }

  override def eval(input: Row): Any = {
    val evalE1 = left.eval(input)
    if(evalE1 == null) {
      -1
    } else {
      val evalE2 = right.eval(input)
      if (evalE2 == null) {
        -1
      } else {
        and(evalE1, evalE2)
      }
    }
  }

  private def hammingDistance(b1: Array[Byte], b2: Array[Byte]) : Int = (b1.zip(b2).map((x: (Byte, Byte)) => numberOfBitsSet((x._1 ^ x._2).toByte))).sum
  private def numberOfBitsSet(b: Byte) : Int = (0 to 7).map((i : Int) => (b >>> i) & 1).sum

  private def hammingDistance(b1: Seq[Long], b2: Seq[Long]) : Int = (b1.zip(b2).map((x: (Long, Long)) => numberOfBitsSet((x._1 ^ x._2)))).sum
  private def numberOfBitsSet(b: Long) : Int = (0 to 63).map((i : Int) => (b >>> i) & 1).sum.toInt
}