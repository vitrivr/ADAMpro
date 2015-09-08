package ch.unibas.dmi.dbis.adam.data.types.bitString

import java.util

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer

/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
@SQLUserDefinedType(udt = classOf[MinimalBitStringUDT])
class MinimalBitString(private val values : util.BitSet) extends BitString[MinimalBitString] with Serializable {
  /**
   *
   * @param other
   * @return
   */
  override def intersectionCount(other: MinimalBitString): Int = {
    val clone = values.clone().asInstanceOf[util.BitSet]
    clone.and(other.values)
    clone.cardinality()
  }

  /**
   *
   * @param start
   * @param end
   * @return
   */
  override def get(start: Int, end: Int): BitString[MinimalBitString] = {
    new MinimalBitString(values.get(start, end))
  }

  /**
   *
   * @return
   */
  override def getIndexes: Seq[Int] = {
    val indexes = ListBuffer[Int]()
    var nextIndex : Int = -1

    do {
      nextIndex += 1
      nextIndex = values.nextSetBit(nextIndex)

      if(nextIndex != -1){
        indexes += nextIndex
      }

    } while(nextIndex != -1)

    indexes.toList
  }

  /**
   *
   * @return
   */
  override def toLong: Long = values.get(0, 64).toLongArray()(0)

  /**
   *
   * @return
   */
  override def toByteSeq: Seq[Byte] = values.toByteArray
}


class MinimalBitStringUDT extends UserDefinedType[MinimalBitString] {

  override def sqlType: DataType =
    StructType(Seq(StructField("value", BinaryType)))

  override def serialize(obj: Any): Row = {
    obj match {
      case dt: MinimalBitString =>
        val row = new GenericMutableRow(1)
        row.update(0, dt.toByteSeq)
        row
    }
  }

  override def deserialize(datum: Any): MinimalBitString = {
    datum match {
      case row: Row =>
        require(row.length == 1)
        val values = util.BitSet.valueOf(row.getSeq[Byte](0).toArray)
        new MinimalBitString(values)
    }
  }

  override def userClass: Class[MinimalBitString] = classOf[MinimalBitString]

  override def asNullable: MinimalBitStringUDT = this
}


object MinimalBitString extends BitStringFactory[MinimalBitString] {
  /**
   *
   * @param values
   * @return
   */
  override def fromBitIndicesToSet(values: Seq[Int]): BitString[MinimalBitString] = {
    val bitSet = new util.BitSet(values.max)
    values.foreach{bitSet.set(_)}
    new MinimalBitString(bitSet)
  }
}