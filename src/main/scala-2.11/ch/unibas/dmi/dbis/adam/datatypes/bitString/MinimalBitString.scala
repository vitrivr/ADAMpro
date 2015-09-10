package ch.unibas.dmi.dbis.adam.datatypes.bitString

import java.util

import scala.collection.mutable.ListBuffer

/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
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
  override def toLong: Long = values.get(0, 64).toLongArray().lift.apply(0).getOrElse(0.toLong)

  /**
   *
   * @return
   */
  override def toByteArray : Array[Byte] = {
    values.toByteArray
  }
}


object MinimalBitString extends BitStringFactory[MinimalBitString] {
  /**
   *
   * @param values
   * @return
   */
  override def fromBitIndicesToSet(values: Seq[Int]): BitString[MinimalBitString] = {
    val max = if(values.length == 0) 0 else values.max

    val bitSet = new util.BitSet(max)
    values.foreach{bitSet.set(_)}
    new MinimalBitString(bitSet)
  }

  /**
   *
   * @param data
   * @return
   */
  def fromByteSeq(data : Seq[Byte]) : BitString[MinimalBitString] = {
    val values = util.BitSet.valueOf(data.toArray)
    new MinimalBitString(values)
  }
}