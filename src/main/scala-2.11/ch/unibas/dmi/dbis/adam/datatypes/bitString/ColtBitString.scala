package ch.unibas.dmi.dbis.adam.datatypes.bitString

import java.io.{ByteArrayInputStream, ObjectInputStream}
import java.nio.ByteBuffer

import cern.colt.bitvector.BitVector

import scala.collection.mutable.ListBuffer

/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
class ColtBitString(private val values : BitVector) extends BitString[ColtBitString] with Serializable {
  /**
   *
   * @param other
   * @return
   */
  override def intersectionCount(other: ColtBitString): Int = {
    val cloned = values.copy()
    cloned.and(other.values)
    cloned.cardinality()
  }

  /**
   *
   * @param start
   * @param end
   * @return
   */
  override def get(start: Int, end: Int): ColtBitString = {
    new ColtBitString(values.partFromTo(start, end))
  }

  /**
   *
   * @return
   */
  override def getIndexes: Seq[Int] = {
    val indexes = ListBuffer[Int]()
    for(i <- 0 until values.size()){
      if(values.get(i)){
        indexes += i
      }
    }

    indexes
  }

  /**
   *
   * @return
   */
  override def toLong: Long = values.getLongFromTo(0, values.size)

  /**
   *
   * @return
   */
  override def toByteArray : Array[Byte] = {
    val buf =  ByteBuffer.allocate(values.elements().length * 64)
    values.elements().foreach({buf.putLong(_)})
    buf.array()
  }
}

object ColtBitString extends BitStringFactory[ColtBitString] {
  /**
   *
   * @param values
   * @return
   */
  override def fromBitIndicesToSet(values: Seq[Int]): BitString[ColtBitString] = {
    val max = if(values.length == 0) 0 else values.max

    val bitSet = new BitVector(max)
    values.foreach{bitSet.set(_)}
    new ColtBitString(bitSet)
  }

  /**
   *
   * @param values
   * @return
   */
  override def fromByteSeq(values: Seq[Byte]): BitString[ColtBitString] = {
    val bais = new ByteArrayInputStream(values.toArray)
    val o = new ObjectInputStream(bais)
    val bitSet = o.readObject().asInstanceOf[BitVector]
    new ColtBitString(bitSet)
  }
}