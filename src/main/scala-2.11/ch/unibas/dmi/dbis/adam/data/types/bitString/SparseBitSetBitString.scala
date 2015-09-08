package ch.unibas.dmi.dbis.adam.data.types.bitString

import java.io.{ByteArrayOutputStream, ObjectOutputStream}

import com.zaxxer.sparsebits.SparseBitSet

import scala.collection.mutable.ListBuffer

/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
class SparseBitSetBitString(private val values : SparseBitSet) extends BitString[SparseBitSetBitString] with Serializable {
  /**
   *
   * @param other
   * @return
   */
  override def intersectionCount(other : SparseBitSetBitString) : Int = {
    val cloned = values.clone()
    cloned.and(other.values)
    cloned.cardinality()
  }

  /**
   *
   * @param start
   * @param end
   * @return
   */
  override def get(start : Int, end : Int) : SparseBitSetBitString = {
    val indexes = ListBuffer[Int]()
    var nextIndex : Int = start - 1

    do {
      nextIndex += 1
      nextIndex = values.nextSetBit(nextIndex)

      if(nextIndex != -1 && nextIndex < end){
        indexes += nextIndex - start
      }

    } while(nextIndex != -1 && nextIndex < end)


    val max = if(indexes.isEmpty){
      1
    } else {
      indexes.max
    }

    val bitSet = new SparseBitSet(max)
    indexes.foreach{bitSet.set(_)}
    new SparseBitSetBitString(bitSet)
  }

  /**
   *
   * @return
   */
  override def getIndexes : Seq[Int] = {
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
  override def toLong : Long = {
    getIndexes.map(x => math.pow(2, x).toLong).sum
  }

  /**
   *
   * @return
   */
  override def toByteSeq : Seq[Byte] = {
    val baos = new ByteArrayOutputStream()
    val o = new ObjectOutputStream(baos)
    o.writeObject(values)
    baos.toByteArray.toSeq
  }
}


object SparseBitSetBitString extends BitStringFactory[SparseBitSetBitString] {
  /**
   *
   * @param values
   * @return
   */
  override def fromBitIndicesToSet(values: Seq[Int]): BitString[SparseBitSetBitString] = {
    val bitSet = new SparseBitSet(values.max)
    values.foreach{bitSet.set(_)}
    new SparseBitSetBitString(bitSet)
  }
}