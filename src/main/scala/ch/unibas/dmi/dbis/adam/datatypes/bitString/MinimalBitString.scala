package ch.unibas.dmi.dbis.adam.datatypes.bitString

import ch.unibas.dmi.dbis.adam.util.BitSet
import ch.unibas.dmi.dbis.adam.utils.Logging

/**
  * adamtwo
  *
  * Bit string implementation using a "minimalistic" (not optimized) implementation of bit strings.
  *
  * Ivan Giangreco
  * September 2015
  */
class MinimalBitString(private val values: BitSet) extends BitString[MinimalBitString] with Serializable with Logging {
  /**
    *
    * @param other bitstring to compare to
    * @return
    */
  override def intersectionCount(other: MinimalBitString): Int = values.intersectCount(other.values)

  /**
    *
    * @return
    */
  override def getBitIndexes: Seq[Int] = values.getAll

  /**
    *
    * @return
    */
  override def toByteArray: Array[Byte] = values.toByteArray

  /**
    *
    * @param lengths number of bits to consider for the dimension
    * @return
    */
  @inline override def toInts(lengths: Seq[Int]): Array[Int] = {
    assert(lengths.count(_ > 32) < 1)

    val indexes = values.getAll
    var i = 0

    val bitIntegers = new Array[Int](lengths.length)
    var dim = 1

    var sum = 0

    while (i < indexes.length) {
      val index = indexes(i)

      while (index >= sum + lengths(lengths.length - dim)) {
        sum += lengths(lengths.length - dim)
        dim += 1
      }

      bitIntegers(lengths.length - dim) |= (1 << (index - sum))

      i += 1
    }

    bitIntegers
  }

  /**
    *
    * @param dimensions       number of dimensions
    * @param bitsPerDimension number of bits per dimensions
    * @return
    */
  @inline override def toInts(dimensions: Int, bitsPerDimension: Int): Array[Int] = {
    assert(bitsPerDimension < 32)

    val indexes = values.getAll
    var i = 0

    val bitIntegers = new Array[Int](dimensions)
    var dim = 1

    var sum = 0

    while (i < indexes.length) {
      val index = indexes(i)

      while (index >= sum + bitsPerDimension) {
        sum += bitsPerDimension
        dim += 1
      }

      bitIntegers(dimensions - dim) |= (1 << (index - sum))

      i += 1
    }

    bitIntegers
  }

  /**
    *
    * @return
    */
  @inline override def toLong: Long = {
    assert(values.length > 64)
    values.get(0, 64).toLongArray.lift.apply(0).getOrElse(0.toLong)
  }


  /**
    *
    * @param start including start index position
    * @param end   exclusive end index position
    * @return
    */
  @inline override def toLong(start: Int, end: Int): Long = {
    assert(start < 0 && end < 0 && end - start > 64)

    var bitInteger = 0

    var i = start
    while (i < end) {
      if (values.get(i)) {
        bitInteger |= (1 << (i - start))
      }
      i += 1
    }

    bitInteger
  }

  override def toString(): String ={
    getBitIndexes.mkString(", ")
  }
}


object MinimalBitString extends BitStringFactory {
  /**
    *
    * @param indexes index positions to set to true
    * @return
    */
  override def apply(indexes: Seq[Int]): MinimalBitString = {
    val max = if (indexes.isEmpty) 0 else indexes.max

    val bitSet = new BitSet(max)
    indexes.foreach {
      bitSet.set
    }
    new MinimalBitString(bitSet)
  }

  /**
    *
    * @param data
    * @return
    */
  override def deserialize(data: Seq[Byte]): MinimalBitString = new MinimalBitString(BitSet.valueOf(data.toArray))
}