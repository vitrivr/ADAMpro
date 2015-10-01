package ch.unibas.dmi.dbis.adam.datatypes.bitString

import ch.unibas.dmi.dbis.adam.util.BitSet

/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
class MinimalBitString(private val values : BitSet) extends BitString[MinimalBitString] with Serializable {
  /**
   *
   * @param other
   * @return
   */
  override def intersectionCount(other: MinimalBitString): Int = {
    values.intersectCount(other.values)
  }

  /**
   *
   * @param lengths
   * @return
   */
  @inline override def getWithBitLengths(lengths : Seq[Int]): Array[Int] = {
    val indexes = values.getAll
    var i = 0

    val bitIntegers = new Array[Int](lengths.length)
    var dim = 1

    var sum = 0
    
    while(i < indexes.length){
      val index = indexes(i)
      
      while(index >= sum + lengths(lengths.length - dim)){
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
   * @param start
   * @param end
   * @return
   */
  @inline override def get(start: Int, end: Int): Int = {
   var bitInteger = 0

    var i = start
    while(i < end){
      if(values.get(i)){
       bitInteger |= (1 << (i - start))
      }
      i += 1
    }

    bitInteger
  }

  /**
   *
   * @return
   */
  override def getIndexes: Seq[Int] = {
    values.getAll
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
    val max = if(values.isEmpty) 0 else values.max

    val bitSet = new BitSet(max)
    values.foreach{bitSet.set(_)}
    new MinimalBitString(bitSet)
  }

  /**
   *
   * @param data
   * @return
   */
  def fromByteSeq(data : Seq[Byte]) : BitString[MinimalBitString] = {
    val values = BitSet.valueOf(data.toArray)
    new MinimalBitString(values)
  }
}