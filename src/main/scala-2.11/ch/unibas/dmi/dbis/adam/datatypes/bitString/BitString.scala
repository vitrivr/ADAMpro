package ch.unibas.dmi.dbis.adam.datatypes.bitString

/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
trait BitString[A] {
  def intersectionCount(other : A) : Int

  def toInts(dimensions : Int, length : Int): Array[Int] = ???
  def toInts(lengths : Seq[Int]): Array[Int] = ???

  def toLong : Long
  def toLong(start : Int, end : Int) : Long

  def toByteArray : Array[Byte]

  protected def getBitIndexes: Seq[Int]

  def intersectionCount(other: BitString[_]): Int = {
    intersectionCount(other.asInstanceOf[A])
  }
}

trait BitStringFactory {
  def apply (values : Seq[Int]) : BitString[_]
  def deserialize (values : Seq[Byte]) : BitString[_]
}


object BitString {
  def apply(values : Seq[Int], bitStringType: BitStringTypes.BitStringType = BitStringTypes.MBS) : BitString[_] = {
    bitStringType.factory(values)
  }

  def fromByteArray(bytes: Array[Byte], bitStringType: BitStringTypes.BitStringType = BitStringTypes.MBS): BitString[_] = {
    bitStringType.factory.deserialize(bytes)
  }
}