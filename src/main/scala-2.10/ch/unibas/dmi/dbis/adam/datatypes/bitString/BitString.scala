package ch.unibas.dmi.dbis.adam.datatypes.bitString

import org.apache.spark.sql.types.{BinaryType, DataType, SQLUserDefinedType, UserDefinedType}

/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
@SQLUserDefinedType(udt = classOf[BitStringUDT])
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


class BitStringUDT extends UserDefinedType[BitString[_]] {
  override def sqlType: DataType = BinaryType
  override def userClass: Class[BitString[_]] = classOf[BitString[_]]
  override def asNullable: BitStringUDT = this

  override def serialize(obj: Any): Array[Byte] = {
    if(obj.isInstanceOf[MinimalBitString]){
      obj.asInstanceOf[MinimalBitString].toByteArray
    } else {
      Array[Byte]()
    }
  }

  override def deserialize(datum: Any): BitString[_] = {
    if(datum.isInstanceOf[Array[Byte]]){
      MinimalBitString.deserialize(datum.asInstanceOf[Array[Byte]])
    } else {
      null
    }
  }
}
