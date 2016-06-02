package ch.unibas.dmi.dbis.adam.datatypes.bitString

import org.apache.spark.Logging
import org.apache.spark.sql.types.{BinaryType, DataType, SQLUserDefinedType, UserDefinedType}

/**
  * adamtwo
  *
  *
  * Ivan Giangreco
  * September 2015
  */
object BitString extends Logging {

  /**
    * Creates a new bit string of the given type in which every index specified is set to true.
    *
    * @param indexes       index positions to set to true
    * @param bitStringType type of bitstring
    * @return
    */
  def apply(indexes: Seq[Int], bitStringType: BitStringTypes.BitStringType = BitStringTypes.MBS): BitString[_] = {
    bitStringType.factory(indexes)
  }

  /**
    * Creates a new bit string of the given type which has been created by deserializing the bytes array given.
    *
    * @param bytes         serialized byte array
    * @param bitStringType type of bitstring
    * @return
    */
  def fromByteArray(bytes: Array[Byte], bitStringType: BitStringTypes.BitStringType = BitStringTypes.MBS): BitString[_] = {
    bitStringType.factory.deserialize(bytes)
  }
}

/**
  * Bit string trait for various implementations of bit strings (e.g., optimized for sparse bit strings, etc.).
  *
  * @tparam A sub-type
  */
@SQLUserDefinedType(udt = classOf[BitStringUDT])
trait BitString[A] extends Serializable {
  /**
    * Hamming distance between two bit strings.
    *
    * @param other bitstring to compare to
    * @return
    */
  def intersectionCount(other: A): Int

  /**
    * Hamming distance between two bit strings.
    *
    * @param other bitstring to compare to
    * @return
    */
  def intersectionCount(other: BitString[_]): Int = {
    intersectionCount(other.asInstanceOf[A])
  }

  /**
    * Splits the bit string into smaller bit strings (of length bitsPerDimension) and a total of dimensions elements,
    * then converts each element into an integer value.
    *
    * @param dimensions       number of dimensions
    * @param bitsPerDimension number of bits per dimensions
    * @return
    */
  def toInts(dimensions: Int, bitsPerDimension: Int): Array[Int]

  /**
    * Splits the bit string into smaller bit strings (of varying length),
    * then converts each element into an integer value.
    *
    * @param lengths number of bits to consider for the dimension
    * @return
    */
  def toInts(lengths: Seq[Int]): Array[Int]

  /**
    * Converts the whole bit string into a long value.
    *
    * @return
    */
  def toLong: Long

  /**
    * Converts the bit string from start to end to a long value.
    *
    * @param start including start index position
    * @param end   exclusive end index position
    * @return
    */
  def toLong(start: Int, end: Int): Long

  /**
    * Converts the full bit string to a byte array.
    *
    * @return
    */
  def toByteArray: Array[Byte]

  /**
    * Returns index position at which the bit is set to true.
    *
    * @return
    */
  protected def getBitIndexes: Seq[Int]
}


/**
  * Factory for creating bit strings (depending on the type of bit string, a different factory is necessary)
  */
trait BitStringFactory {
  /**
    * Creates a new bit string in which every index specified is set to true.
    *
    * @param indexes index positions to set to true
    * @return
    */
  def apply(indexes: Seq[Int]): BitString[_]

  /**
    * Deserializes a byte array to the bit string.
    *
    * @param values serialized byte array
    * @return
    */
  def deserialize(values: Seq[Byte]): BitString[_]
}

/**
  * UDT class for storing bit strings in Spark.
  */
@SerialVersionUID(1L)
class BitStringUDT extends UserDefinedType[BitString[_]] {
  override def sqlType: DataType = BinaryType

  override def userClass: Class[BitString[_]] = classOf[BitString[_]]

  override def asNullable: BitStringUDT = this

  override def serialize(obj: Any): Array[Byte] = {
    //possibly adjust for other bit string types, but note that we do not want to store the type every time with the bit string
    //as this would need too much space and would work against efficient lookup
    obj.asInstanceOf[MinimalBitString].toByteArray
  }

  override def deserialize(datum: Any): BitString[_] = {
    MinimalBitString.deserialize(datum.asInstanceOf[Array[Byte]])
  }
}
