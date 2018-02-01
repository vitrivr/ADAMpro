package org.vitrivr.adampro.data.datatypes.bitstring

import org.vitrivr.adampro.utils.Logging
import com.googlecode.javaewah.IntIterator
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
  def apply(indexes: Seq[Int], bitStringType: BitStringTypes.BitStringType = BitStringTypes.EWAH): BitString[_] = {
    bitStringType.factory(indexes)
  }

  /**
    * Creates a new bit string of the given type which has been created by deserializing the bytes array given.
    *
    * @param bytes         serialized byte array
    * @param bitStringType type of bitstring
    * @return
    */
  def fromByteArray(bytes: Array[Byte], bitStringType: BitStringTypes.BitStringType = BitStringTypes.EWAH): BitString[_] = {
    bitStringType.factory.deserialize(bytes)
  }
}

/**
  * Bit string trait for various implementations of bit strings (e.g., optimized for sparse bit strings, etc.).
  *
  * @tparam A sub-type
  */
trait BitString[A] extends Serializable {
  /**
    * Hamming distance between two bit strings.
    *
    * @param other bitstring to compare to
    * @return
    */
  def hammingDistance(other: A): Int

  /**
    * Hamming distance between two bit strings.
    *
    * @param other bitstring to compare to
    * @return
    */
  def hammingDistance(other: BitString[_]): Int = {
    hammingDistance(other.asInstanceOf[A])
  }

  /**
    * Converts the full bit string to a byte array.
    *
    * @return
    */
  def serialize: Array[Byte]

  /**
    * Iterator that returns the index position at which the bit is set to true.
    *
    * @return
    */
  def iterator : IntIterator

  /**
    * Length.
    * @return
    */
  def length : Int
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
