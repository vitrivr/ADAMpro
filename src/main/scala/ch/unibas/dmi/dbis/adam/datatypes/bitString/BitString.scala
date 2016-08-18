package ch.unibas.dmi.dbis.adam.datatypes.bitString

import ch.unibas.dmi.dbis.adam.utils.Logging
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
@SQLUserDefinedType(udt = classOf[BitStringUDT])
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
    obj.asInstanceOf[EWAHBitString].serialize
  }

  override def deserialize(datum: Any): BitString[_] = {
    EWAHBitString.deserialize(datum.asInstanceOf[Array[Byte]])
  }
}
