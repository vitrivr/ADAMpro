package org.vitrivr.adampro.data.datatypes.bitstring

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import org.vitrivr.adampro.utils.Logging
import com.googlecode.javaewah.datastructure.BitSet

/**
  * adamtwo
  *
  * Bit string implementation using the JavaEWAH implementation of bit strings.
  *
  * Ivan Giangreco
  * July 2016
  */
class EWAHBitString(private val bs: BitSet) extends BitString[EWAHBitString] with Serializable with Logging {
  /**
    *
    * @param other bitstring to compare to
    * @return
    */
  override def hammingDistance(other: EWAHBitString): Int = bs.xorcardinality(other.bs)

  /**
    *
    * @return
    */
  override def iterator = bs.intIterator()

  /**
    *
    * @return
    */
  override def serialize : Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    bs.serialize(new DataOutputStream(baos))
    baos.toByteArray()
  }

  /**
    *
    * @return
    */
  override def toString : String = bs.toString
}


object EWAHBitString extends BitStringFactory {
  /**
    *
    * @param indexes index positions to set to true
    * @return
    */
  override def apply(indexes: Seq[Int]): EWAHBitString = {
    new EWAHBitString(BitSet.bitmapOf(indexes : _*))
  }

  /**
    *
    * @param data
    * @return
    */
  override def deserialize(data: Seq[Byte]): EWAHBitString = {
    val bs = new BitSet()
    bs.deserialize(new DataInputStream(new ByteArrayInputStream(data.toArray)))
    new EWAHBitString(bs)
  }
}