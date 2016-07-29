package ch.unibas.dmi.dbis.adam.datatypes.bitString

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import ch.unibas.dmi.dbis.adam.utils.Logging
import com.googlecode.javaewah.datastructure.BitSet

import scala.collection.mutable.ListBuffer

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
  override def intersectionCount(other: EWAHBitString): Int = bs.andcardinality(other.bs)

  /**
    *
    * @return
    */
  override def getBitIndexes: Seq[Int] = {
    val lb = new ListBuffer[Int]()
    val it = bs.intIterator()

    while(it.hasNext){
      lb += it.next()
    }

    lb.toSeq
  }

  /**
    *
    * @return
    */
  override def serialize : Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    bs.serialize(new DataOutputStream(baos))
    baos.toByteArray()
  }
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