package ch.unibas.dmi.dbis.adam.datatypes.bitString

import java.io.{ObjectInputStream, ByteArrayInputStream, ByteArrayOutputStream, ObjectOutputStream}

import com.zaxxer.sparsebits.SparseBitSet

import scala.collection.mutable.ListBuffer

/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
class SparseBitSetBitString(private val values : SparseBitSet) extends BitString[SparseBitSetBitString] with Serializable {
  override def toLong : Long = getBitIndexes.map(x => math.pow(2, x).toLong).sum
  override def toLong(start : Int, end : Int) : Long = ???

  override def intersectionCount(other : SparseBitSetBitString) : Int = {
    val cloned = values.clone()
    cloned.and(other.values)
    cloned.cardinality()
  }

  override def getBitIndexes : Seq[Int] = {
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

  override def toByteArray : Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val o = new ObjectOutputStream(baos)
    o.writeObject(values)
    baos.toByteArray
  }
}



object SparseBitSetBitString extends BitStringFactory {
  override def apply(values: Seq[Int]): SparseBitSetBitString = {
    val max = if(values.length == 0) 0 else values.max
    val bitSet = new SparseBitSet(max)
    values.foreach{bitSet.set(_)}
    new SparseBitSetBitString(bitSet)
  }

  override def deserialize(values: Seq[Byte]): SparseBitSetBitString = {
    val bais = new ByteArrayInputStream(values.toArray)
    val o = new ObjectInputStream(bais)
    val bitSet = o.readObject().asInstanceOf[SparseBitSet]
    new SparseBitSetBitString(bitSet)
  }
}