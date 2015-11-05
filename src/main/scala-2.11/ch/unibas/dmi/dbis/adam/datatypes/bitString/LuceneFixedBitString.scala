package ch.unibas.dmi.dbis.adam.datatypes.bitString

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

import org.apache.lucene.util.FixedBitSet

import scala.collection.mutable.ListBuffer

/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
class LuceneFixedBitString(private val values : FixedBitSet) extends BitString[LuceneFixedBitString] with Serializable {
  override def toLong : Long = getBitIndexes.map(x => math.pow(2, x).toLong).sum
  override def toLong(start: Int, end: Int): Long = ???

  override def intersectionCount(other: LuceneFixedBitString): Int = FixedBitSet.intersectionCount(values, other.values).toInt


  override def getBitIndexes: Seq[Int] = {
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


object LuceneFixedBitString extends BitStringFactory {
  override def apply(values: Seq[Int]): LuceneFixedBitString = {
    val max = if(values.isEmpty) 0 else values.max
    val bitSet = new FixedBitSet(max)
    values.foreach{bitSet.set(_)}
    new LuceneFixedBitString(bitSet)
  }

  override def deserialize(values: Seq[Byte]): LuceneFixedBitString = {
    val bais = new ByteArrayInputStream(values.toArray)
    val o = new ObjectInputStream(bais)
    val bitSet = o.readObject().asInstanceOf[FixedBitSet]
    new LuceneFixedBitString(bitSet)
  }
}