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
  /**
   *
   * @param other
   * @return
   */
  override def intersectionCount(other: LuceneFixedBitString): Int = {
    FixedBitSet.intersectionCount(values, other.values).toInt
  }

  /**
   *
   * @param start
   * @param end
   * @return
   */
  override def get(start: Int, end: Int): LuceneFixedBitString = {
    val newValues = new FixedBitSet(end - start)

    var nextIndex = start - 1

    do{
      nextIndex += 1
      nextIndex = values.nextSetBit(nextIndex)

      if(nextIndex != -1){
        newValues.set(nextIndex)
      }
    } while (nextIndex != -1 && nextIndex < end)

    new LuceneFixedBitString(newValues)
  }

  /**
   *
   * @return
   */
  override def getIndexes: Seq[Int] = {
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

  /**
   *
   * @return
   */
  override def toLong : Long = {
    getIndexes.map(x => math.pow(2, x).toLong).sum
  }

  /**
   *
   * @return
   */
  override def toByteArray : Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val o = new ObjectOutputStream(baos)
    o.writeObject(values)
    baos.toByteArray
  }
}


object LuceneFixedBitString extends BitStringFactory[LuceneFixedBitString] {
  /**
   *
   * @param values
   * @return
   */
  override def fromBitIndicesToSet(values: Seq[Int]): BitString[LuceneFixedBitString] = {
    val max = if(values.length == 0) 0 else values.max

    val bitSet = new FixedBitSet(max)
    values.foreach{bitSet.set(_)}
    new LuceneFixedBitString(bitSet)
  }

  /**
   *
   * @param values
   * @return
   */
  override def fromByteSeq(values: Seq[Byte]): BitString[LuceneFixedBitString] = {
    val bais = new ByteArrayInputStream(values.toArray)
    val o = new ObjectInputStream(bais)
    val bitSet = o.readObject().asInstanceOf[FixedBitSet]
    new LuceneFixedBitString(bitSet)
  }
}