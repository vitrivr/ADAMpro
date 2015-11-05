package ch.unibas.dmi.dbis.adam.datatypes.bitString

import ch.unibas.dmi.dbis.adam.util.BitSet
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.types.{BinaryType, DataType, SQLUserDefinedType, UserDefinedType}

/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
@SQLUserDefinedType(udt = classOf[MinimalBitStringUDT])
class MinimalBitString(private val values : BitSet) extends BitString[MinimalBitString] with Serializable {
  override def intersectionCount(other: MinimalBitString): Int =  values.intersectCount(other.values)

  override def getBitIndexes: Seq[Int] = values.getAll
  override def toByteArray : Array[Byte] = values.toByteArray

  @inline override def toInts(lengths : Seq[Int]): Array[Int] = {
    val indexes = values.getAll
    var i = 0

    val bitIntegers = new Array[Int](lengths.length)
    var dim = 1

    var sum = 0
    
    while(i < indexes.length){
      val index = indexes(i)
      
      while(index >= sum + lengths(lengths.length - dim)){
        sum += lengths(lengths.length - dim)
        dim += 1
      }

      bitIntegers(lengths.length - dim) |= (1 << (index - sum))

      i += 1
    }

    bitIntegers
  }

  @inline override def toInts(dimensions : Int, length : Int): Array[Int] = {
    val indexes = values.getAll
    var i = 0

    val bitIntegers = new Array[Int](dimensions)
    var dim = 1

    var sum = 0

    while(i < indexes.length){
      val index = indexes(i)

      while(index >= sum + length){
        sum += length
        dim += 1
      }

      bitIntegers(dimensions - dim) |= (1 << (index - sum))

      i += 1
    }

    bitIntegers
  }

  override def toLong: Long = {
    if (values.length > 64) throw new IndexOutOfBoundsException()
    values.get(0, 64).toLongArray().lift.apply(0).getOrElse(0.toLong)
  }

  @inline override def toLong(start: Int, end: Int): Long = {
    if (start < 0 || end < 0 || end - start > 64) throw new IndexOutOfBoundsException()
    var bitInteger = 0
    var i = start
    while(i < end){
      if(values.get(i)){ bitInteger |= (1 << (i - start)) }
      i += 1
    }
    bitInteger
  }
}


object MinimalBitString extends BitStringFactory {
  override def apply(values: Seq[Int]): MinimalBitString = {
    val max = if(values.isEmpty) 0 else values.max

    val bitSet = new BitSet(max)
    values.foreach{bitSet.set(_)}
    new MinimalBitString(bitSet)
  }

  override def deserialize(data : Seq[Byte]) : MinimalBitString = new MinimalBitString(BitSet.valueOf(data.toArray))
}



class MinimalBitStringUDT extends UserDefinedType[MinimalBitString] {
  override def sqlType: DataType = BinaryType
  override def userClass: Class[MinimalBitString] = classOf[MinimalBitString]
  override def asNullable: MinimalBitStringUDT = this

  override def serialize(obj: Any): InternalRow = {
    val row = new GenericMutableRow(1)

    if(obj.isInstanceOf[MinimalBitString]){
      row.update(0, obj.asInstanceOf[MinimalBitString].toByteArray)
    } else {
      row.setNullAt(0)
    }

    row
  }

  override def deserialize(datum: Any): MinimalBitString = {
    if(datum.isInstanceOf[InternalRow]){
      val row = datum.asInstanceOf[InternalRow]
      MinimalBitString.deserialize(row.getBinary(0))
    } else {
      null
    }
  }
}
