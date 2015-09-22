package ch.unibas.dmi.dbis.adam.datatypes.bitString

import java.io.{ByteArrayInputStream, ObjectInputStream}

import ch.unibas.dmi.dbis.adam.util.BitSet
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.types._

import scala.reflect.runtime.universe._

/**
 * adamtwo
 *
 * Ivan Giangreco
 * September 2015
 */
trait BitStringFactory[A] {
  /**
   *
   * @param values
   * @return
   */
  def fromBitIndicesToSet(values : Seq[Int]) : BitString[A]
  def fromByteSeq(values : Seq[Byte]) : BitString[A]

}

@SQLUserDefinedType(udt = classOf[BitStringUDT])
trait BitString[A] {
  /**
   *
   * @param other
   * @return
   */
  def intersectionCount(other : A) : Int

  /**
   *
   * @param other
   * @return
   */
  def intersectionCount(other: BitString[_]): Int = {
    if(other.isInstanceOf[A]){
      intersectionCount(other.asInstanceOf[A])
    } else {
      getIndexes.intersect(other.getIndexes).length
    }
  }

  /**
   *
   * @param start
   * @param end
   * @return
   */
  def get(start : Int, end : Int) : Int

  /**
   *
   * @return
   */
  def getIndexes : Seq[Int]

  /**
   *
   * @return
   */
  def toLong : Long

  /**
   *
   * @return
   */
  def toByteArray : Array[Byte]
}

/**
 *
 */
object BitStringTypes {
  sealed abstract class BitStringType(val num : Byte, val name : String)

  case object CBS extends BitStringType(0, "ColtBitString")
  case object LFBS extends BitStringType(1, "LuceneFixedBitString")
  case object SBSBS extends BitStringType(2, "SparseBitSetBitString")
  case object MBS extends BitStringType(3, "MinimalBitString")
}

/**
 *
 */
class BitStringUDT extends UserDefinedType[BitString[_]] {

  /**
   *
   * @return
   */
  override def sqlType: DataType =
    StructType(Seq(StructField("code", ByteType), StructField("value", BinaryType)))

  /**
   *
   * @param obj
   * @return
   */
  override def serialize(obj: Any): InternalRow = {
    obj match {
      case cbs : ColtBitString =>
        val row = new GenericMutableRow(2)
        row.setByte(0, BitStringTypes.CBS.num)
        row.update(1, cbs.toByteArray)
        row
      case lfbs : LuceneFixedBitString =>
        val row = new GenericMutableRow(2)
        row.setByte(0, BitStringTypes.LFBS.num)
        row.update(1, lfbs.toByteArray)
        row
      case sbsbs : SparseBitSetBitString =>
        val row = new GenericMutableRow(2)
        row.setByte(0, BitStringTypes.SBSBS.num)
        row.update(1, sbsbs.toByteArray)
        row
      case mbs : BitString[_] =>
        val row = new GenericMutableRow(2)
        row.setByte(0, BitStringTypes.MBS.num)
        row.update(1, mbs.toByteArray)
        row
    }
  }

  /**
   *
   * @param datum
   * @return
   */
  override def deserialize(datum: Any): BitString[_] = {
    if(datum.isInstanceOf[InternalRow]){
      val row = datum.asInstanceOf[InternalRow]
        require(row.numFields == 2)

        val code = row.getByte(0)
        val values = row.getBinary(1)

        return code match {
          case BitStringTypes.CBS.num => ColtBitString.fromByteSeq(values)
          case BitStringTypes.LFBS.num => LuceneFixedBitString.fromByteSeq(values)
          case BitStringTypes.SBSBS.num => SparseBitSetBitString.fromByteSeq(values)
          case BitStringTypes.MBS.num => MinimalBitString.fromByteSeq(values)
        }
    }

    null
  }

  /**
   *
   * @return
   */
  override def userClass: Class[BitString[_]] = classOf[BitString[_]]

  /**
   *
   * @return
   */
  override def asNullable: BitStringUDT = this
}


object BitString {
  type BitStringType = MinimalBitString

  /**
   *
   * @param values
   * @return
   */
  def fromBitIndicesToSet(values : Seq[Int]) : BitString[_] = {
    typeOf[BitStringType] match {
      case t if t =:= typeOf[ColtBitString] => ColtBitString.fromBitIndicesToSet(values)
      case t if t =:= typeOf[LuceneFixedBitString] => LuceneFixedBitString.fromBitIndicesToSet(values)
      case t if t =:= typeOf[SparseBitSetBitString] => SparseBitSetBitString.fromBitIndicesToSet(values)
      case t if t =:= typeOf[MinimalBitString] => MinimalBitString.fromBitIndicesToSet(values)
      case _ => null
    }
  }

  /**
   *
   * @param bytes
   * @return
   */
  def fromByteArray(bytes: Array[Byte]): BitString[_] = {
    val bis = new ByteArrayInputStream(bytes)
    val in = new ObjectInputStream(bis)

    val readObject = in.readObject()

    readObject match {
      case v : cern.colt.bitvector.BitVector => new ColtBitString(v)
      case v : org.apache.lucene.util.FixedBitSet => new LuceneFixedBitString(v)
      case v : com.zaxxer.sparsebits.SparseBitSet => new SparseBitSetBitString(v)
      case _ => new MinimalBitString(BitSet.valueOf(bytes))
    }
  }
}