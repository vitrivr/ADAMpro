package ch.unibas.dmi.dbis.adam.data.types.bitString

import java.io.{ByteArrayInputStream, ObjectInputStream}
import java.util

import org.apache.spark.sql.Row
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
    getIndexes.intersect(other.getIndexes).length
  }

  /**
   *
   * @param start
   * @param end
   * @return
   */
  def get(start : Int, end : Int) : BitString[A]

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


class BitStringUDT extends UserDefinedType[BitString[_]] {

  /**
   *
   * @return
   */
  override def sqlType: DataType =
    StructType(Seq(StructField("code", StringType), StructField("value", BinaryType)))

  /**
   *
   * @param obj
   * @return
   */
  //TODO: use ENUMS
  override def serialize(obj: Any): Row = {
    obj match {
      case cbs : ColtBitString =>
        val row = new GenericMutableRow(2)
        row.setString(0, "cbs")
        row.update(1, cbs.toByteArray)
        row
      case lfbs : LuceneFixedBitString =>
        val row = new GenericMutableRow(2)
        row.setString(0, "lfbs")
        row.update(1, lfbs.toByteArray)
        row
      case sbsbs : SparseBitSetBitString =>
        val row = new GenericMutableRow(2)
        row.setString(0, "sbsbs")
        row.update(1, sbsbs.toByteArray)
        row
      case mbs : BitString[_] =>
        val row = new GenericMutableRow(2)
        row.setString(0, "mbs")
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
    if(datum.isInstanceOf[Row]){
      val row = datum.asInstanceOf[Row]
        require(row.length == 2)

        val code = row.getString(0)
        val values = row.getAs[Array[Byte]](1)

        return code match {
          case "cbs" => ColtBitString.fromByteSeq(values)
          case "lfbs" => LuceneFixedBitString.fromByteSeq(values)
          case "sbsbs" => SparseBitSetBitString.fromByteSeq(values)
          case "mbs" => MinimalBitString.fromByteSeq(values)
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
      case _ => new MinimalBitString(util.BitSet.valueOf(bytes))
    }
  }
}