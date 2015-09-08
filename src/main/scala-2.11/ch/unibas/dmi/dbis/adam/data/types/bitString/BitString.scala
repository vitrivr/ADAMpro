package ch.unibas.dmi.dbis.adam.data.types.bitString

import java.io.{ByteArrayInputStream, ObjectInputStream}
import java.util

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
}


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
  def toByteSeq : Seq[Byte]
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