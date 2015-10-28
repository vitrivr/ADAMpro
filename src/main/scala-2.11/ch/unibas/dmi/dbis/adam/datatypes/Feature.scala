package ch.unibas.dmi.dbis.adam.datatypes

import breeze.linalg.{DenseVector, SparseVector, Vector}
import ch.unibas.dmi.dbis.adam.datatypes.Feature._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.types._

/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
object Feature {
  //type definition
  type VectorBase = Float
  type WorkingVector = Vector[VectorBase]
  type DenseWorkingVector = DenseVector[VectorBase]
  type SparseWorkingVector = SparseVector[VectorBase]

  type StoredVector = Seq[VectorBase]


  //conversions
  implicit def conv_stored2vector(value: StoredVector): DenseWorkingVector = new DenseVector[Float](value.toArray)

  implicit def conv_stored2vector(value: (Int, Seq[Int], StoredVector)): SparseWorkingVector = new SparseVector(value._2.toArray, value._3.toArray, value._1)

  implicit def conv_vector2stored(value: WorkingVector): StoredVector = value.toArray

  implicit def conv_str2stored(value: String): StoredVector = {
    if (value.length <= 3) {
      return Seq[Float]()
    }
    value.substring(1, value.length - 2).split(",").map(_.toDouble).map(_.toFloat).toSeq
  }

  implicit def conv_str2vector(value: String): WorkingVector = conv_stored2vector(conv_str2stored(value))

  implicit def conv_double2base(value: Double): Float = value.toFloat

  implicit def conv_double2vectorBase(value: Double): Float = value.toFloat

  implicit def conv_doublestored2floatstored(value: Seq[Double]): StoredVector = value.map(_.toFloat)

  implicit def conv_doublevector2floatvector(value: DenseVector[Double]): WorkingVector = new DenseVector[Float](value.data.map(_.toFloat))


}



@SQLUserDefinedType(udt = classOf[WorkingVectorWrapperUDT])
case class WorkingVectorWrapper(value : WorkingVector) extends Serializable {}




class WorkingVectorWrapperUDT extends UserDefinedType[WorkingVectorWrapper] {

  /**
   *
   * @return
   */
  override def sqlType: DataType = ByteType

  /**
   *
   * @param obj
   * @return
   */
  override def serialize(obj: Any): InternalRow = {
    if(!obj.isInstanceOf[WorkingVectorWrapper]){
      return null
    }

    obj.asInstanceOf[WorkingVectorWrapper].value match {
      case dwv : DenseWorkingVector =>
        val row = new GenericMutableRow(4)
        row.setByte(0, 0)
        row.setNullAt(1)
        row.setNullAt(2)
        row.update(3, new GenericArrayData(dwv.data.map(_.asInstanceOf[Any])))
        row
      case swv : SparseWorkingVector =>
        val row = new GenericMutableRow(4)
        row.setByte(0, 0)
        row.setInt(1, swv.length)
        row.update(2, new GenericArrayData(swv.index.map(_.asInstanceOf[Any])))
        row.update(3, new GenericArrayData(swv.data.map(_.asInstanceOf[Any])))
        row
    }
  }

  /**
   *
   * @param datum
   * @return
   */
  override def deserialize(datum: Any): WorkingVectorWrapper = {
    if(datum.isInstanceOf[InternalRow]){
      val row = datum.asInstanceOf[InternalRow]
      require(row.numFields == 4)

      val code = row.getByte(0)
      val data = row.getArray(3).toFloatArray()

      return code match {
        case 0 => {

          WorkingVectorWrapper(DenseVector(data))
        }
        case 1 => {
          val length = row.getInt(1)
          val index = row.getArray(2).toIntArray()

          WorkingVectorWrapper(new SparseVector(index, data, length))
        }
      }
    } else {
      null
    }
  }

  /**
   *
   * @return
   */
  override def userClass: Class[WorkingVectorWrapper] = classOf[WorkingVectorWrapper]

  /**
   *
   * @return
   */
  override def asNullable: WorkingVectorWrapperUDT = this
}