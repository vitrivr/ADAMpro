package org.vitrivr.adampro.datatypes.feature

import breeze.linalg.{DenseVector, SparseVector}
import org.vitrivr.adampro.datatypes.feature.Feature.{DenseFeatureVector, FeatureVector, SparseFeatureVector, VectorBase}
import org.vitrivr.adampro.datatypes.feature.FeatureVectorTypes.{DenseFeatureVectorType, SparseFeatureVectorType}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.types._

/**
 * adamtwo
 *
 * Ivan Giangreco
 * October 2015
 */
@SQLUserDefinedType(udt = classOf[FeatureVectorWrapperUDT])
case class FeatureVectorWrapper(vector: FeatureVector) extends Serializable {
  def this(value : Seq[VectorBase]){
    this(new Feature.DenseFeatureVector(value.toArray))
  }

  def this(index : Seq[Int], value : Seq[VectorBase], length : Int){
    this(new Feature.SparseFeatureVector(index.toArray, value.toArray, length))
  }

  /**
    * Returns a scala data type "vector".
    *
    * @return
    */
  def toSeq : Seq[VectorBase] = vector.toDenseVector.toArray

  /**
    *
    * @return
    */
  override def toString : String = toSeq.mkString(",")
}

@SerialVersionUID(1L)
class FeatureVectorWrapperUDT extends UserDefinedType[FeatureVectorWrapper] {
  override def sqlType: DataType = {
    StructType(Seq(
      StructField("type", ByteType, nullable = false),
      StructField("size", IntegerType, nullable = true),
      StructField("indices", ArrayType(IntegerType, containsNull = false), nullable = true),
      StructField("values", ArrayType(FloatType, containsNull = false), nullable = true)))
  }
  override def userClass: Class[FeatureVectorWrapper] = classOf[FeatureVectorWrapper]
  override def asNullable: FeatureVectorWrapperUDT = this

  override def serialize(obj: Any): InternalRow = {
    val row = new GenericMutableRow(4)

    obj.asInstanceOf[FeatureVectorWrapper].vector match {
      case dwv: DenseFeatureVector =>
        row.setByte(0, DenseFeatureVectorType.num)
        row.update(3, new GenericArrayData(dwv.data.map(_.asInstanceOf[Any])))
      case swv: SparseFeatureVector =>
        row.setByte(0, SparseFeatureVectorType.num)
        row.setInt(1, swv.length)
        row.update(2, new GenericArrayData(swv.index.map(_.asInstanceOf[Any])))
        row.update(3, new GenericArrayData(swv.data.map(_.asInstanceOf[Any])))
      case _ =>
    }

    row
  }


  override def deserialize(datum: Any): FeatureVectorWrapper = {
    if (!datum.isInstanceOf[InternalRow]) {
      return null
    }

    val row = datum.asInstanceOf[InternalRow]
    require(row.numFields == 4)

    val code = row.getByte(0)
    val data = row.getArray(3).toFloatArray()

    code match {
      case DenseFeatureVectorType.num => FeatureVectorWrapper(DenseVector(data))
      case SparseFeatureVectorType.num =>
        val length = row.getInt(1)
        val index = row.getArray(2).toIntArray()

        FeatureVectorWrapper(new SparseVector(index, data, length))
    }
  }
}
