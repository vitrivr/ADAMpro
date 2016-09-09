package ch.unibas.dmi.dbis.adam.index.structures.va

import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.datatypes.bitString.BitStringUDT
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature.{FeatureVector, VectorBase}
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.exception.QueryNotConformException
import ch.unibas.dmi.dbis.adam.index.Index.{IndexName, IndexTypeName}
import ch.unibas.dmi.dbis.adam.index._
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.index.structures.va.marks.{EquidistantMarksGenerator, EquifrequentMarksGenerator, MarksGenerator}
import ch.unibas.dmi.dbis.adam.index.structures.va.signature.FixedSignatureGenerator
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.distance.{DistanceFunction, MinkowskiDistance}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.util.random.Sampling


/**
  * adamtwo
  *
  * Ivan Giangreco
  * September 2015
  *
  * VAF: this VA-File index will use for every dimension the same number of bits (original implementation)
  * note that using VAF, we may still use both the equidistant or the equifrequent marks generator
  */
class VAFIndexer(maxMarks: Int = 64, marksGenerator: MarksGenerator, bitsPerDimension: Int, trainingSize: Int, distance: MinkowskiDistance)(@transient implicit val ac: AdamContext) extends IndexGenerator {
  override val indextypename: IndexTypeName = IndexTypes.VAFINDEX

  /**
    * 
    * @param indexname name of index
    * @param entityname name of entity
    * @param data data to index
    * @return
    */
  override def index(indexname: IndexName, entityname: EntityName, data: RDD[IndexingTaskTuple[_]]): (DataFrame, Serializable) = {
    val entity = Entity.load(entityname).get

    val n = entity.count
    val fraction = Sampling.computeFractionForSampleSize(math.max(trainingSize, MINIMUM_NUMBER_OF_TUPLE), n, withReplacement = false)
    var trainData = data.sample(false, fraction).collect()
    if(trainData.length < MINIMUM_NUMBER_OF_TUPLE){
      trainData = data.take(MINIMUM_NUMBER_OF_TUPLE)
    }

    val meta = train(trainData)

    log.debug("VA-File (fixed) indexing...")

    val indexdata = data.map(
      datum => {
        val cells = getCells(datum.feature, meta.marks)
        val signature = meta.signatureGenerator.toSignature(cells)
        Row(datum.id, signature)
      })

    val schema = StructType(Seq(
      StructField(entity.pk.name, entity.pk.fieldtype.datatype, nullable = false),
      StructField(FieldNames.featureIndexColumnName, new BitStringUDT, nullable = false)
    ))

    val df = ac.sqlContext.createDataFrame(indexdata, schema)

    (df, meta)
  }

  /**
    *
    * @param trainData training data
    * @return
    */
  private def train(trainData: Array[IndexingTaskTuple[_]]): VAIndexMetaData = {
    log.trace("VA-File (fixed) started training")

    val dim = trainData.head.feature.length

    val signatureGenerator = new FixedSignatureGenerator(dim, bitsPerDimension)
    val marks = marksGenerator.getMarks(trainData, maxMarks)

    log.trace("VA-File (fixed) finished training")

    VAIndexMetaData(marks, signatureGenerator)
  }


  /**
    *
    */
  @inline private def getCells(f: FeatureVector, marks: Seq[Seq[VectorBase]]): Seq[Int] = {
    f.toArray.zip(marks).map {
      case (x, l) =>
        val index = l.toArray.indexWhere(p => p >= x, 1)
        if (index == -1) l.length - 1 - 1 else index - 1
    }
  }
}

class VAFIndexGeneratorFactory extends IndexGeneratorFactory {
  /**
    * @param distance   distance function
    * @param properties indexing properties
    */
  def getIndexGenerator(distance: DistanceFunction, properties: Map[String, String] = Map[String, String]())(implicit ac: AdamContext): IndexGenerator = {
    val maxMarks = properties.getOrElse("nmarks", "64").toInt

    if (!distance.isInstanceOf[MinkowskiDistance]) {
      log.error("only Minkowski distances allowed for VAF Indexer")
      throw new QueryNotConformException()
    }

    val marksGeneratorDescription = properties.getOrElse("marktype", "equifrequent")
    val marksGenerator = marksGeneratorDescription.toLowerCase match {
      case "equifrequent" => EquifrequentMarksGenerator
      case "equidistant" => EquidistantMarksGenerator
    }

    val fixedNumBitsPerDimension = properties.getOrElse("signature-nbits-dim", math.ceil(scala.math.log(maxMarks) / scala.math.log(2)).toInt.toString).toInt
    val trainingSize = properties.getOrElse("ntraining", "5000").toInt

    new VAFIndexer(maxMarks, marksGenerator, fixedNumBitsPerDimension, trainingSize, distance.asInstanceOf[MinkowskiDistance])
  }
}