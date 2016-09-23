package ch.unibas.dmi.dbis.adam.index.structures.va

import breeze.linalg._
import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.datatypes.bitString.BitStringUDT
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.exception.QueryNotConformException
import ch.unibas.dmi.dbis.adam.index.Index.{IndexName, IndexTypeName}
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.index.structures.va.marks.{EquidistantMarksGenerator, EquifrequentMarksGenerator, MarksGenerator}
import ch.unibas.dmi.dbis.adam.index.structures.va.signature.VariableSignatureGenerator
import ch.unibas.dmi.dbis.adam.index.{ParameterInfo, IndexGenerator, IndexGeneratorFactory, IndexingTaskTuple}
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
  * VAV: this VA-File index will have a training phase in which we learn the number of bits per dimension (new version of VA-File)
  * note that using VAF, we may still use both the equidistant or the equifrequent marks generator
  */
class VAVIndexGenerator(nbits: Option[Int], marksGenerator: MarksGenerator, trainingSize: Int, distance: MinkowskiDistance)(@transient implicit val ac: AdamContext) extends IndexGenerator {
  override val indextypename: IndexTypeName = IndexTypes.VAVINDEX

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
    val fraction = Sampling.computeFractionForSampleSize(math.max(trainingSize, MINIMUM_NUMBER_OF_TUPLE), n, false)
    var trainData = data.sample(false, fraction).collect()
    if(trainData.length < MINIMUM_NUMBER_OF_TUPLE){
      trainData = data.take(MINIMUM_NUMBER_OF_TUPLE)
    }

    val meta = train(trainData)

    log.debug("VA-File (variable) indexing...")

    val indexdata = data.map(
      datum => {
        val cells = getCells(datum.feature, meta.marks)
        val signature = meta.signatureGenerator.toSignature(cells)
        Row(datum.id, signature)
      })

    val schema = StructType(Seq(
      StructField(entity.pk.name, entity.pk.fieldtype.datatype, false),
      StructField(FieldNames.featureIndexColumnName, new BitStringUDT, false)
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
    log.trace("VA-File (variable) started training")

    //data
    val dTrainData = trainData.map(x => x.feature.map(x => x.toDouble).toArray)

    val dataMatrix = DenseMatrix(dTrainData.toList: _*)

    val nfeatures = dTrainData.head.length
    val numComponents = math.min(nfeatures, nbits.getOrElse(nfeatures * 8))

    // pca
    val variance = diag(cov(dataMatrix, center = true)).toArray
    val sumVariance = variance.sum

    // compute shares of bits
    val modes = variance.map(variance => (variance / sumVariance * nbits.getOrElse(nfeatures * 8)).toInt)

    val signatureGenerator = new VariableSignatureGenerator(modes)
    val marks = marksGenerator.getMarks(trainData, modes.map(x => 2 << (x - 1)))

    log.trace("VA-File (variable) finished training")

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


class VAVIndexGeneratorFactory extends IndexGeneratorFactory {
  /**
    * @param distance   distance function
    * @param properties indexing properties
    */
  def getIndexGenerator(distance: DistanceFunction, properties: Map[String, String] = Map[String, String]())(implicit ac: AdamContext): IndexGenerator = {
    val marksGeneratorDescription = properties.getOrElse("marktype", "equifrequent")
    val marksGenerator = marksGeneratorDescription.toLowerCase match {
      case "equifrequent" => EquifrequentMarksGenerator
      case "equidistant" => EquidistantMarksGenerator
    }

    if (!distance.isInstanceOf[MinkowskiDistance]) {
      log.error("only Minkowski distances allowed for VAV Indexer")
      throw new QueryNotConformException()
    }

    val totalNumBits = if (properties.get("signature-nbits").isDefined) {
      Some(properties.get("signature-nbits").get.toInt)
    } else {
      None
    }
    val trainingSize = properties.getOrElse("ntraining", "1000").toInt

    new VAVIndexGenerator(totalNumBits, marksGenerator, trainingSize, distance.asInstanceOf[MinkowskiDistance])
  }


  /**
    *
    * @return
    */
  override def parametersInfo: Seq[ParameterInfo] = Seq(
    new ParameterInfo("ntraining", "number of training tuples", Seq[String]()),
    new ParameterInfo("signature-nbits", "number of bits for the complete signature", Seq(32, 64, 128, 256, 1024).map(_.toString)),
    new ParameterInfo("marktype", "distribution of marks", Seq("equidistant", "equifrequent"))
  )
}