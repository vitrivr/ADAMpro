package org.vitrivr.adampro.data.index.structures.va

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}
import org.vitrivr.adampro.config.AttributeNames
import org.vitrivr.adampro.data.datatypes.vector.Vector._
import org.vitrivr.adampro.utils.exception.QueryNotConformException
import org.vitrivr.adampro.query.tracker.QueryTracker
import org.vitrivr.adampro.data.index.Index.IndexTypeName
import org.vitrivr.adampro.data.index._
import org.vitrivr.adampro.data.index.structures.IndexTypes
import org.vitrivr.adampro.data.index.structures.va.marks.{EquidistantMarksGenerator, EquifrequentMarksGenerator, MarksGenerator}
import org.vitrivr.adampro.data.index.structures.va.signature.FixedSignatureGenerator
import org.vitrivr.adampro.process.SharedComponentContext
import org.vitrivr.adampro.query.distance.{DistanceFunction, MinkowskiDistance}


/**
  * adamtwo
  *
  * Ivan Giangreco
  * September 2015
  *
  * VAF: this VA-File index will use for every dimension the same number of bits (original implementation)
  * note that using VAF, we may still use both the equidistant or the equifrequent marks generator
  */
class VAFIndexGenerator(fixedNumBitsPerDimension: Option[Int], marksGenerator: MarksGenerator, trainingSize: Int, distance: MinkowskiDistance)(@transient implicit val ac: SharedComponentContext) extends IndexGenerator {
  override val indextypename: IndexTypeName = IndexTypes.VAFINDEX


  /**
    *
    * @param data raw data to index
    * @return
    */
  override def index(data: DataFrame, attribute : String)(tracker : QueryTracker): (DataFrame, Serializable) = {
    log.trace("VA-File (fixed) started indexing")

    val meta = train(getSample(math.max(trainingSize, MINIMUM_NUMBER_OF_TUPLE), attribute)(data))

    val cellUDF = udf((c: DenseSparkVector) => {
      val cells = getCells(c, meta.marks)
      meta.signatureGenerator.toSignature(cells).serialize
    })
    val indexed = data.withColumn(AttributeNames.featureIndexColumnName, cellUDF(data(attribute)))

    log.trace("VA-File (fixed) finished indexing")

    (indexed, meta)
  }

  /**
    *
    * @param trainData training data
    * @return
    */
  private def train(trainData: Seq[IndexingTaskTuple]): VAIndexMetaData = {
    log.trace("VA-File (fixed) started training")

    val dim = trainData.head.ap_indexable.length

    //formula based on results from Weber/Böhm (2000): Trading Quality for Time with Nearest Neighbor Search
    val nbits = fixedNumBitsPerDimension.getOrElse(math.max(5, math.ceil(5 + 0.5 * math.log(dim / 10) / math.log(2))).toInt)
    val nmarks = 1 << nbits

    val signatureGenerator = new FixedSignatureGenerator(dim, nbits)
    val marks = marksGenerator.getMarks(trainData, nmarks)

    log.trace("VA-File (fixed) finished training")

    VAIndexMetaData(marks, signatureGenerator)
  }


  /**
    *
    */
  @inline private def getCells(f: Iterable[VectorBase], marks: Seq[Seq[VectorBase]]): Seq[Int] = {
    f.zip(marks).map {
      case (x, l) =>
        val index = l.toArray.indexWhere(p => p >= x, 1)
        if (index == -1) l.length - 1 - 1 else index - 1
    }.toSeq
  }
}

class VAFIndexGeneratorFactory extends IndexGeneratorFactory {
  /**
    * @param distance   distance function
    * @param properties indexing properties
    */
  def getIndexGenerator(distance: DistanceFunction, properties: Map[String, String] = Map[String, String]())(implicit ac: SharedComponentContext): IndexGenerator = {
    if (!distance.isInstanceOf[MinkowskiDistance]) {
      throw new QueryNotConformException("VAF index only supports Minkowski distance")
    }
    
    val maxMarks = properties.get("nmarks").map(_.toInt)

    val marksGeneratorDescription = properties.getOrElse("marktype", "equifrequent")
    val marksGenerator = marksGeneratorDescription.toLowerCase match {
      case "equifrequent" => EquifrequentMarksGenerator
      case "equidistant" => EquidistantMarksGenerator
    }

    val trainingSize = properties.getOrElse("ntraining", "5000").toInt

    new VAFIndexGenerator(maxMarks, marksGenerator, trainingSize, distance.asInstanceOf[MinkowskiDistance])
  }

  /**
    *
    * @return
    */
  override def parametersInfo: Seq[ParameterInfo] = Seq(
    new ParameterInfo("ntraining", "number of training tuples", Seq[String]()),
    new ParameterInfo("nbits", "number of marks per dimension", Seq(3, 4, 5, 6, 7, 8, 9).map(_.toString)),
    new ParameterInfo("marktype", "distribution of marks", Seq("equidistant", "equifrequent"))
  )
}