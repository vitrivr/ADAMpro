package org.vitrivr.adampro.utils.ml


import java.io.File

import breeze.linalg.DenseVector
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LabeledPoint, LinearRegressionModel, LinearRegressionWithSGD}
import org.vitrivr.adampro.process.SharedComponentContext
import org.vitrivr.adampro.utils.Logging



/**
  * ADAMpro
  *
  * Ivan Giangreco
  * August 2017
  */
class LinearRegression(val path : String)(@transient implicit val ac: SharedComponentContext) extends Logging with Serializable {
  val model : LinearRegressionModel = LinearRegressionModel.load(ac.sc, path  + LinearRegression.MODEL_SUFFIX)


  /**
    *
    * @param f
    * @return
    */
  def test(f: DenseVector[Double]): Double = model.predict(Vectors.dense(f.toArray))
}

object LinearRegression {
  val MODEL_SUFFIX = ".model"
  val DATA_SUFFIX = ".data"

  def train(data: Seq[TrainingSample], path : String)(implicit ac : SharedComponentContext): Unit ={
    val labelledData = data.map(x => LabeledPoint(x.time, Vectors.dense(x.f.toArray)))
    val rdd = ac.sc.parallelize(labelledData)

    rdd.saveAsObjectFile(path + "/data/data_" + (System.currentTimeMillis() / 1000L).toString + DATA_SUFFIX)

    val modelFile = new File(path  + MODEL_SUFFIX)

    if(modelFile.exists()){
      modelFile.delete()
    }

    val lr = new LinearRegressionWithSGD()

    val trainingData = new File(path + "/data/").listFiles.filter(_.isFile).filter(_.getName.endsWith(DATA_SUFFIX)).map{ dataFile =>
      ac.sc.objectFile[LabeledPoint](dataFile.getAbsolutePath)
    }.reduce(_ union _)


    val model = lr.run(trainingData)

    model.save(ac.sc, path + MODEL_SUFFIX)
  }
}