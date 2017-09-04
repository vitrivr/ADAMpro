package org.vitrivr.adampro.utils.ml


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
  val model : LinearRegressionModel = LinearRegressionModel.load(ac.sc, path)


  /**
    *
    * @param f
    * @return
    */
  def test(f: DenseVector[Double]): Double = model.predict(Vectors.dense(f.toArray))
}

object LinearRegression {
  def train(data: Seq[TrainingSample], path : String)(implicit ac : SharedComponentContext): Unit ={
    val labelledData = data.map(x => LabeledPoint(x.time, Vectors.dense(x.f.toArray)))

    val lr = new LinearRegressionWithSGD()
    val model = lr.run(ac.sc.parallelize(labelledData))

    model.save(ac.sc, path)
  }
}