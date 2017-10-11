package org.vitrivr.adampro.utils.ml


import java.io.File

import breeze.linalg.DenseVector
import org.apache.spark.mllib.classification.LogisticRegressionModel
import org.apache.spark.mllib.classification.LogisticRegressionWithSGD
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression._
import org.apache.spark.mllib.tree.{GradientBoostedTrees, RandomForest}
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.model.{GradientBoostedTreesModel, RandomForestModel}
import org.apache.spark.rdd.RDD
import org.vitrivr.adampro.process.SharedComponentContext
import org.vitrivr.adampro.utils.Logging
import org.vitrivr.adampro.utils.ml.Regression._


/**
  * ADAMpro
  *
  * Ivan Giangreco
  * August 2017
  */
class Regression(val path: String)(@transient implicit val ac: SharedComponentContext) extends Logging with Serializable {
  def modelPath(name : String) = path + "/model/model_" + name + MODEL_SUFFIX

  val algorithms = Seq(
    new LinearRegression(modelPath("lin")),
    new LogisticRegression(modelPath("log")),
    new RidgeRegression(modelPath("rig")),
    new LassoRegression(modelPath("lasso")),
    new RandomForestRegression(modelPath("rf")),
    new GBTRegression(modelPath("gbt"))
  )

  def train(data: Seq[TrainingSample])(implicit ac: SharedComponentContext): Unit = {
    val labelledData = data.map(x => LabeledPoint(x.time, Vectors.dense(x.f.toArray)))
    val rdd = ac.sc.parallelize(labelledData)

    rdd.saveAsObjectFile(path + "/data/data_" + (System.currentTimeMillis() / 1000L).toString + DATA_SUFFIX)




    val trainingData = new File(path + "/data/").listFiles.filter(_.isFile).filter(_.getName.endsWith(DATA_SUFFIX)).map { dataFile =>
      ac.sc.objectFile[LabeledPoint](dataFile.getAbsolutePath)
    }.reduce(_ union _)

    algorithms.foreach { algo =>
      algo.train(trainingData)
    }
  }


  def test(f: DenseVector[Double]): Double = {
    log.info(algorithms.map(x => (x.getClass.getSimpleName, x.test(f))).mkString("; "))

    algorithms.head.test(f)
  }
}

object Regression {
  val MODEL_SUFFIX = ".model"
  val DATA_SUFFIX = ".data"


  abstract class RegressionModelClass {
    def train(input: RDD[LabeledPoint]) : Unit

    def test(f: DenseVector[Double]) : Double
  }

  /**
    *
    * @param path
    * @param ac
    */
  case class LinearRegression(path: String)(@transient implicit val ac: SharedComponentContext) extends RegressionModelClass {
    var model: Option[LinearRegressionModel] =  None

    override def train(input: RDD[LabeledPoint]) : Unit = {
      model = None
      new LinearRegressionWithSGD().run(input).save(ac.sc, path)
    }

    override def test(f: DenseVector[Double]) : Double = {
      if(model.isEmpty){
        model = Some(LinearRegressionModel.load(ac.sc, path))
      }

      model.get.predict(Vectors.dense(f.toArray))
    }
  }


  /**
    *
    * @param path
    * @param ac
    */
  case class LogisticRegression(path: String)(@transient implicit val ac: SharedComponentContext) extends RegressionModelClass {
    var model: Option[LogisticRegressionModel] =  None

    override def train(input: RDD[LabeledPoint]) : Unit = {
      model = None
      new LogisticRegressionWithSGD().run(input).save(ac.sc, path)
    }

    override def test(f: DenseVector[Double]) : Double = {
      if(model.isEmpty){
        model = Some(LogisticRegressionModel.load(ac.sc, path))
      }

      model.get.predict(Vectors.dense(f.toArray))
    }
  }

  /**
    *
    * @param path
    * @param ac
    */
  case class RidgeRegression(path: String)(@transient implicit val ac: SharedComponentContext) extends RegressionModelClass {
    var model: Option[RidgeRegressionModel] =  None

    override def train(input: RDD[LabeledPoint]) : Unit = {
      model = None
      new RidgeRegressionWithSGD().run(input).save(ac.sc, path)
    }

    override def test(f: DenseVector[Double]) : Double = {
      if(model.isEmpty){
        model = Some(RidgeRegressionModel.load(ac.sc, path))
      }

      model.get.predict(Vectors.dense(f.toArray))
    }
  }

  /**
    *
    * @param path
    * @param ac
    */
  case class LassoRegression(path: String)(@transient implicit val ac: SharedComponentContext) extends RegressionModelClass {
    var model: Option[LassoModel] =  None

    override def train(input: RDD[LabeledPoint]) : Unit = {
      model = None
      new LassoWithSGD().run(input).save(ac.sc, path)
    }

    override def test(f: DenseVector[Double]) : Double = {
      if(model.isEmpty){
        model = Some(LassoModel.load(ac.sc, path))
      }

      model.get.predict(Vectors.dense(f.toArray))
    }
  }

  /**
    *
    * @param path
    * @param ac
    */
  case class RandomForestRegression(path: String)(@transient implicit val ac: SharedComponentContext) extends RegressionModelClass {
    var model: Option[RandomForestModel] =  None

    override def train(input: RDD[LabeledPoint]) : Unit = {
      model = None

      val categoricalFeaturesInfo = Map[Int, Int]()
      val numTrees = 10
      val featureSubsetStrategy = "auto"
      val impurity = "variance"
      val maxDepth = 4
      val maxBins = 32

      RandomForest.trainRegressor(input, categoricalFeaturesInfo,
        numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins).save(ac.sc, path)
    }

    override def test(f: DenseVector[Double]) : Double = {
      if(model.isEmpty){
        model = Some(RandomForestModel.load(ac.sc, path))
      }

      model.get.predict(Vectors.dense(f.toArray))
    }
  }


  /**
    *
    * @param path
    * @param ac
    */
  case class GBTRegression(path: String)(@transient implicit val ac: SharedComponentContext) extends RegressionModelClass {
    var model: Option[GradientBoostedTreesModel] =  None

    override def train(input: RDD[LabeledPoint]) : Unit = {
      model = None

      val boostingStrategy = BoostingStrategy.defaultParams("Regression")
      boostingStrategy.setNumIterations(20)
      boostingStrategy.treeStrategy.setMaxDepth(5)

      GradientBoostedTrees.train(input, boostingStrategy).save(ac.sc, path)
    }

    override def test(f: DenseVector[Double]) : Double = {
      if(model.isEmpty){
        model = Some(GradientBoostedTreesModel.load(ac.sc, path))
      }

      model.get.predict(Vectors.dense(f.toArray))
    }
  }
}