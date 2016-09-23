package ch.unibas.dmi.dbis.adam.helpers.benchmark

import ch.unibas.dmi.dbis.adam.api.IndexOp
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.QueryHints.{IndexQueryHint, QueryHint}
import ch.unibas.dmi.dbis.adam.query.distance.EuclideanDistance
import ch.unibas.dmi.dbis.adam.utils.Logging

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * September 2016
  */
trait BenchmarkIndexCollector extends Logging with Serializable {
  def getIndexes: Seq[Index]
}

/**
  *
  * @param entityname
  */
class ExistingIndexCollector(entityname: EntityName, attribute: String)(@transient implicit val ac: AdamContext) extends BenchmarkIndexCollector {
  override def getIndexes: Seq[Index] = Entity.load(entityname).get.indexes.filterNot(_.isFailure).map(_.get).filter(_.attribute == attribute)
}

/**
  *
  * @param entityname
  */
class NewIndexGeneratorCollector(entityname: EntityName, attribute: String, hints: Seq[QueryHint])(@transient implicit val ac: AdamContext) extends BenchmarkIndexCollector {
  private def combine[A](xs: Traversable[Traversable[A]]): Seq[Seq[A]] = {
    xs.foldLeft(Seq(Seq.empty[A])) {
      (x, y) => for (a <- x.view; b <- y) yield a :+ b
    }
  }

  override def getIndexes: Seq[Index] = {
    hints.flatMap { hint =>
      if (hint.isInstanceOf[IndexQueryHint]) {
        val iqh = hint.asInstanceOf[IndexQueryHint]

        val factory = iqh.structureType.indexGeneratorFactoryClass.newInstance()

        val params: Seq[Map[String, String]] = combine(factory.parametersInfo.map(x => x.suggestedValues.map(v => x.name -> v))).map(_.toMap)

        params.map { param =>
          IndexOp.create(entityname, attribute, iqh.structureType, EuclideanDistance, param)
        }
      } else {
        log.error("only index query hints accepted")
        null
      }

    }.filter(x => x != null && x.isSuccess).map(_.get)
  }

  //TODO: delete most of the created index structures...
}