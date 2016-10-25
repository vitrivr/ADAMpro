package ch.unibas.dmi.dbis.adam.helpers.benchmark

import ch.unibas.dmi.dbis.adam.api.IndexOp
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.exception.GeneralAdamException
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.QueryHints
import ch.unibas.dmi.dbis.adam.query.QueryHints.{IndexQueryHint, QueryHint}
import ch.unibas.dmi.dbis.adam.query.distance.EuclideanDistance
import ch.unibas.dmi.dbis.adam.utils.Logging

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * September 2016
  */
private[benchmark] trait IndexCollection extends Logging with Serializable {
  def getIndexes: Seq[Index]
}

/**
  *
  * @param entityname
  */
private[benchmark] case class ExistingIndexCollection(entityname: EntityName, attribute: String)(@transient implicit val ac: AdamContext) extends IndexCollection {
  def this(entityname: EntityName, attribute: String, params: Map[String, String])(implicit ac: AdamContext) {
    this(entityname, attribute)
  }

  override def getIndexes: Seq[Index] = Entity.load(entityname).get.indexes.filterNot(_.isFailure).map(_.get).filter(_.attribute == attribute)
}

/**
  *
  * @param entityname
  */
private[benchmark] case class NewIndexCollection(entityname: EntityName, attribute: String, hints: Seq[QueryHint])(@transient implicit val ac: AdamContext) extends IndexCollection {
  def this(entityname: EntityName, attribute: String, params: Map[String, String])(implicit ac: AdamContext) {
    this(entityname, attribute, params.get("hints").get.split(",").map(QueryHints.withName(_)).filter(_.isDefined).map(_.get))
  }

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
}

object IndexCollectionFactory {

  def apply(entityname: EntityName, attribute: String, ico: IndexCollectionOption, params: Map[String, String])(implicit ac: AdamContext): IndexCollection = {
    ico match {
      case NewIndexCollectionOption => new NewIndexCollection(entityname, attribute, params)
      case ExistingIndexCollectionOption => new ExistingIndexCollection(entityname, attribute, params)
      case _ => throw new GeneralAdamException("index collection option not known")
    }
  }

  sealed abstract class IndexCollectionOption

  case object ExistingIndexCollectionOption extends IndexCollectionOption

  case object NewIndexCollectionOption extends IndexCollectionOption

}