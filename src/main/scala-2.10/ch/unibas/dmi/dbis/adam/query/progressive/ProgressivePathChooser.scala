package ch.unibas.dmi.dbis.adam.query.progressive

import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.index.IndexHandler
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.handler.QueryHints.{IndexQueryHint, QueryHint}
import org.apache.log4j.Logger

/**
  * adampro
  *
  * Ivan Giangreco
  * April 2016
  */
/**
  * Specifies which indexes to use in progressive querying
  */
trait ProgressivePathChooser {
  val log = Logger.getLogger(getClass.getName)
  def getPaths[U](entityname: EntityName): Seq[IndexName]
}

/**
  * Chooses from all index types one.
  *
  * @param ac
  */
class SimpleProgressivePathChooser()(implicit ac: AdamContext) extends ProgressivePathChooser {
  override def getPaths[U](entityname: EntityName): Seq[IndexName] = {
    IndexTypes.values.map(indextypename => IndexHandler.list(entityname, indextypename).head).map(_._1)
  }
}

/**
  * Chooses all index paths for progressive query.
  *
  * @param ac
  */
class AllProgressivePathChooser(implicit ac: AdamContext) extends ProgressivePathChooser {
  override def getPaths[U](entityname: EntityName): Seq[IndexName] = {
    IndexHandler.list(entityname).map(_._1)
  }
}

/**
  * Chooses first index based on given index types.
  *
  * @param indextypenames
  * @param ac
  */
class IndexTypeProgressivePathChooser(indextypenames: Seq[IndexTypeName])(implicit ac: AdamContext) extends ProgressivePathChooser {
  override def getPaths[U](entityname: EntityName): Seq[IndexName] = {
    indextypenames.map(indextypename => IndexHandler.list(entityname, indextypename).head).map(_._1)
  }
}

/**
  * Chooses first index based on hints given.
  *
  * @param hints list of QueryHints, note that only IndexQueryHints are accepted at the moment
  * @param ac
  */
class QueryHintsProgressivePathChooser(hints: Seq[QueryHint])(implicit ac: AdamContext) extends ProgressivePathChooser {
  override def getPaths[U](entityname: EntityName): Seq[IndexName] = {
    hints.map(choosePlan(entityname, _)).filterNot(_ == null).toSeq
  }

  private def choosePlan(entityname: EntityName, hint: QueryHint)(implicit ac: AdamContext) = {
    if (hint.isInstanceOf[IndexQueryHint]) {
      IndexHandler.list(entityname, hint.asInstanceOf[IndexQueryHint].structureType).head._1
    } else {
      log.error("only query hints of the type IndexQueryHint are accepted")
      null
    }
  }
}

/**
  * Chooses index based on names in given list.
  *
  * @param indexnames
  * @param ac
  */
class IndexnameSpecifiedProgressivePathChooser(indexnames: Seq[IndexName])(implicit ac: AdamContext) extends ProgressivePathChooser {
  override def getPaths[U](entityname: EntityName): Seq[IndexName] = indexnames
}