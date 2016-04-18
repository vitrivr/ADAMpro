package ch.unibas.dmi.dbis.adam.query.progressive

import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.index.Index._
import ch.unibas.dmi.dbis.adam.index.IndexHandler
import ch.unibas.dmi.dbis.adam.main.AdamContext

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
  def getPaths[U](entityname: EntityName) : Seq[IndexName]
}

/**
  * Chooses all index paths for progressive query.
  * @param ac
  */
class AllProgressivePathChooser(implicit ac: AdamContext) extends ProgressivePathChooser {
  override def getPaths[U](entityname: EntityName): Seq[IndexName] = {
    IndexHandler.list(entityname).map(_._1)
  }
}

/**
  * Chooses first index based on given index types.
  * @param indextypenames
  * @param ac
  */
class IndexTypeProgressivePathChooser(indextypenames : Seq[IndexTypeName])(implicit ac: AdamContext) extends ProgressivePathChooser {
  override def getPaths[U](entityname: EntityName): Seq[IndexName] = {
    indextypenames.map(indextypename => IndexHandler.list(entityname).head).map(_._1)
  }
}

/**
  * Chooses index based on names in given list.
  * @param indexnames
  * @param ac
  */
class IndexnameSpecifiedProgressivePathChooser(indexnames : Seq[IndexName])(implicit ac: AdamContext) extends ProgressivePathChooser {
  override def getPaths[U](entityname: EntityName): Seq[IndexName] = indexnames
}