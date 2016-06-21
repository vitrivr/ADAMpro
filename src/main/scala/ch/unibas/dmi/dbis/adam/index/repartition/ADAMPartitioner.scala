package ch.unibas.dmi.dbis.adam.index.repartition

import ch.unibas.dmi.dbis.adam.utils.Logging

/**
  * adampar
  *
  * Created by silvan on 20.06.16.
  */
trait ADAMPartitioner extends Logging{

  def partitionerName: PartitionerChoice.Value
}
