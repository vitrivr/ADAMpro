package org.vitrivr.adampro.data.index.structures.va

import org.apache.spark.mllib.feature.PCAModel
import org.vitrivr.adampro.data.index.structures.va.VAIndex._
import org.vitrivr.adampro.data.index.structures.va.signature.SignatureGenerator

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * September 2016
  */
private[adampro] class VAPlusIndexMetaData(override val marks: Marks, override val signatureGenerator: SignatureGenerator, val pca: PCAModel, val approximate : Boolean) extends VAIndexMetaData(marks, signatureGenerator) with Serializable {}
