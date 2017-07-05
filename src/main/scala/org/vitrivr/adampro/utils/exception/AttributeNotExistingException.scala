package org.vitrivr.adampro.utils.exception

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * January 2017
  */
case class AttributeNotExistingException(message : String = "Attribute does not exists.")  extends GeneralAdamException(message)
