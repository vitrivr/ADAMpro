package org.vitrivr.adampro.data.datatypes.bitstring

/**
 * adamtwo
 *
 * Ivan Giangreco
 * October 2015
 */
object BitStringTypes {
  sealed abstract class BitStringType(val num : Byte, val factory : BitStringFactory)
  case object EWAH extends BitStringType(0, EWAHBitString)
}