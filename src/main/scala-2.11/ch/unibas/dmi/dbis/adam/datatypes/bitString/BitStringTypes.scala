package ch.unibas.dmi.dbis.adam.datatypes.bitString

/**
 * adamtwo
 *
 * Ivan Giangreco
 * October 2015
 */
object BitStringTypes {
  sealed abstract class BitStringType(val num : Byte, val factory : BitStringFactory)

  case object CBS extends BitStringType(0, ColtBitString)
  case object LFBS extends BitStringType(1, LuceneFixedBitString)
  case object SBSBS extends BitStringType(2, SparseBitSetBitString)
  case object MBS extends BitStringType(3, MinimalBitString)
}