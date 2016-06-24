package ch.unibas.dmi.dbis.adam.query.handler.internal

import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.query.handler.generic.{ExpressionDetails, QueryExpression}
import org.apache.http.annotation.Experimental
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
  * ADAMpro
  *
  * Ivan Giangreco
  * June 2016
  */
@Experimental
case class CompoundIndexScanExpression(private val exprs: Seq[IndexScanExpression], id: Option[String] = None)(filterExpr: Option[QueryExpression] = None)(@transient implicit val ac: AdamContext) extends QueryExpression(id) {
  override val info = ExpressionDetails(None, Some("Compound Query Index Scan Expression"), id, None)
  children ++= exprs ++ filterExpr.map(Seq(_)).getOrElse(Seq())
  var confidence: Option[Float] = None

  //only work on one entity
  assert(exprs.map(_.index.entityname).distinct.length == 1)
  //use multiple indices
  assert(exprs.length >= 2)

  val entity = exprs.head.index.entity.get

  override protected def run(filter: Option[DataFrame] = None)(implicit ac: AdamContext): Option[DataFrame] = {
    log.debug("evaluate compound query index scan")

    ac.sc.setJobGroup(id.getOrElse(""), "compound query index scan", interruptOnCancel = true)

    exprs.map(_.filter = filter)
    val results = exprs.map(_.evaluate())

    val res = results.filter(_.isDefined).map(_.get).reduce[DataFrame] { case (a, b) => {
      val dfa = a.select(entity.pk.name, FieldNames.distanceColumnName).withColumnRenamed(FieldNames.distanceColumnName, "a-" + FieldNames.distanceColumnName)
      val dfb = b.select(entity.pk.name, FieldNames.distanceColumnName).withColumnRenamed(FieldNames.distanceColumnName, "b-" + FieldNames.distanceColumnName)

      var res = dfa.join(dfb, entity.pk.name)
      res = res.withColumn(FieldNames.distanceColumnName, distCombinationUDF(res("a-" + FieldNames.distanceColumnName), res("b-" + FieldNames.distanceColumnName)))
      res = res.drop("a-" + FieldNames.distanceColumnName).drop("b-" + FieldNames.distanceColumnName)

      res
    }}

    Some(res)
  }

  val distCombinationUDF = udf((a: Float, b: Float) => {
    //TODO: possibly use other functions
    a + b
  })

  override def equals(that: Any): Boolean =
    that match {
      case that: CompoundIndexScanExpression => this.exprs.equals(that.exprs)
      case _ => exprs.equals(that)
    }

  override def hashCode(): Int = exprs.hashCode
}
