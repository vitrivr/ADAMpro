package org.vitrivr.adampro.query.handler.internal

import com.google.common.hash.{BloomFilter, Funnel, PrimitiveSink}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.vitrivr.adampro.config.AttributeNames
import org.vitrivr.adampro.datatypes.vector.Vector._
import org.vitrivr.adampro.entity.Entity
import org.vitrivr.adampro.entity.Entity.EntityName
import org.vitrivr.adampro.exception.QueryNotConformException
import org.vitrivr.adampro.main.AdamContext
import org.vitrivr.adampro.query.distance.Distance
import org.vitrivr.adampro.query.handler.generic.{ExpressionDetails, QueryEvaluationOptions, QueryExpression}
import org.vitrivr.adampro.query.query.NearestNeighbourQuery
import org.vitrivr.adampro.utils.Logging

/**
  * adamtwo
  *
  * Ivan Giangreco
  * May 2016
  */
case class SequentialScanExpression(private val entity: Entity)(private val nnq: NearestNeighbourQuery, id: Option[String] = None)(filterExpr: Option[QueryExpression] = None)(@transient implicit val ac: AdamContext) extends QueryExpression(id) {
  override val info = ExpressionDetails(Some(entity.entityname), Some("Sequential Scan Expression"), id, None)
  val sourceDescription = {
    if (filterExpr.isDefined) {
      filterExpr.get.info.scantype.getOrElse("undefined") + "->" + info.scantype.getOrElse("undefined")
    } else {
      info.scantype.getOrElse("undefined")
    }
  }

  _children ++= filterExpr.map(Seq(_)).getOrElse(Seq())

  def this(entityname: EntityName)(nnq: NearestNeighbourQuery, id: Option[String])(filterExpr: Option[QueryExpression])(implicit ac: AdamContext) {
    this(Entity.load(entityname).get)(nnq, id)(filterExpr)
  }

  override protected def run(options: Option[QueryEvaluationOptions], filter: Option[DataFrame] = None)(implicit ac: AdamContext): Option[DataFrame] = {
    log.debug("perform sequential scan")

    ac.sc.setLocalProperty("spark.scheduler.pool", "sequential")
    ac.sc.setJobGroup(id.getOrElse(""), "sequential scan: " + entity.entityname.toString, interruptOnCancel = true)

    //check conformity
    if (!nnq.isConform(entity)) {
      throw QueryNotConformException("query is not conform to entity")
    }

    val df = entity.getData().get

    //prepare filter
    val funnel = new Funnel[Any] {
      override def funnel(t: Any, primitiveSink: PrimitiveSink): Unit =
        t match {
          case s: String => primitiveSink.putUnencodedChars(s)
          case l: Long => primitiveSink.putLong(l)
          case i: Int => primitiveSink.putInt(i)
          case _ => primitiveSink.putUnencodedChars(t.toString)
        }
    }
    val ids = BloomFilter.create[Any](funnel, 1000, 0.05)

    if (filter.isDefined) {
      filter.get.select(entity.pk.name).collect().map(_.getAs[Any](entity.pk.name)).toSeq.foreach {
        ids.put(_)
      }
    }

    if (filterExpr.isDefined) {
      filterExpr.get.filter = filter
      filterExpr.get.evaluate(options).get.select(entity.pk.name).collect().map(_.getAs[Any](entity.pk.name)).toSeq.foreach {
        ids.put(_)
      }
    }

    var result = if (filter.isDefined || filterExpr.isDefined) {
      val idsBc = ac.sc.broadcast(ids)
      val filterUdf = udf((arg: Any) => idsBc.value.mightContain(arg))
      Some(df.filter(filterUdf(col(entity.pk.name))))
    } else {
      Some(df)
    }

    //adjust output
    if (result.isDefined && options.isDefined && options.get.storeSourceProvenance) {
      result = Some(result.get.withColumn(AttributeNames.sourceColumnName, lit(sourceDescription)))
    }

    //distance computation
    result.map(SequentialScanExpression.scan(_, nnq))
  }

  override def equals(other: Any): Boolean =
    other match {
      case that: SequentialScanExpression => this.entity.entityname.equals(that.entity.entityname) && this.nnq.equals(that.nnq)
      case _ => false
    }

  override def hashCode(): Int = {
    val prime = 31
    var result = 1
    result = prime * result + entity.hashCode
    result = prime * result + nnq.hashCode
    result
  }
}

object SequentialScanExpression extends Logging {

  /**
    * Scans the feature data based on a nearest neighbour query.
    *
    * @param df  data frame
    * @param nnq nearest neighbour query
    * @return
    */
  def scan(df: DataFrame, nnq: NearestNeighbourQuery)(implicit ac: AdamContext): DataFrame = {
    val q = ac.sc.broadcast(nnq.q)
    val w: Broadcast[Option[MathVector]] = ac.sc.broadcast(nnq.weights)

    val res = if(df.schema.apply(nnq.attribute).dataType.isInstanceOf[StructType]){
      //sparse vectors
      df.withColumn(AttributeNames.distanceColumnName, Distance.sparseVectorDistUDF(nnq, q, w)(df(nnq.attribute)))
    } else {
      //dense vectors
      df.withColumn(AttributeNames.distanceColumnName, Distance.denseVectorDistUDF(nnq, q, w)(df(nnq.attribute)))
    }

    import org.apache.spark.sql.functions.{col}
    res.orderBy(col(AttributeNames.distanceColumnName))
      .limit(nnq.k)
  }
}



