package org.vitrivr.adampro.query.ast.internal

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.vitrivr.adampro.config.AttributeNames
import org.vitrivr.adampro.data.datatypes.TupleID.TupleID
import org.vitrivr.adampro.data.entity.Entity
import org.vitrivr.adampro.data.entity.Entity.EntityName
import org.vitrivr.adampro.process.SharedComponentContext
import org.vitrivr.adampro.query.ast.generic.{ExpressionDetails, QueryEvaluationOptions, QueryExpression}
import org.vitrivr.adampro.query.distance.Distance
import org.vitrivr.adampro.query.query.{Predicate, RankingQuery}
import org.vitrivr.adampro.query.tracker.QueryTracker
import org.vitrivr.adampro.utils.Logging
import org.vitrivr.adampro.utils.exception.QueryNotConformException

/**
  * adamtwo
  *
  * Ivan Giangreco
  * May 2016
  */
case class SequentialScanExpression(private val entity: Entity)(private val nnq: RankingQuery, id: Option[String] = None)(filterExpr: Option[QueryExpression] = None)(@transient implicit val ac: SharedComponentContext) extends QueryExpression(id) {
  override val info = ExpressionDetails(Some(entity.entityname), Some("Sequential Scan Expression"), id, None)
  val sourceDescription = {
    if (filterExpr.isDefined) {
      filterExpr.get.info.scantype.getOrElse("undefined") + "->" + info.scantype.getOrElse("undefined")
    } else {
      info.scantype.getOrElse("undefined")
    }
  }

  _children ++= filterExpr.map(Seq(_)).getOrElse(Seq())

  def this(entityname: EntityName)(nnq: RankingQuery, id: Option[String])(filterExpr: Option[QueryExpression])(implicit ac: SharedComponentContext) {
    this(Entity.load(entityname).get)(nnq, id)(filterExpr)
  }

  override protected def run(options: Option[QueryEvaluationOptions], filter: Option[DataFrame] = None)(tracker: QueryTracker)(implicit ac: SharedComponentContext): Option[DataFrame] = {
    log.trace("perform sequential scan")

    ac.sc.setLocalProperty("spark.scheduler.pool", "sequential")
    ac.sc.setJobGroup(id.getOrElse(""), "sequential scan: " + entity.entityname.toString, interruptOnCancel = true)

    //check conformity
    if (!nnq.isConform(entity)) {
      throw QueryNotConformException("query is not conform to entity")
    }

    val prefilter = if (filter.isDefined && filterExpr.isDefined) {
      filterExpr.get.filter = filter

      val filterVals = filter.get.select(entity.pk.name)
      val filterExprVals = filterExpr.get.execute(options)(tracker).get.select(entity.pk.name)

      Some(filterVals.intersect(filterExprVals))
    } else if (filter.isDefined) {
      Some(filter.get.select(entity.pk.name))
    } else if (filterExpr.isDefined) {
      Some(filterExpr.get.execute(options)(tracker).get.select(entity.pk.name))
    } else {
      None
    }


    var result  = if(prefilter.isDefined){
      lazy val ids = prefilter.get.select(entity.pk.name).collect.map(_.getAs[TupleID](entity.pk.name))

      val df = if (ac.config.manualPredicatePushdown) {
        log.trace("using manual predicate")
        entity.getData(predicates = Seq(Predicate(entity.pk.name, None, ids))).get
      } else {
        entity.getData().get
      }

      ac.config.filteringMethod match {
        case ac.config.FilteringMethod.BloomFilter => {
          // Bloom
          log.trace("using Bloom filter")
          val bf = prefilter.get.stat.bloomFilter(entity.pk.name, 2000, 0.05)

          val bfBc = ac.sc.broadcast(bf)
          tracker.addBroadcast(bfBc)

          val filterUdf = udf((arg: TupleID) => if (arg != null) bfBc.value.mightContain(arg) else false)
          Some(df.filter(filterUdf(col(entity.pk.name))))
        }

        case ac.config.FilteringMethod.IsInFilter => {
          // Is in
          log.trace("using is in filter")
          val subdf = ids.sliding(500, 500).map{
            subids => df.filter(col(entity.pk.name).isin(subids : _*))
          }.reduce(_.union(_))

          Some(subdf)
        }

        case ac.config.FilteringMethod.SemiJoin => {
          // Semi join
          log.trace("using semijoin filter")
          Some(df.join(prefilter.get, df.col(entity.pk.name) === prefilter.get.col(entity.pk.name), "leftsemi"))
        }

        case _ => {
          log.trace("using no filter")
          Some(df)
        }
      }
    } else {
      val df = entity.getData().get

      Some(df)
    }



    //adjust output
    if (result.isDefined && options.isDefined && options.get.storeSourceProvenance) {
      result = Some(result.get.withColumn(AttributeNames.sourceColumnName, lit(sourceDescription)))
    }

    //distance computation
    result.map(SequentialScanExpression.scan(_, nnq)(tracker))
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
  def scan(df: DataFrame, nnq: RankingQuery)(tracker: QueryTracker)(implicit ac: SharedComponentContext): DataFrame = {
    val qBc = ac.sc.broadcast(nnq.q)
    tracker.addBroadcast(qBc)
    val wBc = ac.sc.broadcast(nnq.weights)
    tracker.addBroadcast(wBc)

    val dfDistance = if (df.schema.apply(nnq.attribute).dataType.isInstanceOf[StructType]) {
      //sparse vectors
      df.withColumn(AttributeNames.distanceColumnName, Distance.sparseVectorDistUDF(nnq, qBc, wBc)(df(nnq.attribute)))
    } else {
      //dense vectors
      df.withColumn(AttributeNames.distanceColumnName, Distance.denseVectorDistUDF(nnq, qBc, wBc)(df(nnq.attribute)))
    }

    import org.apache.spark.sql.functions.col
    val res = dfDistance.orderBy(col(AttributeNames.distanceColumnName))
      .limit(nnq.k)


    res
  }
}



