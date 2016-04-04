package ch.unibas.dmi.dbis.adam.query.handler

import java.util.concurrent.TimeUnit

import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.entity.Entity
import ch.unibas.dmi.dbis.adam.entity.Entity._
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.query.Result
import ch.unibas.dmi.dbis.adam.query.query.NearestNeighbourQuery
import ch.unibas.dmi.dbis.adam.query.scanner.FeatureScanner
import org.apache.spark.sql.{Row, DataFrame}

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
  * adampro
  *
  * Ivan Giangreco
  * March 2016
  */
object CompoundQueryHandler {
  case class CompoundQueryHolder(entityname: EntityName, nnq: NearestNeighbourQuery, expr : Expression, withMetadata: Boolean) extends Expression {
    override def eval() = indexQueryWithResults(entityname)(nnq, expr, withMetadata)
  }

  /**
    *
    * @param entityname
    * @param nnq
    * @param expr
    * @param withMetadata
    * @return
    */
  def indexQueryWithResults(entityname: EntityName)(nnq: NearestNeighbourQuery, expr : Expression, withMetadata: Boolean) : DataFrame = {
    val tidList = expr.eval().map(x => Result(0.toFloat, x.getAs[Long](FieldNames.idColumnName))).collect().toSet
    var res = FeatureScanner(Entity.load(entityname).get, nnq, Some(tidList.map(_.tid)))

    if (withMetadata) {
      log.debug("join metadata to results of compound query")
      res = QueryHandler.joinWithMetadata(entityname, res)
    }

    res
  }

  /**
    *
    * @param expr
    * @return
    */
  def indexOnlyQuery(expr : Expression) = {
    val rdd =  expr.eval().map(x => Row(0.toFloat, x.getAs[Long](FieldNames.idColumnName)))
    SparkStartup.sqlContext.createDataFrame(rdd, Result.resultSchema)
  }

  /**
    *
    */
  abstract class Expression {
    def eval(): DataFrame
  }

  /**
    *
    * @param l
    * @param r
    */
  case class UnionExpression(l: Expression, r: Expression) extends Expression {
    override def eval(): DataFrame = {
      val lfut = Future {
        l.eval().select(FieldNames.idColumnName)
      }
      val rfut = Future {
        r.eval().select(FieldNames.idColumnName)
      }

      val f = for {
        leftResult <- lfut
        rightResult <- rfut
      } yield ({
        //possibly return DF with type Result here and sum up distance field
        leftResult.unionAll(rightResult).dropDuplicates(Seq(FieldNames.idColumnName))
      })

      Await.result(f, Duration(100, TimeUnit.SECONDS))
    }
  }

  /**
    *
    * @param l
    * @param r
    */
  case class IntersectExpression(l: Expression, r: Expression) extends Expression {
    override def eval(): DataFrame = {
      val lfut = Future {
        l.eval().select(FieldNames.idColumnName)
      }
      val rfut = Future {
        r.eval().select(FieldNames.idColumnName)
      }

      val f = for {
        leftResult <- lfut
        rightResult <- rfut
      } yield ({
        //possibly return DF with type Result here and sum up distance field
        leftResult.intersect(rightResult)
      })

      Await.result(f, Duration(100, TimeUnit.SECONDS))

    }
  }

  /**
    *
    * @param l
    * @param r
    */
  case class ExceptExpression(l: Expression, r: Expression) extends Expression {
    override def eval(): DataFrame = {
      val lfut = Future {
        l.eval().select(FieldNames.idColumnName)
      }
      val rfut = Future {
        r.eval().select(FieldNames.idColumnName)
      }

      val f = for {
        leftResult <- lfut
        rightResult <- rfut
      } yield ({
        //possibly return DF with type Result here and sum up distance field
        leftResult.except(rightResult)
      })

      Await.result(f, Duration(100, TimeUnit.SECONDS))
    }
  }
}
