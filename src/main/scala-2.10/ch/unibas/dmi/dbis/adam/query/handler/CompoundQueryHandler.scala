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
    override def eval() = indexQueryWithResults(entityname)(nnq, expr, withMetadata).map(r => Result(0.toFloat, r.getAs[Long](FieldNames.idColumnName))).collect()
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
    val tidList = expr.eval()
    var res = FeatureScanner(Entity.load(entityname).get, nnq, Some(tidList.map(_.tid).toSet))

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
  def indexOnlyQuery(expr : Expression) : DataFrame = {
    val rdd =  SparkStartup.sc.parallelize(expr.eval().map(res => Row(res.distance, res.tid)))
    SparkStartup.sqlContext.createDataFrame(rdd, Result.resultSchema)
  }

  /**
    *
    */
  abstract class Expression {
    /**
      *
      * @return
      */
    def eval(): Seq[Result]

    /**
      *
      * @return
      */
    def evalToDF() = {
      val rdd =  SparkStartup.sc.parallelize(eval().map(res => Row(res.distance, res.tid)))
      SparkStartup.sqlContext.createDataFrame(rdd, Result.resultSchema)
    }
  }

  /**
    *
    * @param l
    * @param r
    */
  case class UnionExpression(l: Expression, r: Expression) extends Expression {
    override def eval(): Seq[Result] = {
      val lfut = Future {
        l.eval()
      }
      val rfut = Future {
        r.eval()
      }

      val f = for {
        leftResult <- lfut
        rightResult <- rfut
      } yield ({
        //possibly return DF with type Result here and sum up distance field
        leftResult.map(_.tid).union(rightResult.map(_.tid)).map(id => new Result(0.toFloat, id))
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
    override def eval(): Seq[Result] = {
      val lfut = Future {
        l.eval()
      }
      val rfut = Future {
        r.eval()
      }

      val f = for {
        leftResult <- lfut
        rightResult <- rfut
      } yield ({
        //possibly return DF with type Result here and sum up distance field
        leftResult.map(_.tid).intersect(rightResult.map(_.tid)).map(id => new Result(0.toFloat, id))
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
    override def eval(): Seq[Result] = {
      val lfut = Future {
        l.eval()
      }
      val rfut = Future {
        r.eval()
      }

      val f = for {
        leftResult <- lfut
        rightResult <- rfut
      } yield ({
        //possibly return DF with type Result here and sum up distance field
        leftResult.map(_.tid).filterNot(rightResult.map(_.tid).toSet).map(id => new Result(0.toFloat, id))
      })

      Await.result(f, Duration(100, TimeUnit.SECONDS))
    }
  }
}
