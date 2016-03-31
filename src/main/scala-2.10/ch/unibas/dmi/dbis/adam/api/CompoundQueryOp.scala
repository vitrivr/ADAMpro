package ch.unibas.dmi.dbis.adam.api

import java.util.concurrent.TimeUnit

import ch.unibas.dmi.dbis.adam.config.FieldNames
import org.apache.spark.sql.DataFrame

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

import scala.concurrent.ExecutionContext.Implicits.global

/**
  * adampro
  *
  * Ivan Giangreco
  * March 2016
  */
object CompoundQueryOp {
  /**
    *
    * @param e
    * @param indexOnly
    * @param withMetadata
    * @return
    */
  def apply(e: Expression, indexOnly: Boolean = false, withMetadata: Boolean = false) : DataFrame = {
    //TODO: implement with metadata and indexOnly
    e.eval()
  }

  /**
    *
    */
  abstract class Expression {
    def eval(): DataFrame = ???
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
        leftResult.except(rightResult)
      })

      Await.result(f, Duration(100, TimeUnit.SECONDS))
    }
  }
}


