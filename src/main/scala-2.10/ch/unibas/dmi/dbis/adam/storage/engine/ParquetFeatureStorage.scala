package ch.unibas.dmi.dbis.adam.storage.engine

import java.lang

import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.entity.Entity.EntityName
import ch.unibas.dmi.dbis.adam.entity.Tuple.TupleID
import ch.unibas.dmi.dbis.adam.storage.components.FeatureStorage
import org.apache.parquet.column.ColumnReader
import org.apache.parquet.filter.ColumnPredicates.LongPredicateFunction
import org.apache.parquet.filter.{ColumnPredicates, ColumnRecordFilter, RecordFilter, UnboundRecordFilter}
import org.apache.spark.sql.{SaveMode, DataFrame}

import scala.collection.Set

/**
  * adamtwo
  *
  * Ivan Giangreco
  * March 2016
  */
object ParquetFeatureStorage extends FeatureStorage {
  //TODO: implement and use TIDFilter
  override def read(entityname: EntityName, filter: Option[Set[TupleID]]): DataFrame = ???

  override def count(entityname: EntityName): Int = ???

  override def drop(entityname: EntityName): Boolean = ???

  override def write(entityname: EntityName, df: DataFrame, mode: SaveMode): Boolean = ???

  class TIDFilter(filter : Set[TupleID]) extends UnboundRecordFilter {
    override def bind(readers: lang.Iterable[ColumnReader]): RecordFilter = {
      ColumnRecordFilter.column(
        FieldNames.idColumnName,
        ColumnPredicates.applyFunctionToLong(new TIDPredicateFunction(filter))
      ).bind(readers)
    }

    class TIDPredicateFunction(filter : Set[TupleID]) extends LongPredicateFunction {
      override def functionToApply(input: TupleID): Boolean = filter.contains(input)
    }
  }
}
