package ch.unibas.dmi.dbis.adam.cli

import ch.unibas.dmi.dbis.adam.cli.operations._
import ch.unibas.dmi.dbis.adam.data.types.Feature._
import ch.unibas.dmi.dbis.adam.query.distance.NormBasedDistanceFunction
import org.apache.spark.sql.types._

import scala.tools.nsc.interpreter.ILoop


/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
class CLI extends ILoop {

  override def commands: List[LoopCommand] = super.commands ++ List(
    new VarArgsCmd("create", "tablename", "creates table", createOp),
    new VarArgsCmd("import", "tablename csvPath", "imports data into table", importOp),
    new NullaryCmd("list", "lists tables", listOp),
    new VarArgsCmd("display", "tablename", "displays tuples of table", displayOp),
    new VarArgsCmd("count", "tablename", "counts tuples in table", countOp),
    new VarArgsCmd("seqquery", "tablename q k", "querys table in kNN search sequentially", seqQueryOp),
    new VarArgsCmd("index", "tablename indextype [properties]", "creates an index of given type with properties", indexOp)
  )


  private def createOp(input : List[String]) : Result = {
    val tablename = input(0)

    val schema = StructType(
      List(
        StructField("id", LongType, false),
        StructField("feature", ArrayType(FloatType), false)
      )
    )

    CreateOp(tablename, schema)
    Result.default
  }

  private def importOp(input: List[String]): Result = {
    val tablename = input(0)
    val csvPath = input(1)

    ImportOp(tablename, csvPath)
    Result.default
  }

  private def listOp(input : String) : Result = {
    ListOp()
    Result.default
  }

  private def displayOp(input : List[String]) : Result = {
    val tablename = input(0)
    DisplayOp(tablename)
    Result.default
  }

  private def countOp(input : List[String]) : Result = {
    val tablename = input(0)
    CountOp(tablename)
    Result.default
  }

  private def indexOp(input: List[String]): Result = {
    val tablename = input(0)
    val indextype = input(1)

    var properties = Map[String,String]()

    /*if(input.length == 3){
      val json = input(2)
      //TODO: check! this doesn't work
      properties = read[Map[String, String]](json)
    }*/

    IndexOp(tablename, indextype, properties)

    Result.default
  }

  private def seqQueryOp(input: List[String]): Result = {
    val tablename = input(0)
    val query = input(1)
    val k = input(2).toInt

    SequentialQueryOp(tablename, query, k, NormBasedDistanceFunction(1))

    Result.default
  }
}
