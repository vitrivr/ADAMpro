package ch.unibas.dmi.dbis.adam.cli

import ch.unibas.dmi.dbis.adam.api._
import ch.unibas.dmi.dbis.adam.datatypes.Feature._
import ch.unibas.dmi.dbis.adam.main.SparkStartup
import ch.unibas.dmi.dbis.adam.query.distance.NormBasedDistanceFunction
import ch.unibas.dmi.dbis.adam.storage.catalog.CatalogOperator
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
    new VarArgsCmd("dbimport", "url port database user password tablename columns", "imports data into table from database", dbImportOp),
    new NullaryCmd("list", "lists tables", listOp),
    new VarArgsCmd("display", "tablename", "displays tuples of table", displayOp),
    new VarArgsCmd("count", "tablename", "counts tuples in table", countOp),
    new VarArgsCmd("index", "tablename indextype [properties]", "creates an index of given type with properties", indexOp),
    new VarArgsCmd("cache", "tablename", "caches all indexes of the given table and the table", cacheOp),
    new NullaryCmd("cacheAllIndexes", "caches all indexes of the given table", cacheAllIndexesOp),
    new VarArgsCmd("seqquery", "tablename q k", "querys table in kNN search sequentially", seqQueryOp),
    new VarArgsCmd("indquery", "indexname q k", "querys table in kNN search using index", indQueryOp),
    new VarArgsCmd("progQuery", "tablename q k", "querys table in kNN search using progressive query", progQueryOp),
    new VarArgsCmd("drop", "tablename", "drops table", dropOp),
    new NullaryCmd("evaluation","evaluation", evaluationOp),

    new NullaryCmd("dropAllIndexes","drops all indexes", dropAllIndexesOp),
    new NullaryCmd("tmpOp","temporary operation only for testing purposes", tmpOp)
  )


  /**
   *
   * @param input
   * @return
   */
  private def countOp(input : List[String]) : Result = {
    val tablename = input(0)
    val count = CountOp(tablename)
    Result.resultFromString(s"COUNT for $tablename: $count")
  }


  /**
   *
   * @param input
   * @return
   */
  private def displayOp(input : List[String]) : Result = {
    val tablename = input(0)
    val results = DisplayOp(tablename)

    Result.resultFromString(
      results.map { result => result._1 + "\t" + result._2.mkString("<", ",", ">")}
        .mkString("\n")
    )
  }


  /**
   *
   * @param input
   * @return
   */
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


  /**
   *
   * @param input
   * @return
   */
  private def dropOp(input : List[String]) : Result = {
    val tablename = input(0)

    DropOp(tablename)
    Result.default
  }

  /**
   *
   * @param input
   * @return
   */
  private def importOp(input: List[String]): Result = {
    val tablename = input(0)
    val csvPath = input(1)

    val csv = SparkStartup.sc.textFile(csvPath.toString)

    ImportOp(tablename, csv.collect())
    Result.default
  }

  /**
   *
   * @param input
   * @return
   */
  private def dbImportOp(input: List[String]): Result = {
    val url = input(0)
    val port = input(1).toInt
    val database = input(2)
    val user = input(3)
    val password = input(4)
    val tablename = input(5)
    val columns = input(6)

    DBImportOp(url, port, database, user, password, tablename, columns)
    Result.default
  }

  /**
   *
   * @param input
   * @return
   */
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

  /**
   *
   * @param input
   * @return
   */
  private def cacheOp(input : List[String]) : Result = {
    val tablename = input(0)

    CacheOp(tablename)
  }

  /**
   *
   * @param input
   * @return
   */
  private def cacheAllIndexesOp(input : String) : Result = {
    CacheAllIndexesOp()
  }

  /**
   *
   * @param input
   * @return
   */
  private def listOp(input : String) : Result = {
    val results = ListOp()

    Result.resultFromString(results.mkString(", \n"))
  }

  /**
   *
   * @param input
   * @return
   */
  private def seqQueryOp(input: List[String]): Result = {
    val tablename = input(0)
    val query = input(1)
    val k = input(2).toInt

    //implicit conversion!
    val results = SeqQueryOp(tablename, query, k, NormBasedDistanceFunction(1))
    Result.resultFromString(results.map(x => "(" + x.tid + "," + x.distance + ")").mkString("\n "))
  }

  /**
   *
   * @param input
   * @return
   */
  private def indQueryOp(input: List[String]): Result = {
    val indexname = input(0)
    val query = input(1)
    val k = input(2).toInt

    //TODO: change so user doesn't have to give indexname but rather the tablename and we
    //choose the index based on a score

    //implicit conversion!
    val results =  IndexQueryOp(indexname, query, k, NormBasedDistanceFunction(1))
    Result.resultFromString(results.map(x => "(" + x.tid + "," + x.distance + ")").mkString("\n "))
  }


  /**
   *
   * @param input
   * @return
   */
  private def progQueryOp(input: List[String]): Result = {
    val tablename = input(0)
    val query = input(1)
    val k = input(2).toInt

    ProgQueryOp(tablename, query, k, NormBasedDistanceFunction(1), (status, results, details) => println(results.mkString(", ")))

    Result.default
  }

  /**
   *
   * @param input
   * @return
   */
  private def evaluationOp(input : String) : Result = {
    EvaluationOp()
    Result.default
  }

  /**
   *
   * @param input
   * @return
   */
  private def dropAllIndexesOp(input : String) : Result = {
    CatalogOperator.dropAllIndexes()
  }


  /**
   *
   * @param input
   * @return
   */
  private def tmpOp(input : String) : Result = {
    val files = new java.io.File("./data/old").listFiles.filter(_.getName.startsWith("data_")).sortBy(_.getName)


    val schema = StructType(
      List(
        StructField("id", LongType, false),
        StructField("feature", ArrayType(FloatType), false)
      )
    )

    files.foreach{
      file =>
        val data = SparkStartup.sqlContext.read.parquet(file.getAbsolutePath)

        val revData = data.map(r =>  Row(r.getInt(0).toLong, r.getSeq[Float](1)))
        val df = SparkStartup.sqlContext.createDataFrame(revData, schema)

        DropOp(file.getName, true)
        CreateOp(file.getName, schema)
        ImportOp(file.getName, df)
    }

    Result.default
  }

}
