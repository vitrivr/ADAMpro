package ch.unibas.dmi.dbis.adam.cli

import ch.unibas.dmi.dbis.adam.api._
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature._
import ch.unibas.dmi.dbis.adam.query.distance.{DistanceFunction, ManhattanDistance, MinkowskiDistance}
import ch.unibas.dmi.dbis.adam.query.handler.QueryHints
import ch.unibas.dmi.dbis.adam.query.query.{BooleanQuery, NearestNeighbourQuery}

import scala.concurrent.duration.Duration
import scala.tools.nsc.interpreter.ILoop


/**
 * adamtwo
 *
 * Ivan Giangreco
 * August 2015
 */
class CLI extends ILoop {

  override def commands: List[LoopCommand] = super.commands ++ List(
    new VarArgsCmd("count", "tablename", "counts tuples in table", countOp),
    new VarArgsCmd("create", "tablename", "creates table", createOp),
    new VarArgsCmd("dbimport", "url port database user password tablename columns", "imports data into table from database", dbImportOp),
    new VarArgsCmd("drop", "tablename", "drops table", dropOp),
    new NullaryCmd("evaluation", "evaluation", evaluationOp),
    new VarArgsCmd("index", "tablename indextype distancenorm [properties]", "creates an index of given type with properties", indexOp),
    new NullaryCmd("list", "lists tables", listOp),
    new VarArgsCmd("query", "tablename q k", "querys table in kNN search", queryOp),
    new VarArgsCmd("seqQuery", "tablename q k", "querys table in kNN search sequentially", seqQueryOp),
    new VarArgsCmd("indQuery", "indexname q k", "querys table in kNN search using index", indQueryOp),
    new VarArgsCmd("progQuery", "tablename q k", "querys table in kNN search using progressive query", progQueryOp),
    new VarArgsCmd("timedProgQuery", "tablename q k t", "querys table in kNN search using progressive query, t in ms", timedProgQueryOp),
    new NullaryCmd("tmpOp", "temporary operation only for testing purposes", tmpOp)
  )

  /**
   *
   * @param input
   * @return
   */
  private def countOp(input: List[String]): Result = {
    val tablename = input(0)
    val count = CountOp(tablename)
    Result.resultFromString(s"COUNT for $tablename: $count")
  }

  /**
   *
   * @param input
   * @return
   */
  private def createOp(input: List[String]): Result = {
    val tablename = input(0)

    CreateOp(tablename)
    Result.default
  }

  /**
   *
   * @param input
   * @return
   */
  private def dropOp(input: List[String]): Result = {
    val tablename = input(0)

    DropOp(tablename)
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
    val norm = input(2).toInt

    var properties = Map[String, String]()
    IndexOp(tablename, indextype, new MinkowskiDistance(norm), properties)
    Result.default
  }

  /**
   *
   * @param input
   * @return
   */
  private def listOp(input: String): Result = {
    val results = ListOp()

    Result.resultFromString(results.mkString(", \n"))
  }

  /**
   *
   * @param input
   * @return
   */
  private def queryOp(input: List[String]): Result = {
    val entityname = input(0)

    def nextOption(map: Map[Symbol, Any], input: List[String]): Map[Symbol, Any] = {
      def isSwitch(s: String) = (s.charAt(0) == '-')

      input match {
        case Nil => map
        case "--hint" :: value :: tail =>
          nextOption(map ++ Map('hint -> value), tail)
        case "--q" :: value :: tail =>
          nextOption(map ++ Map('q -> value), tail)
        case "--distance" :: value :: tail =>
          nextOption(map ++ Map('norm -> new MinkowskiDistance(value.toInt)), tail)
        case "--k" :: value :: tail =>
          nextOption(map ++ Map('k -> value.toInt), tail)
        case "--indexOnly" :: value :: tail =>
          nextOption(map ++ Map('indexOnly -> value.toBoolean), tail)
        case "--indexOptions" :: value :: tail =>
          nextOption(map ++ Map('indexOptions -> value.substring(1, value.length - 1)
            .split(",")
            .map(_.split(":"))
            .map { case Array(k, v) => (k.substring(1, k.length - 1), v.substring(1, v.length - 1)) }
            .toMap), tail)
        case "--where" :: value :: tail =>
          nextOption(map ++ Map('where -> value.substring(1, value.length - 1)
            .split(",")
            .map(_.split(":"))
            .map { case Array(k, v) => (k.substring(1, k.length - 1), v.substring(1, v.length - 1)) }
            .toMap), tail)
        case option :: tail => println("Unknown option " + option)
          Map[Symbol, Any]()
      }
    }


    val options = nextOption(Map(), input)

    val hint = QueryHints.withName(options.getOrElse('hint, null).asInstanceOf[String])

    val nnq = NearestNeighbourQuery(
      options('q).asInstanceOf[FeatureVector],
      options.getOrElse('distance, ManhattanDistance).asInstanceOf[DistanceFunction],
      options.getOrElse('k, 100).asInstanceOf[Int],
      options.getOrElse('indexOnly, false).asInstanceOf[Boolean],
      options.getOrElse('indexOptions, Map[String, String]()).asInstanceOf[Map[String, String]])

    val bq = if(options.contains('where)){
      Option(BooleanQuery(options('where).asInstanceOf[Map[String, String]], null))
    } else {
      None
    }


    val results = QueryOp(entityname, hint, nnq, bq, true)
    Result.resultFromString(results.map(x => "(" + x.tid + "," + x.distance + ")").mkString("\n "))
  }


  /**
   *
   * @param input
   * @return
   */
  private def seqQueryOp(input: List[String]): Result = {
    val entityname = input(0)
    val query = input(1)
    val k = input(2).toInt
    val norm = 1
    val withMetadata = true

    val nnq = NearestNeighbourQuery(query, new MinkowskiDistance(norm), k)
    val bq = None

    val results = QueryOp.sequential(entityname, nnq, bq, withMetadata)
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
    val norm = 1
    val withMetadata = true

    val nnq = NearestNeighbourQuery(query, new MinkowskiDistance(norm), k)
    val bq = None

    val results = QueryOp.index(indexname, nnq, bq, withMetadata)
    Result.resultFromString(results.map(x => "(" + x.tid + "," + x.distance + ")").mkString("\n "))
  }


  /**
   *
   * @param input
   * @return
   */
  private def progQueryOp(input: List[String]): Result = {
    val entityname = input(0)
    val query = input(1)
    val k = input(2).toInt
    val norm = 1
    val withMetadata = true

    val nnq = NearestNeighbourQuery(query, new MinkowskiDistance(norm), k)
    val bq = None

    QueryOp.progressive(entityname, nnq, bq, (status, results, confidence, details) => println(results.mkString(", ")), withMetadata)

    Result.default
  }

  /**
   *
   * @param input
   * @return
   */
  private def timedProgQueryOp(input: List[String]): Result = {
    val tablename = input(0)
    val query = input(1)
    val k = input(2).toInt
    val time = Duration(input(3).toLong, "millis")
    val withMetadata = true

    val nnq = NearestNeighbourQuery(query, ManhattanDistance, k)
    val bq = None

    val (results, confidence) = QueryOp.timedProgressive(tablename, nnq, bq, time, withMetadata)

    Result.resultFromString(results.map(x => "(" + x.tid + "," + x.distance + ")").mkString("\n "))

    Result.default
  }

  /**
   *
   * @param input
   * @return
   */
  private def evaluationOp(input: String): Result = {
    EvaluationOp()
    Result.default
  }

  /**
   *
   * @param input
   * @return
   */
  private def tmpOp(input: String): Result = {
    Result.default
  }



}
