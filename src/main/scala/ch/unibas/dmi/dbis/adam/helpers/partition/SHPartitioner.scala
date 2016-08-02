package ch.unibas.dmi.dbis.adam.helpers.partition

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.util.Base64

import ch.unibas.dmi.dbis.adam.catalog.CatalogOperator
import ch.unibas.dmi.dbis.adam.config.FieldNames
import ch.unibas.dmi.dbis.adam.datatypes.bitString.BitString
import ch.unibas.dmi.dbis.adam.datatypes.feature.Feature.FeatureVector
import ch.unibas.dmi.dbis.adam.entity.{Entity, EntityNameHolder}
import ch.unibas.dmi.dbis.adam.exception.GeneralAdamException
import ch.unibas.dmi.dbis.adam.index.Index
import ch.unibas.dmi.dbis.adam.index.structures.IndexTypes
import ch.unibas.dmi.dbis.adam.index.structures.sh.{SHIndexMetaData, SHUtils}
import ch.unibas.dmi.dbis.adam.main.AdamContext
import ch.unibas.dmi.dbis.adam.utils.Logging
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

import scala.util.Random

/**
  * Created by silvanheller on 26.07.16.
  */
class SHPartitioner(nPart: Int, noBits: Int) extends Partitioner with Logging {
  override def numPartitions: Int = nPart

  log.info("Number of Partitions: " + nPart)
  log.info("Number of Bits: " + noBits)

  //Two Inital clusters are 1...1 and 0...0 - TODO Is this fair?
  val upperBound = BitString(Seq.tabulate(noBits)(el => el))
  val lowerBound = BitString(Seq())
  var clusters = IndexedSeq(upperBound, lowerBound)

  val noSamples = 200

  //TODO Switch sampling here to real data
  //TODO Is it useful to incorporate assumptions of the distribution of data here?
  //TODO Maybe switch to K-Means for to generate a set of initial guesses -> See PQ-Indexer
  //TODO Maybe use PQ-Idea / eCP for Clustering
  var counter = 0

  while(counter< nPart - 2){
    val samples = Seq.fill(noSamples)(generateRandomBitString(noBits))

    //We take last here since we want the point with the biggest distance to existing cluster centers
    val best = samples.sortBy(el => getMinDistance(el)).last
    //log.debug("New cluster was chosen: "+best+" with distance: "+getMinDistance(best))
    clusters = clusters.+:(best)
    //log.debug("New Cluster list: "+clusters.mkString(" :: "))
    counter+=1
  }

  /** Generates a BitString with Random Bits set */
  def generateRandomBitString(len: Int) : BitString[_] = {
    val idxs = Seq.tabulate(len)(idx => idx)

    val res = idxs.map(idx => {
      if(Random.nextBoolean()){
        idx
      } else -1
    }).filter(el => el>0)
    BitString(res)
  }

  /** Gets the minimum distance of a bitString compared to all current clusters */
  def getMinDistance(c: BitString[_]) : Int = clusters.map(_.intersectionCount(c)).sortBy(el => el).head

  /**
    * We expect the key here to a bitstring.
    * Careful: BitString is stored as an  array of Indices where the bit is set to true.
    */
  override def getPartition(key: Any): Int = {
    val bitString = key.asInstanceOf[BitString[_]]
    val cluster = clusters.zipWithIndex.map(f => (f._1.intersectionCount(bitString), f._2)).sortBy(_._1).head
    cluster._2
  }

  def getClusters : IndexedSeq[BitString[_]] = clusters
}

object SHPartitioner extends ADAMPartitioner with Logging {
  /** Use this name to store Partitioner Clusters in the CatalogOperator*/
  def clusterOptionName = "partitionClusters"
  override def partitionerName = PartitionerChoice.SH

  /**
    * Loads the Information for the Partitioner and Index corresponding to the given entity and then returns the BitString
    * @param q Queryvector
    * @param eName Entity on which the SHIndex exists
    * @return BitString hashed by the SHUtils
    */
  def getBitString(q: FeatureVector, eName : EntityNameHolder)(implicit ac: AdamContext): BitString[_] = {
    val index = Entity.load(eName).get.indexes.find(f => f.get.indextypename == IndexTypes.SHINDEX).get.get
    val metaData = CatalogOperator.getIndexMeta(index.indexname).get.asInstanceOf[SHIndexMetaData]
    SHUtils.hashFeature(q, metaData)
  }

  override def apply(data: DataFrame, cols: Option[Seq[String]], indexName: Option[EntityNameHolder], nPartitions: Int)(implicit ac: AdamContext): DataFrame = {
    val indextype = IndexTypes.SHINDEX
    if(indexName.isEmpty){
      throw new GeneralAdamException("Indexname was not specified")
    }
    try {
      //This line causes you to load the data from the first index that is found which matches the type
      val index = Entity.load(Index.load(indexName.get).get.entityname).get.indexes.find(f => f.get.indextypename == indextype).get.get
      val noBits = CatalogOperator.getIndexMeta(index.indexname).get.asInstanceOf[SHIndexMetaData].noBits

      val joinDF = index.getData.withColumnRenamed(FieldNames.featureIndexColumnName, FieldNames.partitionKey)
      val joinedDF = data.join(joinDF, FieldNames.pk)

      val partitioner =  new SHPartitioner(nPartitions, noBits)

      CatalogOperator.updateIndexOption(indexName.get,clusterOptionName,clusterToString(partitioner.getClusters))

      val repartitioned: RDD[(Any, Row)] = joinedDF.map(r => (r.getAs[Any](FieldNames.partitionKey), r)).partitionBy(partitioner)
      val reparRDD = repartitioned.mapPartitions((it) => {
        it.map(f => f._2)
      }, true)

      ac.sqlContext.createDataFrame(reparRDD, joinedDF.schema)
    } catch {
      case e: java.util.NoSuchElementException => {
        log.error("Repartitioning with this mode is not possible because the index: " + indextype.name + " does not exist", e)
        throw new GeneralAdamException("Index: " + indextype.name + " does not exist, aborting repartitioning")
      }
    }
  }

  /**
    * Returns a List of the cluster-centers or centroids or whatever bitStrings of interest for the SHPartitioner which was created for the index-structure
    * Currently no error checks so we just assume the  partitioner exists
    */
  def getClusterList(eName: EntityNameHolder)(implicit ac: AdamContext) : IndexedSeq[BitString[_]] = {
    val index = Entity.load(eName).get.indexes.find(f => f.get.indextypename == IndexTypes.SHINDEX).get.get
    clusterFromString(CatalogOperator.getIndexOption(index.indexname).get.get(clusterOptionName).get)
  }



  /** http://stackoverflow.com/questions/134492/how-to-serialize-an-object-into-a-string */
  private def clusterFromString(s: String) : IndexedSeq[BitString[_]] = {
    val data = Base64.getDecoder().decode( s )
    val ois = new ObjectInputStream(
      new ByteArrayInputStream(  data ) )
    val o  = ois.readObject()
    ois.close()
    o.asInstanceOf[IndexedSeq[BitString[_]]]
  }


  /** http://stackoverflow.com/questions/134492/how-to-serialize-an-object-into-a-string */
  private def clusterToString(clusterList : IndexedSeq[BitString[_]]) : String = {
    val baos = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream( baos )
    oos.writeObject( clusterList )
    oos.close()
    baos.close()
    Base64.getEncoder().encodeToString(baos.toByteArray())
  }
}