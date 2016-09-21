package ch.unibas.dmi.dbis.adam.evaluation.main

import ch.unibas.dmi.dbis.adam.evaluation.utils.VecImporter
import io.grpc.ManagedChannelBuilder
import io.grpc.okhttp.OkHttpChannelBuilder
import org.vitrivr.adam.grpc.grpc.AdamDefinitionGrpc

/**
  * Created by silvan on 04.09.16.
  */
object ImportStartup {

  var grpcHost = "10.34.58.136"
  //grpcHost = "localhost"
  val grpcPort = 5890

  def main(args: Array[String]): Unit = {
    System.out.println("Starting Import on " + grpcHost + ":" + grpcPort)

    val channel = OkHttpChannelBuilder.forAddress(grpcHost, grpcPort).usePlaintext(true).asInstanceOf[ManagedChannelBuilder[_]].build()

    val stub = AdamDefinitionGrpc.stub(channel)

    VecImporter.genEntity(stub)
    VecImporter.importVec("evaluation/src/main/resources/sift_base.fvecs", stub)
    System.exit(1)
  }
}
