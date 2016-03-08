package ch.unibas.dmi.dbis.adam.rpc

import ch.unibas.dmi.dbis.adam.http.grpc.adam._

import scala.concurrent.Future

/**
  * adamtwo
  *
  * Ivan Giangreco
  * March 2016
  */
class DataDefinitionImpl extends AdamDefinitionGrpc.AdamDefinition {
  override def createEntity(request: EntityNameMessage): Future[AckMessage] = ???
  override def count(request: EntityNameMessage): Future[AckMessage] = ???
  override def insert(request: InsertMessage): Future[AckMessage] = ???
  override def index(request: IndexMessage): Future[AckMessage] = ???
  override def dropEntity(request: EntityNameMessage): Future[AckMessage] = ???
  override def dropIndex(request: IndexNameMessage): Future[AckMessage] = ???
}