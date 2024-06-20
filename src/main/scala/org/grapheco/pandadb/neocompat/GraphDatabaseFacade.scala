package org.grapheco.pandadb.neocompat

import org.grapheco.pandadb.facade
import org.neo4j.graphdb.{GraphDatabaseService, QueryExecutionException, Result, ResultTransformer, Transaction}
import org.neo4j.kernel.api.exceptions.Status.Statement

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import java.time.Duration
import java.util
import java.util.concurrent.TimeUnit
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, Future}

case class GraphDatabaseFacade(private val dbName: String, private val delegate: facade.GraphDatabaseService) extends GraphDatabaseService{

  override def isAvailable(timeout: Long): Boolean = true

  override def beginTx(): Transaction = new TransactionImpl(this.delegate.beginTransaction)

  override def beginTx(timeout: Long, unit: TimeUnit): Transaction = ???

  override def executeTransactionally(query: String): Unit = {
    try this.delegate.executeQuery(query)
    catch {
      case e: Exception =>
        throw new QueryExecutionException(e.getMessage, e, Statement.ExecutionFailed.name)
    }
  }

  override def executeTransactionally(query: String, parameters: util.Map[String, AnyRef]): Unit = {
    try this.delegate.executeQuery(query, parameters.asScala.toMap)
    catch {
      case e: Exception =>
        throw new QueryExecutionException(e.getMessage, e, Statement.ExecutionFailed.name)
    }
  }

  override def executeTransactionally[T](query: String, parameters: util.Map[String, AnyRef], resultTransformer: ResultTransformer[T]): T = {
    var tx: Transaction = null
    var result: Result = null
    try {
      tx = beginTx()
      result = tx.execute(query, parameters)
      return resultTransformer.apply(result)
    } catch {
      case e: Exception =>
        throw new QueryExecutionException(e.getMessage, e, Statement.ExecutionFailed.name)
    }
    finally {
      result.close()
      tx.close()
    }
  }

  override def executeTransactionally[T](query: String, parameters: util.Map[String, AnyRef], resultTransformer: ResultTransformer[T], timeout: Duration): T = {
    val future = Future {executeTransactionally(query, parameters, resultTransformer)}
    try {
      Await.result(future, FiniteDuration(timeout.toNanos, TimeUnit.NANOSECONDS))
    } catch {
      case e: Exception => throw new RuntimeException("Operation timed out", e)
    }
  }

  override def databaseName(): String = dbName
}
