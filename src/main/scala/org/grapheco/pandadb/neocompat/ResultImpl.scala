package org.grapheco.pandadb.neocompat

import org.grapheco.lynx.{LynxResult, LynxRecord}
import org.grapheco.pandadb.facade.PandaTransaction
import org.neo4j.cypher.internal.javacompat.ResultRowImpl
import org.neo4j.graphdb.{ExecutionPlanDescription, NotFoundException, Notification, QueryExecutionType, QueryStatistics, ResourceIterator, Result}

import java.io.PrintWriter
import scala.collection.JavaConverters._
import java.{lang, util}

case class ResultImpl(private val tx: PandaTransaction, private val delegate: LynxResult) extends Result {

  private val _iterator: Iterator[LynxRecord] = delegate.records()
  private val _columns: util.List[String] = delegate.columns().toList.asJava
  override def getQueryExecutionType: QueryExecutionType = ???

  override def columns(): util.List[String] = _columns

  override def columnAs[T](name: String): ResourceIterator[T] = {
    if (!_columns.contains(name)) throw new NotFoundException()
    new ResourceIterator[T]() {
      override def close(): Unit = {
        ResultImpl.this.close()
      }

      override def hasNext: Boolean = ResultImpl.this.hasNext

      override def next: T = {
        val next: util.Map[String, AnyRef] = ResultImpl.this.next
        next.get(name).asInstanceOf[T]
      }
    }
  }

  override def hasNext: Boolean = _iterator.hasNext

  override def next(): util.Map[String, AnyRef] = {
    _iterator.next().toMap.mapValues(lv => TypeConverter.unwrapLynxValue(lv).asInstanceOf[AnyRef]).asJava
  }

  override def close(): Unit = {}

  override def getQueryStatistics: QueryStatistics = ???

  override def getExecutionPlanDescription: ExecutionPlanDescription = ???

  override def resultAsString(): String = this.delegate.asString()

  override def writeAsStringTo(writer: PrintWriter): Unit = {
    writer.write(resultAsString())
  }

  override def getNotifications: lang.Iterable[Notification] = ???

  override def accept[VisitationException <: Exception](visitor: Result.ResultVisitor[VisitationException]): Unit = {
    while(hasNext) {
      val r = new ResultRowImpl(next)
      visitor.visit(r)
    }
    close()
  }
}
