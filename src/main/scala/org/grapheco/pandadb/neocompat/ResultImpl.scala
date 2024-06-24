package org.grapheco.pandadb.neocompat

import org.grapheco.lynx.LynxResult
import org.grapheco.pandadb.facade.PandaTransaction
import org.neo4j.graphdb.{ExecutionPlanDescription, NotFoundException, Notification, QueryExecutionType, QueryStatistics, ResourceIterator, Result}

import java.io.{PrintWriter}
import scala.collection.JavaConverters._
import java.{lang, util}
case class ResultImpl(private val tx: PandaTransaction, private val delegate: LynxResult) extends Result {

  private var _columns: util.List[String] = null
  override def getQueryExecutionType: QueryExecutionType = ???

  override def columns(): util.List[String] = {
    if (_columns == null) _columns = delegate.columns().toList.asJava
    _columns
  }

  override def columnAs[T](name: String): ResourceIterator[T] = {
    if (!delegate.columns().contains(name)) throw new NotFoundException()
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

  override def hasNext: Boolean = delegate.records().hasNext

  override def next(): util.Map[String, AnyRef] = {
    delegate.records().next().toMap.mapValues(lv => TypeConverter.scalaType2javaType(lv.value).asInstanceOf[AnyRef]).asJava
  }

  override def close(): Unit = {}

  override def getQueryStatistics: QueryStatistics = ???

  override def getExecutionPlanDescription: ExecutionPlanDescription = ???

  override def resultAsString(): String = {
    this.delegate.show()
    delegate.toString//TODO
  }

  override def writeAsStringTo(writer: PrintWriter): Unit = ???

  override def getNotifications: lang.Iterable[Notification] = ???

  override def accept[VisitationException <: Exception](visitor: Result.ResultVisitor[VisitationException]): Unit = ???
}
