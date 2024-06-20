package org.grapheco.pandadb.neocompat

import org.grapheco.lynx.types.structural.{HasProperty, LynxPropertyKey}
import org.neo4j.graphdb.NotFoundException

import scala.collection.JavaConverters._
import java.{lang, util}

abstract class AbstractEntity(private var delegate: HasProperty) extends org.neo4j.graphdb.Entity{

  def setEntityDelegate(delegate: HasProperty) = this.delegate = delegate

  override def hasProperty(key: String): Boolean = delegate.keys.contains(LynxPropertyKey(key))

  override def getProperty(key: String): AnyRef = delegate.property(LynxPropertyKey(key)).getOrElse(throw new NotFoundException).value.asInstanceOf[AnyRef] //TODO support org.neo4j.graphdb.spatial.Point returns

  override def getProperty(key: String, defaultValue: Any): AnyRef = delegate.property(LynxPropertyKey(key)).map(_.value).getOrElse(defaultValue).asInstanceOf[AnyRef]

  override def getPropertyKeys: lang.Iterable[String] = delegate.keys.map(_.value).asJava

  override def getProperties(keys: String*): util.Map[String, AnyRef] =  delegate.keys.intersect(keys.map(LynxPropertyKey(_))).map(k => k.value -> delegate.property(k).map(_.value.asInstanceOf[AnyRef]).getOrElse(null)).toMap.asJava

  override def getAllProperties: util.Map[String, AnyRef] = delegate.keys.map(k => k.value -> delegate.property(k).map(_.value.asInstanceOf[AnyRef]).getOrElse(null)).toMap.asJava
}
