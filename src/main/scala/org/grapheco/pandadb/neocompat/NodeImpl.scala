package org.grapheco.pandadb.neocompat

import org.grapheco.lynx.types.LynxValue
import org.neo4j.graphdb.{Direction, Label, Node, Relationship, RelationshipType}
import org.grapheco.lynx.types.structural.{HasProperty, LynxNode, LynxNodeLabel, LynxPropertyKey}
import org.grapheco.pandadb.facade
import org.grapheco.pandadb.facade.PandaTransaction

import scala.collection.JavaConverters._
import java.lang

case class NodeImpl(private val tx: PandaTransaction, private var delegate: LynxNode) extends AbstractEntity(delegate) with Node {

  // todo check if PandaTransaction terminated !!
  override def delete(): Unit = {
    tx.deleteNodesSafely(Seq(delegate.id).iterator, false)
  }

  override def getRelationships: lang.Iterable[Relationship] = {
    tx.getRelationshipsByNode(delegate, Seq.empty, facade.Direction.BOTH).map(lp => RelationshipImpl(tx, lp.storedRelation).asInstanceOf[Relationship]).toIterable.asJava
  }

  override def hasRelationship: Boolean = {
    tx.hasRelationship(delegate, Seq.empty, facade.Direction.BOTH)
  }

  override def getRelationships(types: RelationshipType*): lang.Iterable[Relationship] = {
    tx.getRelationshipsByNode(delegate, types.map(_.name()), facade.Direction.BOTH).map(lp => RelationshipImpl(tx, lp.storedRelation).asInstanceOf[Relationship]).toIterable.asJava
  }

  override def getRelationships(direction: Direction, types: RelationshipType*): lang.Iterable[Relationship] = {
    tx.getRelationshipsByNode(delegate, types.map(_.name()), TypeConverter.toPandaDirection(direction)).map(lp => RelationshipImpl(tx, lp.storedRelation).asInstanceOf[Relationship]).toIterable.asJava
  }

  override def hasRelationship(types: RelationshipType*): Boolean = {
    tx.hasRelationship(delegate, types.map(_.name()), facade.Direction.BOTH)
  }

  override def hasRelationship(direction: Direction, types: RelationshipType*): Boolean = {
    tx.hasRelationship(delegate, types.map(_.name()), TypeConverter.toPandaDirection(direction))
  }

  override def getRelationships(dir: Direction): lang.Iterable[Relationship] = {
    tx.getRelationshipsByNode(delegate, Seq.empty, TypeConverter.toPandaDirection(dir)).map(lp => RelationshipImpl(tx, lp.storedRelation).asInstanceOf[Relationship]).toIterable.asJava
  }

  override def hasRelationship(dir: Direction): Boolean = {
    tx.hasRelationship(delegate, Seq.empty, TypeConverter.toPandaDirection(dir))
  }

  override def getSingleRelationship(`type`: RelationshipType, dir: Direction): Relationship = {
    val lrs = tx.getRelationshipsByNode(delegate, Seq(`type`.name()), TypeConverter.toPandaDirection(dir))
    if(!lrs.hasNext) return null
    val r = RelationshipImpl(tx, lrs.next().storedRelation)
    if (lrs.hasNext) throw new RuntimeException
    r
  }

  override def createRelationshipTo(otherNode: Node, `type`: RelationshipType): Relationship = {
    if (otherNode == null) throw new IllegalArgumentException("Other node missing")
    val rid = tx.addRelationship(None, `type`.name(), delegate.id.toLynxInteger.v, otherNode.getId, Map.empty)
    RelationshipImpl(tx, tx.relationshipAt(rid).get)
  }

  override def getRelationshipTypes: lang.Iterable[RelationshipType] = {
    getRelationships.asScala.map(_.getType).asJava
  }

  override def getDegree: Int = {
    getRelationships.asScala.size
  }

  override def getDegree(`type`: RelationshipType): Int = getRelationships(`type`).asScala.size

  override def getDegree(direction: Direction): Int = getRelationships(direction).asScala.size

  override def getDegree(`type`: RelationshipType, direction: Direction): Int = getRelationships(direction, `type`).asScala.size

  override def addLabel(label: Label): Unit = {
    val newNode = tx.updateNode(delegate.id, Seq(LynxNodeLabel(label.name())), Map.empty).get
    delegate = newNode
    setEntityDelegate(newNode)
  }

  override def removeLabel(label: Label): Unit = {
    val newNode = tx.removeNodesLabels(Seq(delegate.id).iterator, Array(label.name())).next().get
    delegate = newNode
    setEntityDelegate(newNode)
  }

  override def hasLabel(label: Label): Boolean = delegate.labels.contains(LynxNodeLabel(label.name()))

  override def getLabels: lang.Iterable[Label] = delegate.labels.map(l => Label.label(l.value)).asJava

  override def getId: Long = delegate.id.toLynxInteger.v

  override def setProperty(key: String, value: Any): Unit = {
    val newNode = tx.updateNode(delegate.id, Seq.empty, Map(LynxPropertyKey(key) -> LynxValue(value))).get
    delegate = newNode
    setEntityDelegate(newNode)
  } //TODO value type check

  override def removeProperty(key: String): AnyRef = {
    val old = getProperty(key, null)
    val newNode = tx.removeNodesProperties(Seq(delegate.id).iterator, Array(key)).next().get
    delegate = newNode
    setEntityDelegate(newNode)
    old
  }

}
