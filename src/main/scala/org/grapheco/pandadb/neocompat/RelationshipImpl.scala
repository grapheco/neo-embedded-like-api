package org.grapheco.pandadb.neocompat

import org.grapheco.lynx.types.LynxValue
import org.neo4j.graphdb.{Node, Relationship, RelationshipType}
import org.grapheco.lynx.types.structural.{LynxPropertyKey, LynxRelationship}
import org.grapheco.pandadb.facade.PandaTransaction

import scala.collection.JavaConverters._
import java.{lang, util}

case class RelationshipImpl(private val tx: PandaTransaction, private var delegate: LynxRelationship) extends AbstractEntity(delegate) with Relationship {

  // todo check if PandaTransaction terminated !!
  override def delete(): Unit =
    tx.deleteRelations(Seq(delegate.id).iterator)

  override def getStartNode: Node = {
    val ln = tx.nodeAt(delegate.startNodeId.toLynxInteger.v).get
    NodeImpl(tx, ln)
  }

  override def getEndNode: Node = {
    val ln = tx.nodeAt(delegate.endNodeId.toLynxInteger.v).get
    NodeImpl(tx, ln)
  }

  override def getOtherNode(node: Node): Node = {
    val sid: java.lang.Long = delegate.endNodeId.toLynxInteger.v
    if (sid.equals(node.getId)) {
      val ln = tx.nodeAt(sid).get
      return NodeImpl(tx, ln)
    }
    getEndNode()
  }

  override def getNodes: Array[Node] = Array(getStartNode, getEndNode)

  override def getType: RelationshipType = {
    val tName = delegate.relationType.getOrElse(throw new RuntimeException("neo4j Relationship must have type, but panda not supply")).value
    RelationshipType.withName(tName)
  }

  override def isType(`type`: RelationshipType): Boolean = delegate.relationType.map(_.value.equals(`type`.name())).getOrElse(false)

  override def getId: Long = delegate.id.toLynxInteger.v-1 //PandaRel id starts from 1, but neo starts from 0.

  override def setProperty(key: String, value: Any): Unit = {
    val newRel = tx.updateRelationShip(delegate.id, Map(LynxPropertyKey(key)->LynxValue(value))).get
    delegate = newRel
    setEntityDelegate(newRel)
  }

  override def removeProperty(key: String): AnyRef = {
    val old = getProperty(key, null)
    val newRel = tx.removeRelationshipsProperties(Seq(delegate.id).iterator, Array(key)).next().get
    delegate = newRel
    setEntityDelegate(newRel)
    old
  }
}
