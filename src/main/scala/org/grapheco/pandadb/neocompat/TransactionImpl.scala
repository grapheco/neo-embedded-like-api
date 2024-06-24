package org.grapheco.pandadb.neocompat

import org.grapheco.lynx.runner.{NodeFilter, RelationshipFilter}
import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.LynxString
import org.grapheco.lynx.types.structural.{LynxNodeLabel, LynxPropertyKey, LynxRelationshipType}
import org.grapheco.pandadb.facade.PandaTransaction
import org.neo4j.graphdb.schema.Schema
import org.neo4j.graphdb.{NotFoundException, TransactionTerminatedException}
import org.neo4j.graphdb.traversal.{BidirectionalTraversalDescription, TraversalDescription}
import org.neo4j.graphdb.{Entity, Label, Lock, Node, Relationship, RelationshipType, ResourceIterable, ResourceIterator, Result, StringSearchMode, Transaction}
import org.neo4j.kernel.api.exceptions.Status

import scala.collection.JavaConverters._
import java.{lang, util}

class TransactionImpl(private val delegate: PandaTransaction) extends Transaction {

  private var txState = TxState.STARTED

  private val nodeResources: collection.mutable.ArrayBuffer[EntityIterator[Node]] = collection.mutable.ArrayBuffer()
  private val relResources: collection.mutable.ArrayBuffer[EntityIterator[Relationship]] = collection.mutable.ArrayBuffer()
  private val resultResources: collection.mutable.ArrayBuffer[Result] = collection.mutable.ArrayBuffer()
  private val lockResources: collection.mutable.ArrayBuffer[Lock] = collection.mutable.ArrayBuffer()

  override def createNode(): Node = {
    checkState()
    val props = Map.empty[String, Any]
    val labels = Seq.empty[String]
    val nodeId = this.delegate.addNode(None, labels,  props)
    val pn = this.delegate.nodeAt(nodeId).get
    NodeImpl(delegate, pn)
  }

  override def createNode(labels: Label*): Node = {
    checkState()
    val n = createNode();
    labels.foreach(n.addLabel(_))
    n
  }

  override def getNodeById(id: Long): Node = {
    checkState()
    val pn = this.delegate.nodeAt(id).getOrElse(throw new NotFoundException())
    NodeImpl(delegate, pn)
  }

  override def getRelationshipById(id: Long): Relationship = {
    checkState()
    val pr = this.delegate.relationshipAt(id).getOrElse(throw new NotFoundException())
    RelationshipImpl(delegate, pr)
  }

  override def bidirectionalTraversalDescription(): BidirectionalTraversalDescription = ???

  override def traversalDescription(): TraversalDescription = ???

  override def execute(query: String): Result = {
    checkState()
    val lr = delegate.executeQuery(query)
    val r = ResultImpl(delegate, lr)
    resultResources.append(r)
    r
  }

  override def execute(query: String, parameters: util.Map[String, AnyRef]): Result = {
    checkState()
    val convertedParameters: Map[String, AnyRef] = parameters.asScala.toMap.mapValues(TypeConverter.javaType2scalaType(_).asInstanceOf[AnyRef])
    val lr = delegate.executeQuery(query, convertedParameters)
    val r = ResultImpl(delegate, lr)
    resultResources.append(r)
    r
  }

  override def getAllLabelsInUse: lang.Iterable[Label] = {
    delegate.getAllLabelsInUse.map(ls => Label.label(ls)).asJava
  }
  override def getAllRelationshipTypesInUse: lang.Iterable[RelationshipType] = {
    delegate.getAllRelationshipTypesInUse.map(ts => RelationshipType.withName(ts)).asJava
  }

  override def getAllLabels: lang.Iterable[Label] =
    delegate.getAllLabels.map(ls => Label.label(ls)).asJava

  override def getAllRelationshipTypes: lang.Iterable[RelationshipType] =
    delegate.getAllRelationTypes.map(ts => RelationshipType.withName(ts)).asJava

  override def getAllPropertyKeys: lang.Iterable[String] = {
    delegate.getAllPropertyKeys.asJava
  }

  override def findNodes(label: Label, key: String, template: String, searchMode: StringSearchMode): ResourceIterator[Node] = {
    checkState()
    val nodeIterator = searchMode match {
      case StringSearchMode.EXACT => { //TODO add other modes
        val ll = LynxNodeLabel(label.name())
        val lps = Map(LynxPropertyKey(key) -> LynxString(template))
        val nodeFilter = NodeFilter(Seq(ll), lps)
        delegate.nodes(nodeFilter)
      }
    }
    val ei = new EntityIterator[Node](nodeIterator.map(ln => NodeImpl(delegate, ln)))
    nodeResources.append(ei)
    ei
  }

  override def findNodes(label: Label, propertyValues: util.Map[String, AnyRef]): ResourceIterator[Node] = {
    checkState()
    val ll = LynxNodeLabel(label.name())
    val lps = propertyValues.asScala.toMap.map(lp => LynxPropertyKey(lp._1) -> LynxValue(lp._2))
    val nodeFilter = NodeFilter(Seq(ll), lps)
    nodeFilter2EntityIterator(nodeFilter)
  }

  override def findNodes(label: Label, key1: String, value1: AnyRef, key2: String, value2: AnyRef, key3: String, value3: AnyRef): ResourceIterator[Node] = {
    checkState()
    val ll = LynxNodeLabel(label.name())
    val lps = Map(LynxPropertyKey(key1) -> LynxValue(value1), LynxPropertyKey(key2) -> LynxValue(value2), LynxPropertyKey(key2) -> LynxValue(value2))
    val nodeFilter = NodeFilter(Seq(ll), lps)
    nodeFilter2EntityIterator(nodeFilter)
  }

  override def findNodes(label: Label, key1: String, value1: AnyRef, key2: String, value2: AnyRef): ResourceIterator[Node] = {
    checkState()
    val ll = LynxNodeLabel(label.name())
    val lps = Map(LynxPropertyKey(key1) -> LynxValue(value1), LynxPropertyKey(key2) -> LynxValue(value2))
    val nodeFilter = NodeFilter(Seq(ll), lps)
    nodeFilter2EntityIterator(nodeFilter)
  }

  override def findNode(label: Label, key: String, value: AnyRef): Node = {
    checkState()
    val ll = LynxNodeLabel(label.name())
    val lps = Map(LynxPropertyKey(key) -> LynxValue(value))
    val nodeFilter = NodeFilter(Seq(ll), lps)
    val ei = nodeFilter2EntityIterator(nodeFilter)
    try{
      if (! ei.hasNext) return null
      val ln = ei.next()
      if (ei.hasNext) throw new org.neo4j.graphdb.MultipleFoundException()
      ln
    }
    finally ei.close()
  }

  override def findNodes(label: Label, key: String, value: AnyRef): ResourceIterator[Node] = {
    checkState()
    val ll = LynxNodeLabel(label.name())
    val lps = Map(LynxPropertyKey(key) -> LynxValue(value))
    val nodeFilter = NodeFilter(Seq(ll), lps)
    nodeFilter2EntityIterator(nodeFilter)
  }

  override def findNodes(label: Label): ResourceIterator[Node] = {
    checkState()
    val ll = LynxNodeLabel(label.name())
    val lps = Map[LynxPropertyKey, LynxValue]()
    val nodeFilter = NodeFilter(Seq(ll), lps)
    nodeFilter2EntityIterator(nodeFilter)
  }

  override def findRelationships(relationshipType: RelationshipType, key: String, template: String, searchMode: StringSearchMode): ResourceIterator[Relationship] = {
    checkState()
    val pathIterator = searchMode match {
      case StringSearchMode.EXACT => { //TODO add other modes
        val lt = LynxRelationshipType(relationshipType.name())
        val lps = Map(LynxPropertyKey(key) -> LynxString(template))
        val relFilter = RelationshipFilter(Seq(lt), lps)
        delegate.relationships(relFilter)
      }
    }
    val ei = new EntityIterator[Relationship](pathIterator.map(p => RelationshipImpl(delegate, p.storedRelation)))
    relResources.append(ei)
    ei
  }

  override def findRelationships(relationshipType: RelationshipType, propertyValues: util.Map[String, AnyRef]): ResourceIterator[Relationship] = {
    checkState()
    val lt = LynxRelationshipType(relationshipType.name())
    val lps = propertyValues.asScala.toMap.map(lp => LynxPropertyKey(lp._1) -> LynxValue(lp._2))
    val relFilter = RelationshipFilter(Seq(lt), lps)
    relFilter2EntityIterator(relFilter)
  }

  override def findRelationships(relationshipType: RelationshipType, key1: String, value1: AnyRef, key2: String, value2: AnyRef, key3: String, value3: AnyRef): ResourceIterator[Relationship] = {
    checkState()
    val lt = LynxRelationshipType(relationshipType.name())
    val lps = Map(LynxPropertyKey(key1) -> LynxValue(value1), LynxPropertyKey(key2) -> LynxValue(value2), LynxPropertyKey(key2) -> LynxValue(value2))
    val relFilter = RelationshipFilter(Seq(lt), lps)
    relFilter2EntityIterator(relFilter)
  }

  override def findRelationships(relationshipType: RelationshipType, key1: String, value1: AnyRef, key2: String, value2: AnyRef): ResourceIterator[Relationship] = {
    checkState()
    val lt = LynxRelationshipType(relationshipType.name())
    val lps = Map(LynxPropertyKey(key1) -> LynxValue(value1), LynxPropertyKey(key2) -> LynxValue(value2))
    val relFilter = RelationshipFilter(Seq(lt), lps)
    relFilter2EntityIterator(relFilter)
  }

  override def findRelationship(relationshipType: RelationshipType, key: String, value: AnyRef): Relationship = {
    checkState()
    val lt = LynxRelationshipType(relationshipType.name())
    val lps = Map(LynxPropertyKey(key) -> LynxValue(value))
    val relFilter = RelationshipFilter(Seq(lt), lps)
    val ri = relFilter2EntityIterator(relFilter)
    try {
      if (!ri.hasNext) return null
      val path = ri.next()
      if (ri.hasNext) throw new org.neo4j.graphdb.MultipleFoundException()
      path
    }
    finally ri.close()
  }

  override def findRelationships(relationshipType: RelationshipType, key: String, value: AnyRef): ResourceIterator[Relationship] = {
    checkState()
    val lt = LynxRelationshipType(relationshipType.name())
    val lps = Map(LynxPropertyKey(key) -> LynxValue(value))
    val relFilter = RelationshipFilter(Seq(lt), lps)
    relFilter2EntityIterator(relFilter)
  }

  override def findRelationships(relationshipType: RelationshipType): ResourceIterator[Relationship] = {
    checkState()
    val lt = LynxRelationshipType(relationshipType.name())
    val lps = Map[LynxPropertyKey, LynxValue]()
    val relFilter = RelationshipFilter(Seq(lt), lps)
    relFilter2EntityIterator(relFilter)
  }

  override def getAllNodes: ResourceIterable[Node] = {
    checkState()
    val ni = delegate.allNodes().toIterable
    new EntityIterable[Node](ni.map(ln => NodeImpl(delegate, ln)))
  }

  override def getAllRelationships: ResourceIterable[Relationship] = {
    checkState()
    val pi = delegate.allRelationships().toIterable
    new EntityIterable[Relationship](pi.map(lp => new RelationshipImpl(delegate, lp.storedRelation)))
  }

  override def acquireWriteLock(entity: Entity): Lock = ???

  override def acquireReadLock(entity: Entity): Lock = ???

  override def schema(): Schema = ???

  override def terminate(): Unit = {
    txState = TxState.Terminated
    releasResource()
    delegate.terminate()
  }

  override def commit(): Unit = {
    if (txState == TxState.Terminated) return rollback()
    txState = TxState.Committed
    releasResource()
    delegate.commit()
  }

  override def rollback(): Unit = {
    txState = TxState.Rollbacked
    releasResource()
    delegate.rollback()
  }

  override def close(): Unit = {
    if (txState != TxState.Committed) rollback()
    txState = TxState.Closed
    //TODO do we need mannual call rollback???
    releasResource()
  }

  private def checkState(): Unit = {
    if(txState==TxState.Terminated) throw new TransactionTerminatedException(Status.Transaction.Terminated)
    if(txState==TxState.Committed) throw new TransactionTerminatedException(Status.Transaction.Terminated)//TODO use proper exception!!
    if(txState==TxState.Closed) throw new TransactionTerminatedException(Status.Transaction.Terminated)//TODO use proper exception!!
  }

  private def nodeFilter2EntityIterator(nodeFilter: NodeFilter): EntityIterator[Node] = {
    val nodeIterator = delegate.nodes(nodeFilter)
    val ei = new EntityIterator[Node](nodeIterator.map(ln => NodeImpl(delegate, ln)))
    nodeResources.append(ei)
    ei
  }

  private def relFilter2EntityIterator(relFilter: RelationshipFilter): EntityIterator[Relationship] = {
    val relIterator = delegate.relationships(relFilter)
    val ei = new EntityIterator[Relationship](relIterator.map(p => new RelationshipImpl(delegate, p.storedRelation)))
    relResources.append(ei)
    ei
  }

  private def releasResource(): Unit = {
    nodeResources.map(_.close())
    relResources.map(_.close())
    resultResources.map(_.close())
    lockResources.map(_.close())
  }
}

object TxState extends Enumeration {
  val STARTED, Terminated, Rollbacked, Committed, Closed = Value
}