package org.grapheco.pandadb.neocompat

import org.neo4j.graphdb.{Entity, ResourceIterator}

class EntityIterator[E <:Entity](eIterator: Iterator[Entity]) extends ResourceIterator[E]{

  override def close(): Unit ={
  }

  override def hasNext: Boolean = ???

  override def next(): E = ???
}
