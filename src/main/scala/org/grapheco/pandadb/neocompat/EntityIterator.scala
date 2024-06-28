package org.grapheco.pandadb.neocompat

import org.neo4j.graphdb.ResourceIterator

class EntityIterator[Entity](eIterator: Iterator[Entity]) extends ResourceIterator[Entity]{

  override def close(): Unit = {}

  override def hasNext: Boolean = eIterator.hasNext

  override def next(): Entity = eIterator.next()
}
