package org.grapheco.pandadb.neocompat

import org.neo4j.graphdb.{Entity, ResourceIterable, ResourceIterator}

class EntityIterable[E <:Entity](eIterable: Iterable[Entity]) extends ResourceIterable[E]{

  override def iterator(): ResourceIterator[E] = {
     new EntityIterator[E](eIterable.iterator)
  }
}
