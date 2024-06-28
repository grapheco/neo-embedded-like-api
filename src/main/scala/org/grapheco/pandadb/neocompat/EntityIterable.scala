package org.grapheco.pandadb.neocompat

import org.neo4j.graphdb.{ResourceIterable, ResourceIterator}

class EntityIterable[Entity](eIterable: Iterable[Entity]) extends ResourceIterable[Entity]{

  override def iterator(): ResourceIterator[Entity] = {
     new EntityIterator[Entity](eIterable.iterator)
  }
}
