package org.grapheco.pandadb.neocompat

import org.grapheco.pandadb.facade.Direction.Direction
import org.grapheco.pandadb.facade.Direction

object TypeConverter {

  private def toNeoDirection(dir: Direction): org.neo4j.graphdb.Direction = {
    dir match {
      case Direction.OUTGOING => org.neo4j.graphdb.Direction.OUTGOING
      case Direction.BOTH => org.neo4j.graphdb.Direction.BOTH
      case Direction.INCOMING => org.neo4j.graphdb.Direction.INCOMING
    }
  }

  def toPandaDirection(dir: org.neo4j.graphdb.Direction): Direction = {
    dir match {
      case org.neo4j.graphdb.Direction.OUTGOING => Direction.OUTGOING
      case org.neo4j.graphdb.Direction.BOTH => Direction.BOTH
      case org.neo4j.graphdb.Direction.INCOMING => Direction.INCOMING
    }
  }

}

