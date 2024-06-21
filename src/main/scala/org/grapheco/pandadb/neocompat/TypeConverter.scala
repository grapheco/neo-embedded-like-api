package org.grapheco.pandadb.neocompat

import org.grapheco.lynx.types.spatial.Geographic2D
import org.grapheco.pandadb.facade.Direction.Direction
import org.grapheco.pandadb.facade.Direction
import org.neo4j.values.storable.Values

import scala.collection.JavaConverters._

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

  def type2neoType(origin: AnyRef): AnyRef = {
    origin match {
      case l: List[AnyRef] => l.map(type2neoType(_)).asJava
      case m: Map[String, AnyRef] => m.mapValues(type2neoType(_)).asJava
      case p: Geographic2D => Values.pointValue(org.neo4j.values.storable.CoordinateReferenceSystem.WGS84, p.x.value, p.y.value)
      case _ => origin
    }
  }

}

