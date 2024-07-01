package org.grapheco.pandadb.neocompat

import org.grapheco.lynx.types.property.LynxFloat
import org.grapheco.lynx.types.spatial.{Cartesian2D, Geographic2D}
import org.grapheco.pandadb.facade.Direction.Direction
import org.grapheco.pandadb.facade.Direction
import org.neo4j.values.storable.Values

import java.util
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

  def scalaType2javaType(origin: Any): Any = {
    origin match {
      case l: List[Any] => l.map(scalaType2javaType(_)).asJava
      case m: Map[String, Any] => m.mapValues(scalaType2javaType(_)).asJava
      case p: Geographic2D => Values.pointValue(org.neo4j.values.storable.CoordinateReferenceSystem.WGS84, p.x.value, p.y.value)
      case p: Cartesian2D => Values.pointValue(org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian, p.x.value, p.y.value)
      case _ => origin
    }
  }

  def javaType2scalaType(origin: Any): Any = {
    origin match {
      case v: util.Map[Any, Any] => v.asScala.toMap
      case v: util.List[Any] => v.asScala.toList
      case v: util.Collection[Any] => v.asScala
      case v: util.Set[Any] => v.asScala.toSet
      case p: org.neo4j.graphdb.spatial.Point =>
        val cs = p.getCoordinate.getCoordinate
        val x = LynxFloat(cs.get(0))
        val y = LynxFloat(cs.get(1))
        p.getCRS match {
          case org.neo4j.values.storable.CoordinateReferenceSystem.WGS84 => Geographic2D(x, y)
          case org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian => Cartesian2D(x, y)
        }
      case v: Any  => v
    }
  }
}

