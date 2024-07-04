package org.grapheco.pandadb.neocompat

import org.grapheco.lynx.types.LynxValue
import org.grapheco.lynx.types.property.{LynxFloat, LynxNull}
import org.grapheco.lynx.types.spatial.{Cartesian2D, Cartesian3D, Geographic2D, Geographic3D}
import org.grapheco.lynx.types.structural.{LynxNode, LynxRelationship}
import org.grapheco.pandadb.facade.Direction.Direction
import org.grapheco.pandadb.facade.{Direction, PandaTransaction}
import org.neo4j.exceptions.CypherExecutionException
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

  def unwrapLynxValue(tx: PandaTransaction, origin: LynxValue): Any = {
    origin.value match {
      case l: List[LynxValue] => l.map(unwrapLynxValue(tx, _)).asJava
      case m: Map[String, LynxValue] => m.mapValues(unwrapLynxValue(tx, _)).asJava
      case n: LynxNode => NodeImpl(tx, n)
      case r: LynxRelationship => RelationshipImpl(tx, r)
      case p: Geographic2D => Values.pointValue(org.neo4j.values.storable.CoordinateReferenceSystem.WGS84, p.x.value, p.y.value)
      case p: Geographic3D => Values.pointValue(org.neo4j.values.storable.CoordinateReferenceSystem.WGS84_3D, p.x.value, p.y.value, p.z.value)
      case p: Cartesian2D => Values.pointValue(org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian, p.x.value, p.y.value)
      case p: Cartesian3D => Values.pointValue(org.neo4j.values.storable.CoordinateReferenceSystem.Cartesian_3D, p.x.value, p.y.value, p.z.value)
      case other => other
    }
  }

  def toLynxValue(origin: Any): Any = {
    origin match {
      case v: Array[Any] => v.map(toLynxValue) // java array
      case v: util.List[Any] => v.asScala.toList.map(toLynxValue)
      case v: util.Map[Any, Any] => v.asScala.toMap.mapValues(toLynxValue)
      case v: util.Collection[Any] => v.asScala.map(toLynxValue)
      case v: util.Set[Any] => v.asScala.toSet.map(toLynxValue)
      case p: org.neo4j.graphdb.spatial.Point =>
        val cs = p.getCoordinate.getCoordinate
        val x = LynxFloat(cs.get(0))
        val y = LynxFloat(cs.get(1))
        p.getCRS.getCode match {
          case 4326 => Geographic2D(x, y)
          case 4979 => Geographic3D(x, y, LynxFloat(cs.get(3)))
          case 7203 => Cartesian2D(x, y)
          case 9157 => Cartesian3D(x, y, LynxFloat(cs.get(3)))
        }
      case _: org.neo4j.graphdb.spatial.Geometry => throw new CypherExecutionException("PandaDB hasn't support Geometry data type.")
      case v  => LynxValue(v)
    }
  }
}

