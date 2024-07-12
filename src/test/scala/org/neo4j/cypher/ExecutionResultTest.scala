/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher

import org.grapheco.neo4panda.TestUtil
import org.neo4j.graphdb.Node
import org.junit.jupiter.api._
import org.junit.jupiter.api.{AfterAll, BeforeAll}
import org.junit.jupiter.api.Assertions.assertTrue
import org.neo4j.graphdb.{GraphDatabaseService, Result, Transaction}
import scala.collection.JavaConverters._

import java.util.regex.Pattern

object ExecutionResultTest {
  private var db: GraphDatabaseService = null
  @BeforeAll
  def prepare(): Unit = {
    TestUtil.startDBMSService()
    db = TestUtil.createDb
  }

  @AfterAll
  def cleanup(): Unit = {
    TestUtil.shutDownDBMSService()
  }
}

class ExecutionResultTest {

  @Test
  def columnOrderIsPreserved = {
    val columns = List("zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine")
    columns.foreach(_ => createNode())

    val q="match (zero), (one), (two), (three), (four), (five), (six), (seven), (eight), (nine) " +
      "where id(zero) = 0 AND id(one) = 1 AND id(two) = 2 AND id(three) = 3 AND id(four) = 4 AND id(five) = 5 AND id(six) = 6 AND id(seven) = 7 AND id(eight) = 8 AND id(nine) = 9 " +
      "return zero, one, two, three, four, five, six, seven, eight, nine"

    val cols  = execute(q).columns.asScala.toList
    assert(cols == columns)

    val regex = "zero.*one.*two.*three.*four.*five.*six.*seven.*eight.*nine"
    val pattern = Pattern.compile(regex)

    val test = execute("match (n) return n")
    println(test.resultAsString())

    val stringDump2 = execute(q)
    val stringDump = stringDump2.resultAsString()
    assertTrue( pattern.matcher(stringDump).find(), "Columns did not appear in the expected order: \n" + stringDump )
  }

  @Test
  def correctLabelStatisticsForCreate = {
    val result = execute("create (n:foo:bar)")
    val stats  = result.getQueryStatistics

    assert(stats.getLabelsAdded == 2)
    assert(stats.getLabelsRemoved == 0)
  }

  @Test
  def correctLabelStatisticsForAdd = {
    val n      = createNode()
    val result = execute(s"match (n) where id(n) = ${n.getId} set n:foo:bar")
    val stats  = result.getQueryStatistics

    assert(stats.getLabelsAdded == 2)
    assert(stats.getLabelsRemoved == 0)
  }

  @Test
  def correctLabelStatisticsForRemove = {
    val n      = createNode()
    execute(s"match (n) where id(n) = ${n.getId} set n:foo:bar")
    val result = execute(s"match (n) where id(n) = ${n.getId} remove n:foo:bar")
    val stats  = result.getQueryStatistics

    assert(stats.getLabelsAdded == 0)
    assert(stats.getLabelsRemoved == 2)
  }

  @Test
  def correctLabelStatisticsForAddAndRemove = {
    val n      = createLabeledNode("foo", "bar")
    val result = execute(s"match (n) where id(n) = ${n.getId} set n:baz remove n:foo:bar")
    val stats  = result.getQueryStatistics

    assert(stats.getLabelsAdded == 1)
    assert(stats.getLabelsRemoved == 2)
  }

  @Test
  def correctLabelStatisticsForLabelAddedTwice = {
    val n      = createLabeledNode("foo", "bar")
    val result = execute(s"match (n) where id(n) = ${n.getId} set n:bar:baz")
    val stats  = result.getQueryStatistics

    assert(stats.getLabelsAdded == 1)
    assert(stats.getLabelsRemoved == 0)
  }

  @Test
  def correctLabelStatisticsForRemovalOfUnsetLabel = {
    val n      = createLabeledNode("foo", "bar")
    val result = execute(s"match (n) where id(n) = ${n.getId} remove n:baz:foo")
    val stats  = result.getQueryStatistics

    assert(stats.getLabelsAdded == 0)
    assert(stats.getLabelsRemoved == 1)
  }

  @Test
  def correctIndexStatisticsForIndexAdded = {
    createLabeledNode("Person")
    val result = execute("create index on :Person(name)")
    val stats  = result.getQueryStatistics

    assert(stats.getIndexesAdded == 1)
    assert(stats.getIndexesRemoved == 0)
  }

  @Test
  def correctIndexStatisticsForIndexWithNameAdded = {
    val result = execute("create index my_index for (n:Person) on (n.name)")
    val stats  = result.getQueryStatistics

    assert(stats.getIndexesAdded == 1)
    assert(stats.getIndexesRemoved == 0)
  }

  @Test
  def correctConstraintStatisticsForUniquenessConstraintAdded =  {
    createLabeledNode("Person")
    val result = execute("create constraint for (n:Person) require n.name is unique")//TODO not opencypher syntax, may impl it using pandadb.createConstraint("Person", "name", UNIQUE)
    val stats  = result.getQueryStatistics

    assert(stats.getConstraintsAdded == 1)
    assert(stats.getConstraintsRemoved == 0)
  }

  @Test
  def hasNextShouldNotChangeResultAsString = {
      val result = execute("UNWIND [1,2,3] AS x RETURN x")
      result.hasNext
      assert(result.resultAsString() ==
        """+---+
          || x |
          |+---+
          || 1 |
          || 2 |
          || 3 |
          |+---+
          |3 rows
          |""".stripMargin)
  }

  @Test
  def nextShouldChangeResultAsString = {
    val result = execute("UNWIND [1,2,3] AS x RETURN x")
    assert(result.next().get("x") == 1)
    assert(result.resultAsString() ==
        """+---+
          || x |
          |+---+
          || 2 |
          || 3 |
          |+---+
          |2 rows
          |""".stripMargin)
  }

  private def execute(cypher: String): Result= {
    var tx: Transaction = null
    try {
      tx = ExecutionResultTest.db.beginTx
      val r = tx.execute(cypher)
      tx.commit()
      r
    } finally {
      if (tx != null) {
        tx.close()
      }
    }
  }

  private def createNode(): Node= {
    execute(s"create (x)").columnAs("x").next().asInstanceOf[Node]
  }

  private def createLabeledNode(labels: String*): Node = {
    val ls = labels.mkString(": ")
    execute(s"create (x: $ls)").columnAs("x").next().asInstanceOf[Node]
  }

}
