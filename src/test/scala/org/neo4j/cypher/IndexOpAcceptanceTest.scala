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
import org.junit.jupiter.api.{AfterAll, BeforeAll}

import org.neo4j.graphdb.{GraphDatabaseService, Result, Label, Transaction}

import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import org.junit.jupiter.api._

object IndexOpAcceptanceTest {
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

class IndexOpAcceptanceTest {

  @Test
  def createIndex: Unit = {
    // WHEN
    execute("create (n: Person{name: 'test'})")
    execute("create index on:Person(name)")// TODO support execute("CREATE INDEX FOR (n:Person) ON (n.name)")
    List.range(0, 100).foreach(print(_))
    execute("drop index on:Person(name)")
    // THEN
    var tx: Transaction = null
    try {
      tx = IndexOpAcceptanceTest.db.beginTx
      assert(indexPropsForLabel(tx, "Person") == List(List("name")))
    } finally {
      if (tx != null) {
        tx.close()
      }
    }
  }

  @Test
  def createIndexShouldFailWhenCreatedTwice: Unit = {
    // GIVEN
    execute("CREATE INDEX FOR (n:Person) ON (n.name)")
    execute("CREATE INDEX FOR (n:Person) ON (n.name)")
  }

  @Test
  def secondIndexCreationShouldFailIfIndexesHasFailed: Unit = {
    execute("CREATE INDEX FOR (n:Person) ON (n.name)")
  }

  @Test
  def dropIndex: Unit = {
    // GIVEN
    execute("CREATE INDEX FOR (n:Person) ON (n.name)")

  // WHEN
    execute("DROP INDEX ON :Person(name)")
    var tx: Transaction = null
    try {
      tx = IndexOpAcceptanceTest.db.beginTx
      assert(indexPropsForLabel( tx, "Person").size == 0)
    } finally {
      if (tx != null) {
        tx.close()
      }
    }
  }

  @Test
  def drop_index_that_does_not_exist: Unit = {
    // WHEN
    execute("DROP INDEX ON :Person(name)") //DropIndexFailureException
  }

  def indexPropsForLabel(tx: Transaction, label: String): List[List[String]] = {
    val indexDefs = tx.schema.getIndexes(Label.label(label)).asScala.toList
    indexDefs.map(_.getPropertyKeys.asScala.toList)
  }

  private def execute(cypher: String): Result= {
    var tx: Transaction = null
    try {
      tx = IndexOpAcceptanceTest.db.beginTx
      val r = tx.execute(cypher)
      tx.commit()
      r
    } finally {
      if (tx != null) {
        tx.close()
      }
    }
  }
}
