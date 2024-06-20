package org.grapheco.pandadb.neocompat

import org.grapheco.pandadb.{GraphDataBaseBuilder, facade}
import org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME
import org.neo4j.dbms.api.{DatabaseExistsException, DatabaseManagementService, DatabaseNotFoundException}
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.config.Configuration
import org.neo4j.graphdb.event.{DatabaseEventListener, TransactionEventListener}

import scala.collection.JavaConverters._
import java.nio.file.Path
import java.util

class DatabaseManagementServiceImpl(homeDir: String) extends DatabaseManagementService {

  private val dbCache = new scala.collection.mutable.HashMap[String, facade.GraphDatabaseService]()

  forceLoadDB(DEFAULT_DATABASE_NAME)

  Path.of(homeDir).toFile.list(org.apache.commons.io.filefilter.FileFilterUtils.suffixFileFilter(".pdb", null)).foreach(dirName => {
    val dbName = dirName.substring(0, dirName.length - 4)
    dbCache.getOrElseUpdate(dbName, null)
  })

  override def database(databaseName: String): GraphDatabaseService = {
    var pdb = dbCache.get(databaseName).getOrElse(throw new DatabaseNotFoundException())
    if (pdb == null) pdb = GraphDataBaseBuilder.newEmbeddedDatabase(f"$homeDir/$databaseName.pdb")
    GraphDatabaseFacade(databaseName, pdb)
  }

  override def createDatabase(databaseName: String, databaseSpecificSettings: Configuration): Unit = {
    if (dbCache.contains(databaseName) || {
      Path.of(f"$homeDir/$databaseName.pdb").toFile.exists()
    }) throw new DatabaseExistsException()
    forceLoadDB(databaseName)
  }

  override def dropDatabase(databaseName: String): Unit = {
    val pdb = dbCache.get(databaseName).getOrElse(throw new DatabaseNotFoundException())
    if (pdb != null) pdb.close()
    org.apache.commons.io.FileUtils.deleteDirectory(Path.of(f"$homeDir/$databaseName.pdb").toFile)
    dbCache.remove(databaseName)
  }

  override def startDatabase(databaseName: String): Unit = {
    val pdb = dbCache.get(databaseName).getOrElse(throw new DatabaseNotFoundException())
    if (pdb == null) forceLoadDB(databaseName)
  }

  override def shutdownDatabase(databaseName: String): Unit = {
    val pdb = dbCache.get(databaseName).getOrElse(throw new DatabaseNotFoundException())
    if (pdb != null) pdb.close()
  }

  override def listDatabases(): util.List[String] = {
    dbCache.keys.toList.asJava
  }

  override def registerDatabaseEventListener(listener: DatabaseEventListener): Unit = ???

  override def unregisterDatabaseEventListener(listener: DatabaseEventListener): Unit = ???

  override def registerTransactionEventListener(databaseName: String, listener: TransactionEventListener[_]): Unit = ???

  override def unregisterTransactionEventListener(databaseName: String, listener: TransactionEventListener[_]): Unit = ???

  override def shutdown(): Unit = {
    dbCache.foreach(pair => pair._2.close())
    dbCache.clear()
  }

  private def forceLoadDB(name: String): GraphDatabaseFacade = {
    val pdb = GraphDataBaseBuilder.newEmbeddedDatabase(f"$homeDir/$name.pdb")
    dbCache.put(name, pdb)
    GraphDatabaseFacade(name, pdb)
  }

  private def trashDatabase(databaseName: String): Unit = {
    dbCache.get(databaseName).getOrElse(throw new DatabaseNotFoundException()).close()
    val dir = Path.of(f"$homeDir/$databaseName.pdb").toFile
    val droppedDir = Path.of(f"$homeDir/$databaseName.pdb.dropped").toFile
    dir.renameTo(droppedDir)
  }

}
