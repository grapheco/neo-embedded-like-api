/*
 * Licensed to Neo4j under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Neo4j licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.grapheco.neo4panda;

import java.io.IOException;
import java.nio.file.Path;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

public class TestEmbeddedNeo
{
    private static final Path databaseDirectory = Path.of( "out/neo4j-hello-db" );

    public String greeting;

    // tag::vars[]
    GraphDatabaseService graphDb;
    Node firstNode;
    Node secondNode;
    Relationship relationship;

    // tag::createReltype[]
    private enum RelTypes implements RelationshipType
    {
        KNOWS
    }
    // end::createReltype[]

    public static void main( final String[] args ) throws IOException
    {
        TestUtil.startDBMSService();
        TestEmbeddedNeo hello = new TestEmbeddedNeo();
        hello.createDb();
        hello.removeData();
    }

    void createDb() throws IOException
    {
        // tag::startDb[]
        graphDb = TestUtil.createDb();
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                TestUtil.shutDownDBMSService();
            }
        } );
        // end::startDb[]

        // tag::transaction[]
        try ( Transaction tx = graphDb.beginTx() )
        {
            // Database operations go here
            // end::transaction[]
            // tag::addData[]
            firstNode = tx.createNode();
            firstNode.setProperty( "message", "Hello, " );
            secondNode = tx.createNode();
            secondNode.setProperty( "message", "World!" );

            relationship = firstNode.createRelationshipTo( secondNode, RelTypes.KNOWS );
            relationship.setProperty( "message", "brave Neo4j " );
            // end::addData[]

            // tag::readData[]
            System.out.print( firstNode.getProperty( "message" ) );
            System.out.print( relationship.getProperty( "message" ) );
            System.out.print( secondNode.getProperty( "message" ) );
            // end::readData[]

            greeting = ( (String) firstNode.getProperty( "message" ) )
                    + ( (String) relationship.getProperty( "message" ) )
                    + ( (String) secondNode.getProperty( "message" ) );

            // tag::transaction[]
            tx.commit();
        }
        // end::transaction[]
    }

    void removeData()
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            // tag::removingData[]
            // let's remove the data
            firstNode = tx.getNodeById( firstNode.getId() );
            secondNode = tx.getNodeById( secondNode.getId() );
            firstNode.getSingleRelationship( RelTypes.KNOWS, Direction.OUTGOING ).delete();
            firstNode.delete();
            secondNode.delete();
            // end::removingData[]

            tx.commit();
        }
    }
}