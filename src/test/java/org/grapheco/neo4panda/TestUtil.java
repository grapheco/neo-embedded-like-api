package org.grapheco.neo4panda;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.io.fs.FileUtils;

import java.io.IOException;
import java.nio.file.Path;
import org.neo4j.graphdb.GraphDatabaseService;


import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class TestUtil {
    private static final Path databaseDirectory = Path.of( "out/neo4j-hello-db" );

    private static DatabaseManagementService managementService;

    public static void startDBMSService() {
        try {
            FileUtils.deleteDirectory( databaseDirectory );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        managementService = new DatabaseManagementServiceBuilder( databaseDirectory ).build();
    }

    // end::vars[]

    public static GraphDatabaseService createDb()
    {
        return managementService.database( DEFAULT_DATABASE_NAME );
    }

    public static void shutDownDBMSService()
    {
        System.out.println( "Shutting down database ..." );
        // tag::shutdownServer[]
        managementService.shutdown();
        // end::shutdownServer[]
    }

}