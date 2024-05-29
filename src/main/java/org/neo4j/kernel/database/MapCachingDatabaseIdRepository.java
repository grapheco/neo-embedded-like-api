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
package org.neo4j.kernel.database;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class MapCachingDatabaseIdRepository implements DatabaseIdRepository.Caching
{
//    private static final Optional<NamedDatabaseId> OPT_SYS_DB = Optional.of( NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID );

    private volatile Map<String,NamedDatabaseId> databaseIdsByName;
    private volatile Map<DatabaseId,NamedDatabaseId> databaseIdsByUuid;

    public MapCachingDatabaseIdRepository( )
    {
        this.databaseIdsByName = new ConcurrentHashMap<>();
        this.databaseIdsByUuid = new ConcurrentHashMap<>();
    }

    @Override
    public Optional<NamedDatabaseId> getByName( NormalizedDatabaseName databaseName )
    {
//        if ( NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID.name().equals( databaseName.name() ) )
//        {
//            return OPT_SYS_DB;
//        }
        var dbId = Optional.ofNullable( databaseIdsByName.getOrDefault( databaseName.name(), new NamedDatabaseId(databaseName.name(), UUID.fromString("9fbef8ae-6ca7-4192-8e9f-659acc5120b2"))));//todo pandadb db has no uuid
        dbId.ifPresent( id -> databaseIdsByUuid.put( id.databaseId(), id ) );
        return dbId;
    }

    @Override
    public Optional<NamedDatabaseId> getById( DatabaseId uuid )
    {
//        if ( NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID.databaseId().equals( uuid ) )
//        {
//            return OPT_SYS_DB;
//        }
        var dbId = Optional.ofNullable( databaseIdsByUuid.getOrDefault( uuid, new NamedDatabaseId("neo4j", uuid)));//todo pandadb db has no uuid
        dbId.ifPresent( id -> databaseIdsByName.put( id.name(), id ) );
        return dbId;
    }

    /**
     * We recreate the maps rather than .clear() because .clear() is not atomic
     *  and a concurrent .computeIfAbsent() could preserve a stale value.
     */
    @Override
    public void invalidateAll()
    {
        this.databaseIdsByName = new ConcurrentHashMap<>();
        this.databaseIdsByUuid = new ConcurrentHashMap<>();
    }
}
