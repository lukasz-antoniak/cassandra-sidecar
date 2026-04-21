/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.sidecar.db;

import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.inject.Singleton;
import io.vertx.core.Promise;
import org.apache.cassandra.sidecar.common.server.CQLSessionProvider;
import org.apache.cassandra.sidecar.common.server.data.Name;
import org.apache.cassandra.sidecar.common.server.data.QualifiedTableName;
import org.apache.cassandra.sidecar.common.server.utils.DurationSpec;
import org.apache.cassandra.sidecar.config.SidecarConfiguration;
import org.apache.cassandra.sidecar.exceptions.CassandraUnavailableException;
import org.apache.cassandra.sidecar.tasks.PeriodicTask;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * The {@link DriverUnsupportedSchemaCache} class maintains cache of CQL schema for tables whose definition is not
 * supported by driver natively. Java driver 3.x does not return {@link TableMetadata} for tables which schema
 * could not be parsed. Since it does not currently support vector type, any table containing vector would not
 * be visible.
 */
@Singleton
public class DriverUnsupportedSchemaCache implements PeriodicTask
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DriverUnsupportedSchemaCache.class);

    private final SidecarConfiguration sidecarConfiguration;
    private final CQLSessionProvider sessionProvider;
    private final CQLSchemaAccessor schemaAccessor;
    private final AtomicReference<PreparedStatement> tableListStatement;
    private final Cache<QualifiedTableName, String> schemaCache;

    public DriverUnsupportedSchemaCache(SidecarConfiguration sidecarConfiguration,
                                        CQLSessionProvider sessionProvider)
    {
        this.sidecarConfiguration = sidecarConfiguration;
        this.sessionProvider = sessionProvider;
        this.schemaAccessor = new CQLSchemaAccessor(sessionProvider);
        // cache 2.5 times longer than refresh period
        this.schemaCache = Caffeine.newBuilder()
                                   .expireAfterWrite((long) (2.5 * delay().toMillis()), TimeUnit.MILLISECONDS)
                                   .build();
        this.tableListStatement = new AtomicReference<>();
        populateSchemaCache();
    }

    /**
     * @return Schema for all tables across all keyspaces not supported by Java driver.
     */
    @NotNull
    public String getFullSchema()
    {
        return getUnsupportedSchema(table -> true);
    }

    /**
     * @return Schema for all tables within given keyspaces not supported by Java driver.
     */
    @NotNull
    public String getKeyspaceSchema(@NotNull Name keyspace)
    {
        return getUnsupportedSchema(table -> keyspace.equals(table.getKeyspace()));
    }

    /**
     * @return Schema for table if it is not supported by Java driver's metadata, {@code null} otherwise.
     */
    @Nullable
    public String getTableSchema(@NotNull Name keyspace, @NotNull Name table)
    {
        QualifiedTableName name = new QualifiedTableName(keyspace, table);
        String schema = schemaCache.getIfPresent(name);
        if (schema == null)
        {
            // proactively trying to fetch table schema
            schema = populateSchemaCache(name);
        }
        return schema;
    }

    @Override
    public DurationSpec delay()
    {
        return sidecarConfiguration.driverConfiguration().unsupportedTableSchemaRefreshTime();
    }

    @Override
    public void execute(Promise<Void> promise)
    {
        try
        {
            populateSchemaCache();
            promise.tryComplete();
        }
        catch (Throwable t)
        {
            promise.fail(t);
        }
    }

    private String getUnsupportedSchema(Predicate<QualifiedTableName> predicate)
    {
        Map<QualifiedTableName, String> snapshot = cacheSnapshot();
        StringBuilder result = new StringBuilder();
        for (Map.Entry<QualifiedTableName, String> entry : snapshot.entrySet())
        {
            if (predicate.test(entry.getKey()))
            {
                result.append(entry.getValue()).append("\n\n");
            }
        }
        return result.toString().trim();
    }

    private Map<QualifiedTableName, String> cacheSnapshot()
    {
        // use sorted map for repeatable results when retrieving full schema
        TreeMap<QualifiedTableName, String> snapshot = new TreeMap<>(Comparator.comparing(QualifiedTableName::toString));
        snapshot.putAll(schemaCache.asMap());
        return snapshot;
    }

    private void populateSchemaCache()
    {
        try
        {
            Set<QualifiedTableName> tables = queryAllTables();
            Set<QualifiedTableName> driverKnownTables = driverKnownTables();

            tables.removeAll(driverKnownTables);

            if (!tables.isEmpty())
            {
                LOGGER.debug("Tables not known to Java driver: {}", tables);
                tables.forEach(this::populateSchemaCache);
            }
        }
        catch (CassandraUnavailableException ignored)
        {
            LOGGER.debug("Not yet connect to Cassandra cluster");
        }
    }

    private String populateSchemaCache(QualifiedTableName table)
    {
        return schemaCache.get(table, (k) -> {
            List<String> cqlSchema = schemaAccessor.getTableSchema(table.getKeyspace(), table.table());
            return cqlSchema != null ? String.join("\n\n", cqlSchema) : null;
        });
    }

    private Set<QualifiedTableName> queryAllTables()
    {
        Session session = sessionProvider.get();
        tableListStatement.compareAndSet(null, session.prepare("SELECT keyspace_name, table_name FROM system_schema.tables"));
        List<Row> rows = session.execute(tableListStatement.get().bind()).all();
        return rows.stream()
                   .map(r -> new QualifiedTableName(r.getString("keyspace_name"), r.getString("table_name")))
                   .collect(Collectors.toSet());
    }

    private Set<QualifiedTableName> driverKnownTables()
    {
        Set<QualifiedTableName> result = new HashSet<>();
        Cluster cluster = sessionProvider.get().getCluster();
        for (KeyspaceMetadata keyspace : cluster.getMetadata().getKeyspaces())
        {
            for (TableMetadata table : keyspace.getTables())
            {
                result.add(new QualifiedTableName(keyspace.getName(), table.getName()));
            }
        }
        return result;
    }
}
