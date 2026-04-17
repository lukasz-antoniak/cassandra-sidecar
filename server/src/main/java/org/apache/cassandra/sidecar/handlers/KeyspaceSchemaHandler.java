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
package org.apache.cassandra.sidecar.handlers;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TableMetadata;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.net.SocketAddress;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.web.RoutingContext;
import org.apache.cassandra.sidecar.acl.authorization.BasicPermissions;
import org.apache.cassandra.sidecar.common.response.SchemaResponse;
import org.apache.cassandra.sidecar.common.server.data.Name;
import org.apache.cassandra.sidecar.concurrent.ExecutorPools;
import org.apache.cassandra.sidecar.db.CQLSchemaAccessor;
import org.apache.cassandra.sidecar.utils.CassandraInputValidator;
import org.apache.cassandra.sidecar.utils.InstanceMetadataFetcher;
import org.apache.cassandra.sidecar.utils.MetadataUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.cassandra.sidecar.utils.HttpExceptions.wrapHttpException;

/**
 * The {@link KeyspaceSchemaHandler} class handles keyspace schema requests.
 * Java driver 3.x does not return {@link TableMetadata} for tables which schema could not be parsed.
 * Since it does not currently support vector type, any table containing vector will not be visible in API
 * response. To workaround this limitation, class maintains a schema cache. Cache entries encapsulate
 * schema string (calculated based on {@code DESCRIBE} statement) and a flag representing compatibility
 * with {@link TableMetadata} variant. If both schemas are compatible, response from {@link TableMetadata}
 * is returned, because it can be more accurate due to asynchronous {@code SCHEMA_CHANGE} events.
 */
@Singleton
public class KeyspaceSchemaHandler extends AbstractHandler<Name> implements AccessProtected
{
    private static final String STATEMENT_DELIMITER = "\n\n";

    private final CQLSchemaAccessor cqlSchemaAccessor;

    private final Cache<Name, KeyspaceSchema> schemaCache = Caffeine.newBuilder()
                                                                    .expireAfterWrite(5, TimeUnit.MINUTES)
                                                                    .build();

    /**
     * Constructs a handler with the provided {@code metadataFetcher}
     *
     * @param metadataFetcher   the interface to retrieve metadata
     * @param executorPools     executor pools for blocking executions
     * @param validator         a validator instance to validate Cassandra-specific input
     * @param cqlSchemaAccessor schema reader using CQL {@code DESCRIBE} statement
     */
    @Inject
    protected KeyspaceSchemaHandler(InstanceMetadataFetcher metadataFetcher,
                                    ExecutorPools executorPools,
                                    CassandraInputValidator validator,
                                    CQLSchemaAccessor cqlSchemaAccessor)
    {
        super(metadataFetcher, executorPools, validator);
        this.cqlSchemaAccessor = cqlSchemaAccessor;
    }

    @Override
    public Set<Authorization> requiredAuthorizations()
    {
        return Collections.singleton(BasicPermissions.READ_SCHEMA_KEYSPACE_SCOPED.toAuthorization());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void handleInternal(RoutingContext context,
                               HttpServerRequest httpRequest,
                               @NotNull String host,
                               SocketAddress remoteAddress,
                               @Nullable Name keyspace)
    {
        schema(host, keyspace)
        .onFailure(cause -> processFailure(cause, context, host, remoteAddress, keyspace))
        .onSuccess(schema -> processSuccess(context, keyspace, schema));
    }

    /**
     * Handles the request with the Cassandra {@link Metadata metadata}.
     *
     * @param context  the event to handle
     * @param keyspace the keyspace parsed from the request
     * @param schema   the CQL schema
     */
    private void processSuccess(RoutingContext context, Name keyspace, String schema)
    {
        SchemaResponse schemaResponse = keyspace == null
                                        ? new SchemaResponse(schema)
                                        : new SchemaResponse(keyspace.name(), schema);
        context.json(schemaResponse);
    }

    /**
     * Gets CQL schema asynchronously,
     *
     * @param host the Cassandra instance host
     * @return {@link Future} containing CQL schema
     */
    private Future<String> schema(String host, Name keyspace)
    {
        return executorPools.service().executeBlocking(() -> {
            // metadata and CQL queries can block, so we need to run in a blocking thread
            Metadata metadata = metadataFetcher.delegate(host).metadata();

            Set<Name> keyspaces = new HashSet<>();
            if (keyspace == null)
            {
                // resolve all possibly known keyspaces
                keyspaces.addAll(cqlSchemaAccessor.getKeyspaces());
                keyspaces.addAll(metadata.getKeyspaces()
                                         .stream()
                                         .map(ks -> new Name(ks.getName()))
                                         .collect(Collectors.toList()));
            }
            else
            {
                keyspaces.add(keyspace);
            }

            StringBuilder fullSchema = new StringBuilder();
            for (Name ks : keyspaces)
            {
                // retrieve keyspace metadata
                KeyspaceMetadata ksMeta = MetadataUtils.keyspace(metadata, ks);

                KeyspaceSchema keyspaceSchema = schemaCache.get(ks, (k) -> {
                    List<String> createStatements = cqlSchemaAccessor.getSchema(ks);

                    if (ksMeta == null || createStatements == null)
                    {
                        // keyspace does not exist
                        String errorMessage = String.format("Keyspace '%s' does not exist.", keyspace);
                        throw wrapHttpException(HttpResponseStatus.NOT_FOUND, errorMessage);
                    }

                    return new KeyspaceSchema(createStatements, ksMeta);
                });

                String schema = keyspaceSchema.metadataCompliant
                                ? ksMeta.exportAsString()
                                : keyspaceSchema.cqlSchema;
                fullSchema.append(schema).append(STATEMENT_DELIMITER);
            }

            return fullSchema.toString().trim();
        });
    }

    /**
     * Parses the request parameters
     *
     * @param context the event to handle
     * @return the keyspace parsed from the request
     */
    @Override
    protected Name extractParamsOrThrow(RoutingContext context)
    {
        return keyspace(context, true);
    }

    private static class KeyspaceSchema
    {
        private final boolean metadataCompliant;
        private final String cqlSchema; // schema retrieved using DESCRIBE CQL statement

        private KeyspaceSchema(List<String> createStatements, KeyspaceMetadata ksMetadata)
        {
            this.metadataCompliant = isMetadataCompliantSchema(createStatements, ksMetadata);
            this.cqlSchema = String.join(STATEMENT_DELIMITER, createStatements);
        }

        private boolean isMetadataCompliantSchema(List<String> createStatements, KeyspaceMetadata ksMetadata)
        {
            // check if we have definition for all tables in driver metadata
            long tableCount = createStatements.stream()
                                              .filter(s -> s.toUpperCase().startsWith("CREATE TABLE"))
                                              .count();
            return ksMetadata.getTables().size() == tableCount;
        }
    }
}
