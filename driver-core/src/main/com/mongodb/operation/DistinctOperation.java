/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.operation;

import com.mongodb.MongoNamespace;
import com.mongodb.ServerAddress;
import com.mongodb.async.AsyncBatchCursor;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.AsyncConnectionSource;
import com.mongodb.binding.AsyncReadBinding;
import com.mongodb.binding.ConnectionSource;
import com.mongodb.binding.ReadBinding;
import com.mongodb.client.model.Collation;
import com.mongodb.connection.AsyncConnection;
import com.mongodb.connection.Connection;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.QueryResult;
import com.mongodb.connection.ServerDescription;
import com.mongodb.operation.CommandOperationHelper.CommandTransformer;
import com.mongodb.operation.OperationHelper.AsyncCallableWithConnectionAndSource;
import com.mongodb.operation.OperationHelper.CallableWithConnectionAndSource;
import com.mongodb.session.SessionContext;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.codecs.Codec;
import org.bson.codecs.Decoder;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.operation.CommandOperationHelper.CommandCreator;
import static com.mongodb.operation.CommandOperationHelper.executeCommand;
import static com.mongodb.operation.CommandOperationHelper.executeWrappedCommandProtocolAsync;
import static com.mongodb.operation.DocumentHelper.putIfNotNullOrEmpty;
import static com.mongodb.operation.DocumentHelper.putIfNotZero;
import static com.mongodb.operation.OperationHelper.LOGGER;
import static com.mongodb.operation.OperationHelper.releasingCallback;
import static com.mongodb.operation.OperationHelper.validateReadConcernAndCollation;
import static com.mongodb.operation.OperationHelper.withConnection;
import static com.mongodb.operation.OperationReadConcernHelper.appendReadConcernToCommand;

/**
 * Finds the distinct values for a specified field across a single collection.
 *
 * <p>When possible, the distinct command uses an index to find documents and return values.</p>
 *
 * @param <T> the type of the distinct value
 * @mongodb.driver.manual reference/command/distinct Distinct Command
 * @since 3.0
 */
@Deprecated
public class DistinctOperation<T> implements AsyncReadOperation<AsyncBatchCursor<T>>, ReadOperation<BatchCursor<T>> {
    private static final String VALUES = "values";

    private final MongoNamespace namespace;
    private final String fieldName;
    private final Decoder<T> decoder;
    private Boolean retryReads;
    private BsonDocument filter;
    private long maxTimeMS;
    private Collation collation;

    /**
     * Construct an instance.
     *
     * @param namespace the database and collection namespace for the operation.
     * @param fieldName the name of the field to return distinct values.
     * @param decoder   the decoder for the result documents.
     */
    public DistinctOperation(final MongoNamespace namespace, final String fieldName, final Decoder<T> decoder) {
        this.namespace = notNull("namespace", namespace);
        this.fieldName = notNull("fieldName", fieldName);
        this.decoder = notNull("decoder", decoder);
    }

    /**
     * Gets the query filter.
     *
     * @return the query filter
     * @mongodb.driver.manual reference/method/db.collection.find/ Filter
     */
    public BsonDocument getFilter() {
        return filter;
    }

    /**
     * Sets the query filter to apply to the query.
     *
     * @param filter the query filter, which may be null.
     * @return this
     * @mongodb.driver.manual reference/method/db.collection.find/ Filter
     */
    public DistinctOperation<T> filter(final BsonDocument filter) {
        this.filter = filter;
        return this;
    }

    /**
     * Enables retryable reads if a read fails due to a network error.
     *
     * @param retryReads true if reads should be retried
     * @return this
     * @mongodb.driver.manual reference/method/db.collection.find/ Filter
     * @since 3.11
     */
    public DistinctOperation<T> retryReads(final Boolean retryReads) {
        this.retryReads = retryReads;
        return this;
    }

    /**
     * Gets the value for retryable reads. The default is true.
     *
     * @return the retryable reads value
     * @since 3.11
     */
    public Boolean getRetryReads() {
        return (this.retryReads == null ? true : retryReads);
    }

    /**
     * Gets the maximum execution time on the server for this operation.  The default is 0, which places no limit on the execution time.
     *
     * @param timeUnit the time unit to return the result in
     * @return the maximum execution time in the given time unit
     */
    public long getMaxTime(final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        return timeUnit.convert(maxTimeMS, TimeUnit.MILLISECONDS);
    }

    /**
     * Sets the maximum execution time on the server for this operation.
     *
     * @param maxTime  the max time
     * @param timeUnit the time unit, which may not be null
     * @return this
     */
    public DistinctOperation<T> maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    /**
     * Returns the collation options
     *
     * @return the collation options
     * @since 3.4
     * @mongodb.server.release 3.4
     */
    public Collation getCollation() {
        return collation;
    }

    /**
     * Sets the collation options
     *
     * <p>A null value represents the server default.</p>
     * @param collation the collation options to use
     * @return this
     * @since 3.4
     * @mongodb.server.release 3.4
     */
    public DistinctOperation<T> collation(final Collation collation) {
        this.collation = collation;
        return this;
    }

    @Override
    public BatchCursor<T> execute(final ReadBinding binding) {
        return withConnection(binding, new CallableWithConnectionAndSource<BatchCursor<T>>() {
            @Override
            public BatchCursor<T> call(final ConnectionSource source, final Connection connection) {
                validateReadConcernAndCollation(connection, binding.getSessionContext().getReadConcern(), collation);
                return executeCommand(binding, namespace.getDatabaseName(), getCommandCreator(binding.getSessionContext()),
                        createCommandDecoder(), transformer(source, connection), getRetryReads());
            }
        });
    }

    @Override
    public void executeAsync(final AsyncReadBinding binding, final SingleResultCallback<AsyncBatchCursor<T>> callback) {
        withConnection(binding, new AsyncCallableWithConnectionAndSource() {
            @Override
            public void call(final AsyncConnectionSource source, final AsyncConnection connection, final Throwable t) {
                SingleResultCallback<AsyncBatchCursor<T>> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
                if (t != null) {
                    errHandlingCallback.onResult(null, t);
                } else {
                    final SingleResultCallback<AsyncBatchCursor<T>> wrappedCallback = releasingCallback(
                            errHandlingCallback, source, connection);
                    validateReadConcernAndCollation(source, connection, binding.getSessionContext().getReadConcern(), collation,
                            new AsyncCallableWithConnectionAndSource() {
                                @Override
                                public void call(final AsyncConnectionSource source, final AsyncConnection connection, final Throwable t) {
                                    if (t != null) {
                                        wrappedCallback.onResult(null, t);
                                    } else {
                                        executeWrappedCommandProtocolAsync(binding, namespace.getDatabaseName(),
                                                getCommand(binding.getSessionContext()), createCommandDecoder(),
                                                connection, asyncTransformer(connection.getDescription()), wrappedCallback);
                                    }
                                }
                    });
                }
            }
        });
    }

    private Codec<BsonDocument> createCommandDecoder() {
        return CommandResultDocumentCodec.create(decoder, VALUES);
    }

    private QueryResult<T> createQueryResult(final BsonDocument result, final ConnectionDescription description) {
        return new QueryResult<T>(namespace, BsonDocumentWrapperHelper.<T>toList(result, VALUES), 0L,
                description.getServerAddress());
    }

    private CommandTransformer<BsonDocument, BatchCursor<T>> transformer(final ConnectionSource source, final Connection connection) {
        return new CommandTransformer<BsonDocument, BatchCursor<T>>() {
            @Override
            public BatchCursor<T> apply(final BsonDocument result, final ServerAddress serverAddress) {
                QueryResult<T> queryResult = createQueryResult(result, connection.getDescription());
                return new QueryBatchCursor<T>(queryResult, 0, 0, decoder, source);
            }
        };
    }

    private CommandTransformer<BsonDocument, AsyncBatchCursor<T>> asyncTransformer(final ConnectionDescription connectionDescription) {
        return new CommandTransformer<BsonDocument, AsyncBatchCursor<T>>() {
            @Override
            public AsyncBatchCursor<T> apply(final BsonDocument result, final ServerAddress serverAddress) {
                QueryResult<T> queryResult = createQueryResult(result, connectionDescription);
                return new AsyncSingleBatchQueryCursor<T>(queryResult);
            }
        };
    }

    private CommandCreator getCommandCreator(final SessionContext sessionContext) {
        return new CommandCreator() {
            @Override
            public BsonDocument create(final ServerDescription serverDescription, final ConnectionDescription connectionDescription) {
                return getCommand(sessionContext);
            }
        };
    }

    private BsonDocument getCommand(final SessionContext sessionContext) {
        BsonDocument commandDocument = new BsonDocument("distinct", new BsonString(namespace.getCollectionName()));
        appendReadConcernToCommand(sessionContext, commandDocument);
        commandDocument.put("key", new BsonString(fieldName));
        putIfNotNullOrEmpty(commandDocument, "query", filter);
        putIfNotZero(commandDocument, "maxTimeMS", maxTimeMS);
        if (collation != null) {
            commandDocument.put("collation", collation.asDocument());
        }
        return commandDocument;
    }
}
