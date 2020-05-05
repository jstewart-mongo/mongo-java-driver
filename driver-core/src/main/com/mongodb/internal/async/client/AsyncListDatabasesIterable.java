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

package com.mongodb.internal.async.client;

import com.mongodb.lang.Nullable;
import org.bson.conversions.Bson;

import java.util.concurrent.TimeUnit;

/**
 * Iterable for ListDatabases.
 *
 * @param <T> The type of the result.
 */
public interface AsyncListDatabasesIterable<T> extends AsyncMongoIterable<T> {

    /**
     * Sets the maximum execution time on the server for this operation.
     *
     * @param maxTime  the max time
     * @param timeUnit the time unit, which may not be null
     * @return this
     * @mongodb.driver.manual reference/operator/meta/maxTimeMS/ Max Time
     */
    AsyncListDatabasesIterable<T> maxTime(long maxTime, TimeUnit timeUnit);

    /**
     * Sets the number of documents to return per batch.
     *
     * @param batchSize the batch size
     * @return this
     * @mongodb.driver.manual reference/method/cursor.batchSize/#cursor.batchSize Batch Size
     */
    AsyncListDatabasesIterable<T> batchSize(int batchSize);

    /**
     * Sets the query filter to apply to the returned database names.
     *
     * @param filter the filter, which may be null.
     * @return this
     * @since 3.6
     * @mongodb.server.release 3.4.2
     */
    AsyncListDatabasesIterable<T> filter(@Nullable Bson filter);

    /**
     * Sets the nameOnly flag that indicates whether the command should return just the database names or return the database names and
     * size information.
     *
     * @param nameOnly the nameOnly flag, which may be null
     * @return this
     * @since 3.6
     * @mongodb.server.release 3.4.3
     */
    AsyncListDatabasesIterable<T> nameOnly(@Nullable Boolean nameOnly);

    /**
     * Sets the authorizedDatabasesOnly flag that indicates whether the command should return just the databases which the user
     * is authorized to see.
     *
     * @param authorizedDatabasesOnly the authorizedDatabasesOnly flag, which may be null
     * @return this
     * @since 4.1
     * @mongodb.server.release 4.0
     */
    AsyncListDatabasesIterable<T> authorizedDatabasesOnly(@Nullable Boolean authorizedDatabasesOnly);
}
