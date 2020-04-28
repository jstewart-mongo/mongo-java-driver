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

package com.mongodb.client.model;


import com.mongodb.CreateIndexCommitQuorum;

import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.notNull;

/**
 * The options to apply to the command when creating indexes.
 *
 * @mongodb.driver.manual reference/command/createIndexes Index options
 * @since 3.6
 */
public class CreateIndexOptions {
    private long maxTimeMS;
    private CreateIndexCommitQuorum commitQuorum;

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
    public CreateIndexOptions maxTime(final long maxTime, final TimeUnit timeUnit) {
        notNull("timeUnit", timeUnit);
        this.maxTimeMS = TimeUnit.MILLISECONDS.convert(maxTime, timeUnit);
        return this;
    }

    /**
     * Gets the create index commit quorum for this operation.
     *
     * @return the create index commit quorum
     */
    public CreateIndexCommitQuorum getCommitQuorum() {
        return commitQuorum;
    }

    /**
     * Sets the create index commit quorum for this operation.
     *
     * @param commitQuorum the create index commit quorum
     * @return this
     */
    public CreateIndexOptions commitQuorum(final CreateIndexCommitQuorum commitQuorum) {
        this.commitQuorum = commitQuorum;
        return this;
    }

    @Override
    public String toString() {
        return "CreateIndexOptions{"
                + "maxTimeMS=" + maxTimeMS
                + ", commitQuorum=" + commitQuorum
                + '}';
    }
}
