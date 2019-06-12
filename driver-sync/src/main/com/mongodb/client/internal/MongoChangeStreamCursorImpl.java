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

package com.mongodb.client.internal;

import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.lang.Nullable;
import com.mongodb.operation.BatchCursor;
import org.bson.BsonDocument;

public class MongoChangeStreamCursorImpl<T> extends MongoBatchCursorAdapter<T> implements MongoChangeStreamCursor<T> {

    public MongoChangeStreamCursorImpl(final BatchCursor<T> batchCursor) {
        super(batchCursor);
    }

    /**
     * Returns the postBatchResumeToken. For testing purposes only.
     *
     * @return the postBatchResumeToken, which can be null.
     */
    @Nullable
    public BsonDocument getPostBatchResumeToken() {
        return getBatchCursor().getPostBatchResumeToken();
    }

    /**
     * Returns the resume token.
     *
     * @return the resume token, which can be null.
     */
    @Nullable
    public BsonDocument getResumeToken() {
        return getBatchCursor().getResumeToken();
    }
}
