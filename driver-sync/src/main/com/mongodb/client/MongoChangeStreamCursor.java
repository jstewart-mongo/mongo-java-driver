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

package com.mongodb.client;

import com.mongodb.lang.Nullable;
import org.bson.BsonDocument;

/**
 * The Mongo Cursor interface for change streams implementing the iterator protocol.
 * <p>
 * An application should ensure that a cursor is closed in all circumstances, e.g. using a try-with-resources statement:
 *
 * <blockquote><pre>
 * try (MongoChangeStreamCursor&lt;Document&gt; cursor = collection.find().iterator()) {
 *     while (cursor.hasNext()) {
 *         System.out.println(cursor.next());
 *     }
 * }
 * </pre></blockquote>
 *
 * @since 3.11
 * @param <TResult> The type of documents the cursor contains
 */

public interface MongoChangeStreamCursor<TResult> extends MongoCursor<TResult> {
    /**
     * Returns the postBatchResumeToken. For testing purposes only.
     *
     * @return the postBatchResumeToken, which can be null.
     */
    @Nullable
    BsonDocument getPostBatchResumeToken();

    /**
     * Returns the resume token.
     *
     * @return the resume token, which can be null.
     */
    @Nullable
    BsonDocument getResumeToken();
}
