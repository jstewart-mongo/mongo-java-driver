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

import com.mongodb.Function;
import com.mongodb.MongoChangeStreamException;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.binding.ReadBinding;
import org.bson.BsonDocument;
import org.bson.BsonTimestamp;
import org.bson.RawBsonDocument;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.operation.ChangeStreamBatchCursorHelper.isRetryableError;

final class ChangeStreamBatchCursor<T> implements BatchCursor<T> {
    private final ReadBinding binding;
    private final ChangeStreamOperation<T> changeStreamOperation;

    private BsonTimestamp initialStartAtOperationTime;
    private BatchCursor<RawBsonDocument> wrapped;
    private BsonDocument postBatchResumeToken;

    ChangeStreamBatchCursor(final ChangeStreamOperation<T> changeStreamOperation,
                            final BatchCursor<RawBsonDocument> wrapped,
                            final ReadBinding binding) {
        this.changeStreamOperation = changeStreamOperation;
        this.initialStartAtOperationTime = changeStreamOperation.getStartAtOperationTime();
        if (changeStreamOperation.getStartAfter() != null) {
            changeStreamOperation.resumeToken(changeStreamOperation.getStartAfter());
            changeStreamOperation.startAfter(null);
        }
        else if (changeStreamOperation.getResumeAfter() != null) {
            changeStreamOperation.resumeToken(changeStreamOperation.getResumeAfter());
            changeStreamOperation.resumeAfter(null);
        }
        if (changeStreamOperation.getResumeToken() == null) {
            changeStreamOperation.startOperationTimeForResume(binding.getSessionContext().getOperationTime());
            changeStreamOperation.resumeToken(wrapped.getPostBatchResumeToken());
        }
        this.wrapped = wrapped;
        this.binding = binding.retain();
        this.postBatchResumeToken = wrapped.getPostBatchResumeToken();
    }

    BatchCursor<RawBsonDocument> getWrapped() {
        return wrapped;
    }

    @Override
    public boolean hasNext() {
        return resumeableOperation(new Function<BatchCursor<RawBsonDocument>, Boolean>() {
            @Override
            public Boolean apply(final BatchCursor<RawBsonDocument> queryBatchCursor) {
                return queryBatchCursor.hasNext();
            }
        });
    }

    @Override
    public List<T> next() {
        return resumeableOperation(new Function<BatchCursor<RawBsonDocument>, List<T>>() {
            @Override
            public List<T> apply(final BatchCursor<RawBsonDocument> queryBatchCursor) {
                cachePostBatchResumeToken(queryBatchCursor);
                return convertResults(queryBatchCursor.next());
            }
        });
    }

    @Override
    public List<T> tryNext() {
        return resumeableOperation(new Function<BatchCursor<RawBsonDocument>, List<T>>() {
            @Override
            public List<T> apply(final BatchCursor<RawBsonDocument> queryBatchCursor) {
                cachePostBatchResumeToken(queryBatchCursor);
                return convertResults(queryBatchCursor.tryNext());
            }
        });
    }

    @Override
    public void close() {
        wrapped.close();
        binding.release();
    }

    @Override
    public void setBatchSize(final int batchSize) {
        wrapped.setBatchSize(batchSize);
    }

    @Override
    public int getBatchSize() {
        return wrapped.getBatchSize();
    }

    @Override
    public ServerCursor getServerCursor() {
        return wrapped.getServerCursor();
    }

    @Override
    public ServerAddress getServerAddress() {
        return wrapped.getServerAddress();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Not implemented!");
    }

    @Override
    public BsonDocument getPostBatchResumeToken() {
        return postBatchResumeToken;
    }

    @Override
    public BsonDocument getResumeToken() {
        return changeStreamOperation.getResumeToken();
    }

    private void cachePostBatchResumeToken(final BatchCursor<RawBsonDocument> queryBatchCursor) {
        if (queryBatchCursor.getPostBatchResumeToken() != null) {
            changeStreamOperation.resumeToken(queryBatchCursor.getPostBatchResumeToken());
            postBatchResumeToken = queryBatchCursor.getPostBatchResumeToken();
        }
    }

    private List<T> convertResults(final List<RawBsonDocument> rawDocuments) {
        List<T> results = null;
        if (rawDocuments != null) {
            results = new ArrayList<T>();
            for (RawBsonDocument rawDocument : rawDocuments) {
                if (!rawDocument.containsKey("_id")) {
                    throw new MongoChangeStreamException("Cannot provide resume functionality when the resume token is missing.");
                }
                results.add(rawDocument.decode(changeStreamOperation.getDecoder()));
            }
            cacheResumeToken(rawDocuments);
        }
        return results;
    }

    private void cacheResumeToken(final List<RawBsonDocument> rawDocuments) {
        RawBsonDocument lastDocument = rawDocuments.get(rawDocuments.size() - 1);
        if (lastDocument.containsKey("postBatchResumeToken")) {
            changeStreamOperation.resumeToken(lastDocument.getDocument("postBatchResumeToken"));
        } else {
            changeStreamOperation.resumeToken(lastDocument.getDocument("_id", null));
        }
    }

    <R> R resumeableOperation(final Function<BatchCursor<RawBsonDocument>, R> function) {
        while (true) {
            try {
                return function.apply(wrapped);
            } catch (Throwable t) {
                if (!isRetryableError(t)) {
                    throw MongoException.fromThrowableNonNull(t);
                }
            }
            wrapped.close();

            changeStreamOperation.startAfter(null);
            if (changeStreamOperation.getResumeToken() != null) {
                changeStreamOperation.startAtOperationTime(null);
                changeStreamOperation.resumeAfter(changeStreamOperation.getResumeToken());
            } else if (changeStreamOperation.getStartAtOperationTime() != null
                    && binding.getReadConnectionSource().getServerDescription().getMaxWireVersion() >= 7) {
                changeStreamOperation.resumeAfter(null);
                changeStreamOperation.startAtOperationTime(initialStartAtOperationTime);
            } else {
                changeStreamOperation.resumeAfter(null);
                changeStreamOperation.startAtOperationTime(null);
            }
            wrapped = ((ChangeStreamBatchCursor<T>) changeStreamOperation.execute(binding)).getWrapped();
            binding.release(); // release the new change stream batch cursor's reference to the binding
        }
    }
}
