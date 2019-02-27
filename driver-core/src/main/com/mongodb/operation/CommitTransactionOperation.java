/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.operation;

import com.mongodb.Function;
import com.mongodb.MongoException;
import com.mongodb.MongoNodeIsRecoveringException;
import com.mongodb.MongoNotPrimaryException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.WriteConcern;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.binding.AsyncWriteBinding;
import com.mongodb.binding.WriteBinding;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.operation.CommandOperationHelper.CommandCreator;
import org.bson.BsonDocument;
import org.bson.BsonInt32;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.MongoException.UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL;
import static java.util.Arrays.asList;

/**
 * An operation that commits a transaction.
 *
 * @since 3.8
 */
@Deprecated
public class CommitTransactionOperation extends TransactionOperation {
    private final boolean alreadyCommitted;
    private final BsonDocument recoveryToken;

    /**
     * Construct an instance.
     *
     * @param writeConcern the write concern
     */
    public CommitTransactionOperation(final WriteConcern writeConcern) {
        this(writeConcern, null, false);
    }

    /**
     * Construct an instance.
     *
     * @param writeConcern the write concern
     * @param alreadyCommitted if the transaction has already been committed.
     * @since 3.11
     */
    public CommitTransactionOperation(final WriteConcern writeConcern, final boolean alreadyCommitted) {
        this(writeConcern, null, alreadyCommitted);
    }

    /**
     * Construct an instance.
     *
     * @param writeConcern the write concern
     * @param recoveryToken the recovery token
     * @param alreadyCommitted if the transaction has already been committed.
     * @since 3.11
     */
    public CommitTransactionOperation(final WriteConcern writeConcern, final BsonDocument recoveryToken, final boolean alreadyCommitted) {
        super(writeConcern);
        this.recoveryToken = recoveryToken;
        this.alreadyCommitted = alreadyCommitted;
    }

    @Override
    public Void execute(final WriteBinding binding) {
        try {
            return super.execute(binding);
        } catch (MongoException e) {
            addErrorLabels(e);
            throw e;
        }
    }

    @Override
    public void executeAsync(final AsyncWriteBinding binding, final SingleResultCallback<Void> callback) {
        super.executeAsync(binding, new SingleResultCallback<Void>() {
            @Override
            public void onResult(final Void result, final Throwable t) {
                 if (t instanceof MongoException) {
                     addErrorLabels((MongoException) t);
                 }
                 callback.onResult(result, t);
            }
        });
    }

    private void addErrorLabels(final MongoException e) {
        if (shouldAddUnknownTransactionCommitResultLabel(e)) {
            e.addLabel(UNKNOWN_TRANSACTION_COMMIT_RESULT_LABEL);
        }
    }

    private static final List<Integer> NON_RETRYABLE_WRITE_CONCERN_ERROR_CODES = asList(79, 100);

    static boolean shouldAddUnknownTransactionCommitResultLabel(final Throwable t) {
        if (!(t instanceof MongoException)) {
            return false;
        }

        MongoException e = (MongoException) t;

        if (e instanceof MongoSocketException || e instanceof MongoTimeoutException
                || e instanceof MongoNotPrimaryException || e instanceof MongoNodeIsRecoveringException) {
            return true;
        }

        if (e instanceof MongoWriteConcernException) {
            return !NON_RETRYABLE_WRITE_CONCERN_ERROR_CODES.contains(e.getCode());
        }

        return false;
    }


    @Override
    protected String getCommandName() {
        return "commitTransaction";
    }

    @Override
    CommandCreator getCommandCreator() {
        if (alreadyCommitted) {
            final CommandCreator creator = super.getCommandCreator();
            return new CommandCreator() {
                @Override
                public BsonDocument create(final ServerDescription serverDescription, final ConnectionDescription connectionDescription) {
                    return getRetryCommandModifier().apply(creator.create(serverDescription, connectionDescription));
                }
            };
        } else {
            return getCommitCommandCreator();
        }
    }

    CommandCreator getCommitCommandCreator() {
        final WriteConcern writeConcern = super.getWriteConcern();
        return new CommandCreator() {
            @Override
            public BsonDocument create(final ServerDescription serverDescription, final ConnectionDescription connectionDescription) {
                BsonDocument command = new BsonDocument(getCommandName(), new BsonInt32(1));
                if (!writeConcern.isServerDefault()) {
                    command.put("writeConcern", writeConcern.asDocument());
                }
                if (recoveryToken != null) {
                    command.put("recoveryToken", recoveryToken);
                }
                return command;
            }
        };
    }

    @Override
    protected Function<BsonDocument, BsonDocument> getRetryCommandModifier() {
        return new Function<BsonDocument, BsonDocument>() {
            @Override
            public BsonDocument apply(final BsonDocument command) {
                WriteConcern retryWriteConcern = getWriteConcern().withW("majority");
                if (retryWriteConcern.getWTimeout(TimeUnit.MILLISECONDS) == null) {
                    retryWriteConcern = retryWriteConcern.withWTimeout(10000, TimeUnit.MILLISECONDS);
                }
                command.put("writeConcern", retryWriteConcern.asDocument());
                if (recoveryToken != null) {
                    command.put("recoveryToken", recoveryToken);
                }
                return command;
            }
        };
    }
}
