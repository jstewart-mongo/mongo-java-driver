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

package com.mongodb.async.client;

import com.mongodb.Block;
import com.mongodb.ClusterFixture;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.async.FutureResultCallback;
import com.mongodb.connection.ClusterConnectionMode;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.ServerSettings;
import com.mongodb.event.CommandEvent;
import com.mongodb.event.CommandFailedEvent;
import com.mongodb.internal.connection.TestCommandListener;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ClusterFixture.getDefaultDatabaseName;
import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.async.client.Fixture.getMongoClientSettingsBuilder;
import static java.lang.String.format;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assume.assumeTrue;

// See https://github.com/mongodb/specifications/tree/master/source/retryable-writes/tests/README.rst#replica-set-failover-test
public class RetryableWritesProseTest extends DatabaseTestCase {
    private static final TestCommandListener COMMAND_LISTENER = new TestCommandListener();
    private static final String DATABASE_NAME = getDefaultDatabaseName();
    private static final String COLLECTION_NAME = "RetryableWritesProseTest";

    private MongoClient clientUnderTest;
    private MongoClient failPointClient;
    private MongoClient stepDownClient;
    private ServerAddress originalPrimary;

    @Before
    @Override
    public void setUp() {
        assumeTrue(serverVersionAtLeast(3, 6) && isDiscoverableReplicaSet());

        clientUnderTest = MongoClients.create(getMongoClientSettingsBuilder()
                .addCommandListener(COMMAND_LISTENER)
                .retryWrites(true)
                .applyToServerSettings(new Block<ServerSettings.Builder>() {
                    @Override
                    public void apply(final ServerSettings.Builder builder) {
                        builder.heartbeatFrequency(60, TimeUnit.SECONDS);
                    }
                })
                .applyToClusterSettings(new Block<ClusterSettings.Builder>() {
                    @Override
                    public void apply(final ClusterSettings.Builder builder) {
                        builder.serverSelectionTimeout(60, TimeUnit.SECONDS);
                    }
                })
                .build());

        originalPrimary = ClusterFixture.getPrimary();
        failPointClient = createMongoClientToTheOriginalPrimary();
        stepDownClient = MongoClients.create(getMongoClientSettingsBuilder().build());
    }

    @After
    public void cleanUp() {
        if (failPointClient != null) {
            failPointClient.close();

            failPointClient = createMongoClientToTheOriginalPrimary();
            configureFailPoint("off");
            failPointClient.close();
        }

        if (clientUnderTest != null) {
            clientUnderTest.close();
        }
        if (stepDownClient != null) {
            stepDownClient.close();
        }
    }

    /**
     * Test whether writes are retried in the event of a primary failover. This test proceeds as follows:
     *
     * Using the client under test, insert a document and observe a successful write result. This will ensure that
     * initial discovery takes place.
     *
     * Using the fail point client, activate the fail point by setting mode to "alwaysOn".
     *
     * Using the step down client, step down the primary by executing the command { replSetStepDown: 60, force: true}.
     * This operation will hang so long as the fail point is activated. When the fail point is later deactivated, the
     * step down will complete and the primary's client connections will be dropped. At that point, any ensuing network
     * error should be ignored.
     *
     * Using the client under test, insert a document and observe a successful write result. The test MUST assert that
     * the insert command fails once against the stepped down node and is successfully retried on the newly elected
     * primary (after SDAM discovers the topology change). The test MAY use APM or another means to observe both attempts.
     *
     * Using the fail point client, deactivate the fail point by setting mode to "off".
     */
    @Test
    public void testRetryableWriteOnFailover() throws InterruptedException {
        futureResult(insertDocument());
        assertFalse(checkMasterNotFound());

        configureFailPoint("alwaysOn");

        stepDownPrimary();

        FutureResultCallback<Void> insertCallback = insertDocument();
        Thread.sleep(5000);
        configureFailPoint("off");
        futureResult(insertCallback);

        assertTrue(checkMasterNotFound());
    }

    private void configureFailPoint(final String mode) {
        FutureResultCallback<Document> futureResultCallback = new FutureResultCallback<Document>();
        failPointClient.getDatabase("admin").runCommand(Document.parse(
                format("{ configureFailPoint : 'stepdownHangBeforePerformingPostMemberStateUpdateActions', mode : '%s' }", mode)),
                futureResultCallback);
        futureResult(futureResultCallback);
    }

    private void stepDownPrimary() {
        stepDownClient.getDatabase("admin")
                .runCommand(Document.parse("{ replSetStepDown: 60, force: true}"), new FutureResultCallback<Document>());

        while (!isSecondary()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
            }
        }
    }

    private boolean isSecondary() {
        FutureResultCallback<Document> waitCallback = new FutureResultCallback<Document>();
        failPointClient.getDatabase("admin").runCommand(new Document("isMaster", 1), waitCallback);
        try {
            return waitCallback.get().getBoolean("secondary");
        } catch (InterruptedException e) {
        }
        return false;
    }

    private FutureResultCallback<Void> insertDocument() {
        FutureResultCallback<Void> insertCallback = new FutureResultCallback<Void>();
        clientUnderTest.getDatabase(DATABASE_NAME).getCollection(COLLECTION_NAME).insertOne(new Document(), insertCallback);
        return insertCallback;
    }

    private boolean checkMasterNotFound() {
        List<CommandEvent> events = COMMAND_LISTENER.getEvents();
        for (CommandEvent event : events) {
            if (event instanceof CommandFailedEvent) {
                MongoException ex = MongoException.fromThrowable(((CommandFailedEvent) event).getThrowable());
                if (ex.getCode() == 10107) {
                    return true;
                }
            }
        }
        return false;
    }

    private MongoClient createMongoClientToTheOriginalPrimary() {
        return MongoClients.create(getMongoClientSettingsBuilder()
                .applyToClusterSettings(new Block<ClusterSettings.Builder>() {
                    @Override
                    public void apply(final ClusterSettings.Builder builder) {
                        builder.mode(ClusterConnectionMode.SINGLE)
                                .hosts(Collections.singletonList(originalPrimary));
                    }
                }).build());
    }

    <T> T futureResult(final FutureResultCallback<T> callback) {
        try {
            return callback.get(70, TimeUnit.SECONDS);
        } catch (Throwable t) {
            throw new MongoException("FutureResultCallback failed: " + t.getMessage(), t);
        }
    }
}
