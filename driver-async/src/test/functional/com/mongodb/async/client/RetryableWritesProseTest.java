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

import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.MongoException;

import com.mongodb.ClusterFixture;
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
import org.junit.Ignore;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.async.client.Fixture.getMongoClientSettingsBuilder;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assume.assumeTrue;

import static com.mongodb.ClusterFixture.getDefaultDatabaseName;

// See https://github.com/mongodb/specifications/tree/master/source/retryable-writes/tests/README.rst#replica-set-failover-test
public class RetryableWritesProseTest extends DatabaseTestCase {
    private MongoClient clientUnderTest;
    private MongoClient failPointClient;
    private MongoClient stepDownClient;
    private MongoDatabase clientDatabase;
    private MongoCollection<Document> clientCollection;
    private boolean notMasterErrorFound = false;
    private ServerAddress originalPrimary = null;
    private FutureResultCallback<Document> stepDownCallback = new FutureResultCallback<Document>();

    private static final TestCommandListener COMMAND_LISTENER = new TestCommandListener();
    private static final String DATABASE_NAME = getDefaultDatabaseName();
    private static final String COLLECTION_NAME = "RetryableWritesProseTest";

    @Before
    @Override
    public void setUp() {
        assumeTrue(canRunTests());

        clientUnderTest = MongoClients.create(getMongoClientSettingsBuilder()
                .addCommandListener(COMMAND_LISTENER)
                .retryWrites(true)
                .applyToServerSettings(new Block<ServerSettings.Builder>() {
                    @Override
                    public void apply(final ServerSettings.Builder builder) {
                        builder.heartbeatFrequency(60000, TimeUnit.MILLISECONDS);
                    }
                })
                .build());

        failPointClient = MongoClients.create(getMongoClientSettingsBuilder().build());
        stepDownClient = MongoClients.create(getMongoClientSettingsBuilder().build());

        originalPrimary = ClusterFixture.getPrimary();

        clientDatabase = clientUnderTest.getDatabase(DATABASE_NAME);
        clientCollection = clientDatabase.getCollection(COLLECTION_NAME);
    }

    @After
    public void cleanUp() {
        if (failPointClient != null) {
            failPointClient.close();

            failPointClient = getClientFromStepdownNode();
            MongoDatabase failPointAdminDb = failPointClient.getDatabase("admin");
            FutureResultCallback<Document> futureResultCallback = new FutureResultCallback<Document>();
            failPointAdminDb.runCommand(
                    Document.parse("{ configureFailPoint : 'stepdownHangBeforePerformingPostMemberStateUpdateActions', mode : 'off' }"),
                    futureResultCallback);
            futureResult(futureResultCallback);

            try {
                futureResult(stepDownCallback);
            } catch (MongoException e) {
            }

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
    public void testRetryableWriteOnFailover() {
        insertDocument();
        assertFalse(notMasterErrorFound);

        activateFailPoint();
        stepDownPrimary();
        insertDocument();
        assertTrue(notMasterErrorFound);
    }

    private boolean canRunTests() {
        return serverVersionAtLeast(3, 6) && isDiscoverableReplicaSet();
    }

    private void activateFailPoint() {
        MongoDatabase adminDB = failPointClient.getDatabase("admin");
        FutureResultCallback<Document> futureResultCallback = new FutureResultCallback<Document>();
        String document = "{ configureFailPoint : 'stepdownHangBeforePerformingPostMemberStateUpdateActions',"
                + " mode : 'alwaysOn' }";
        adminDB.runCommand(Document.parse(document), futureResultCallback);
        futureResult(futureResultCallback);
    }

    private void stepDownPrimary() {
        final MongoDatabase stepDownDB = stepDownClient.getDatabase("admin");

        stepDownDB.runCommand(Document.parse("{ replSetStepDown: 60, force: true}"), stepDownCallback);

        // Wait for the primary to step down.
        waitForPrimaryStepdown();
    }

    private void waitForPrimaryStepdown() {
        MongoClient primaryClient = getClientFromStepdownNode();
        MongoDatabase primaryDatabase = primaryClient.getDatabase("admin");
        while (!isSecondary(primaryDatabase)) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ex) {
            }
        }
        primaryClient.close();
    }

    private boolean isSecondary(MongoDatabase database) {
        FutureResultCallback<Document> waitCallback = new FutureResultCallback<Document>();
        database.runCommand(new BasicDBObject("isMaster", 1), waitCallback);
        try {
            return waitCallback.get().getBoolean("secondary");
        } catch (InterruptedException e) {
        }
        return false;
    }

    private void insertDocument() {
        FutureResultCallback<Void> futureResultCallback = new FutureResultCallback<Void>();

        // Reset the list of events in the command listener to track just the upcoming insert events.
        COMMAND_LISTENER.reset();

        try {
            clientCollection.insertOne(new Document("x", 22), futureResultCallback);
            futureResult(futureResultCallback);
        } catch (MongoException ex) {
            checkNotMasterFound(COMMAND_LISTENER.getEvents());
            System.out.println("--- notMasterErrorFound: " + notMasterErrorFound);
            throw ex;
        }

        checkNotMasterFound(COMMAND_LISTENER.getEvents());
    }

    private void checkNotMasterFound(List<CommandEvent> events) {
        for (int i = 0; i < events.size(); i++) {
            CommandEvent event = events.get(i);

            if (event instanceof CommandFailedEvent) {
                MongoException ex = MongoException.fromThrowable(((CommandFailedEvent) event).getThrowable());
                if (ex.getCode() == 10107) {
                    notMasterErrorFound = true;
                }
            }
        }
    }

    private MongoClient getClientFromStepdownNode() {
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
            return callback.get(60, TimeUnit.SECONDS);
        } catch (Throwable t) {
            throw new MongoException("FutureResultCallback failed", t);
        }
    }
}
