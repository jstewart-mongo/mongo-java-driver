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

import com.mongodb.Block;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.MongoServerException;

import com.mongodb.MongoSocketReadException;
import com.mongodb.connection.ServerSettings;
import com.mongodb.event.CommandEvent;
import com.mongodb.internal.connection.TestCommandListener;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.Document;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import static com.mongodb.ClusterFixture.getDefaultDatabaseName;

// See https://github.com/mongodb/specifications/tree/master/source/retryable-writes/tests/README.rst#replica-set-failover-test
public class RetryableWritesProseTest extends DatabaseTestCase {
    private MongoClient clientUnderTest;
    private MongoClient failPointClient;
    private MongoClient stepDownClient;
    private final TestCommandListener commandListener;
    private final String databaseName;
    private final String collectionName;
    private final String description;

    public RetryableWritesProseTest() {
        this.description = "Replica set failover test for retryable writes";
        this.databaseName = getDefaultDatabaseName();
        this.collectionName = "test";
        this.commandListener = new TestCommandListener();
    }

    @BeforeClass
    public static void beforeClass() {
    }

    @AfterClass
    public static void afterClass() {
    }

    @Before
    @Override
    public void setUp() {
        assumeTrue(canRunTests());

        setUpClientUnderTest();
        setUpFailPointClient();
        setUpStepDownClient();
    }

    @After
    public void cleanUp() {
        if (failPointClient != null) {
            MongoDatabase failPointAdminDB = failPointClient.getDatabase("admin");
            failPointAdminDB.runCommand(
                    new Document("configureFailPoint", "stepdownHangBeforePerformingPostMemberStateUpdateActions")
                    .append("mode", "off"));
        }

        if (clientUnderTest != null) {
            clientUnderTest.close();
        }
        if (stepDownClient != null) {
            stepDownClient.close();
        }
    }

    @Test
    public void shouldPassAllOutcomes() {
        // Using the client under test, insert a document and observe a successful write result.
        // This will ensure that initial discovery takes place.
        MongoCollection<Document> collection = insertInitialDocument();

        // Using the fail point client, activate the fail point by setting mode to "alwaysOn".
        activateFailPoint();

        // Using the step down client, step down the primary by executing the command { replSetStepDown: 60, force: true}.
        // This operation will hang so long as the fail point is activated. When the fail point is later deactivated,
        // the step down will complete and the primary's client connections will be dropped. At that point, any ensuing
        // network error should be ignored.
        MongoDatabase stepDownDB = stepDownPrimary();

        // Using the client under test, insert a document and observe a successful write result. The test MUST assert
        // that the insert command fails once against the stepped down node and is successfully retried on the newly
        // elected primary (after SDAM discovers the topology change). The test MAY use APM or another means to observe
        // both attempts.
        insertDocument(collection);
    }

    private boolean canRunTests() {
        return serverVersionAtLeast(3, 6) && isDiscoverableReplicaSet();
    }

    private void setUpClientUnderTest() {
        MongoClientSettings settings = getMongoClientSettingsBuilder()
                .addCommandListener(commandListener)
                .retryWrites(true)
                .applyToServerSettings(new Block<ServerSettings.Builder>() {
                    @Override
                    public void apply(final ServerSettings.Builder builder) {
                        builder.heartbeatFrequency(60000, TimeUnit.MILLISECONDS);
                    }
                })
                .build();

        clientUnderTest = MongoClients.create(settings);
        System.out.println("setUpClientUnderTest: success");
    }

    private void setUpFailPointClient() {
        failPointClient = MongoClients.create(getMongoClientSettingsBuilder().build());
        System.out.println("setUpFailPointClient: success");
    }

    private void setUpStepDownClient() {
        stepDownClient = MongoClients.create(getMongoClientSettingsBuilder().build());
        System.out.println("setUpStepDownClient: success");
    }

    private MongoCollection<Document> insertInitialDocument() {
        // Using the client under test, insert a document and observe a successful write result.
        // This will ensure that initial discovery takes place.
        MongoDatabase database = clientUnderTest.getDatabase(databaseName);
        MongoCollection<Document> collection = database.getCollection(collectionName);
        Document doc = new Document("_id", 1).append("x", 11);
        try {
            System.out.println("--- inserting initial document...");
            collection.insertOne(doc);
            System.out.println("--- DONE inserting initial document");
        } catch (MongoServerException ex) {
            fail("Initial insert failed");
        }

        return collection;
    }

    private void activateFailPoint() {
        MongoDatabase adminDB = failPointClient.getDatabase("admin");
        Document command =
                new Document("configureFailPoint", "stepdownHangBeforePerformingPostMemberStateUpdateActions")
                        .append("mode", "alwaysOn");
        System.out.println("--- activating fail point...");
        adminDB.runCommand(command);
        System.out.println("--- DONE activating fail point");
    }

    private MongoDatabase stepDownPrimary() {
        final MongoDatabase stepDownDB = stepDownClient.getDatabase("admin");
        System.out.println("--- stepping down primary...");
        Thread t = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    stepDownDB.runCommand(new Document("replSetStepDown", 60).append("force", true));
                } catch (MongoSocketReadException e) {
                } catch (IllegalStateException ex) {
                }
            }
        });
        t.start();

        return stepDownDB;
    }

    private void insertDocument(MongoCollection<Document> collection) {
        // Using the client under test, insert a document and observe a successful write result. The test MUST assert
        // that the insert command fails once against the stepped down node and is successfully retried on the newly
        // elected primary (after SDAM discovers the topology change). The test MAY use APM or another means to observe
        // both attempts.
        System.out.println("--- insert one doc after stepdown...");
        collection.insertOne(new Document("_id", 2).append("x", 22));
    }
}
