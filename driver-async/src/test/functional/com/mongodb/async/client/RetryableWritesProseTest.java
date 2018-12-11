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
import com.mongodb.MongoException;
import com.mongodb.MongoServerException;

import com.mongodb.async.SingleResultCallback;
import com.mongodb.connection.ServerSettings;
import org.bson.Document;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import static com.mongodb.ClusterFixture.getDefaultDatabaseName;
import static com.mongodb.async.client.Fixture.getMongoClientBuilderFromConnectionString;

// See https://github.com/mongodb/specifications/tree/master/source/retryable-writes/tests/README.rst#replica-set-failover-test
public class RetryableWritesProseTest extends DatabaseTestCase {
    private MongoClient clientUnderTest;
    private MongoClient failPointClient;
    private MongoClient stepDownClient;
    private final String databaseName;
    private final String collectionName;
    private final String description;

    public RetryableWritesProseTest() {
        this.description = "Replica set failover test for retryable writes";
        this.databaseName = getDefaultDatabaseName();
        this.collectionName = "test";
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
            Document command =
                    new Document("onPrimaryTransactionWrite", "off")
                            .append("mode", "off");
            failPointAdminDB.runCommand(command, new SingleResultCallback<Document>() {
                @Override
                public void onResult(final Document result, final Throwable t) {
                    System.out.println(result.toJson());
                }
            });
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
        MongoDatabase database = clientUnderTest.getDatabase(databaseName);
        MongoCollection<Document> collection = database.getCollection(collectionName);
        Document doc = new Document("_id", 1).append("x", 11);
        try {
            collection.insertOne(doc, new SingleResultCallback<Void>() {
                @Override
                public void onResult(final Void result, final Throwable t) {
                    System.out.println("Inserted!");
                }
            });
        } catch (MongoServerException ex) {
            fail("Initial insert failed");
        }

        // Using the fail point client, activate the fail point by setting mode to "alwaysOn".
        activateFailPoint();

        // Using the step down client, step down the primary by executing the command { replSetStepDown: 60, force: true}.
        // This operation will hang so long as the fail point is activated. When the fail point is later deactivated,
        // the step down will complete and the primary's client connections will be dropped. At that point, any ensuing
        // network error should be ignored.
        stepDownPrimary();

        // Using the client under test, insert a document and observe a successful write result. The test MUST assert
        // that the insert command fails once against the stepped down node and is successfully retried on the newly
        // elected primary (after SDAM discovers the topology change). The test MAY use APM or another means to observe
        // both attempts.
        try {
            collection.insertOne(new Document("_id", 2).append("x", 22), new SingleResultCallback<Void>() {
                @Override
                public void onResult(final Void result, final Throwable t) {
                    System.out.println("This insert should fail.");
                    if (t != null) {
                        System.out.println("Exception: " + t.getMessage());
                        throw new MongoException(t.getMessage());
                    }
                }
            });
            fail("Exception should have been raised on insert");
        } catch (MongoException ex) {
            System.out.println("Expected exception raised: " + ex.getMessage());
        }

        try {
            collection.insertOne(new Document("_id", 2).append("x", 22), new SingleResultCallback<Void>() {
                @Override
                public void onResult(final Void result, final Throwable t) {
                    System.out.println("This insert should succeed.");
                }
            });
        } catch (MongoException ex) {
            fail("Exception should not have been raised on insert: " + ex.getMessage());
        }
    }

    private boolean canRunTests() {
        return serverVersionAtLeast(3, 6) && isDiscoverableReplicaSet();
    }

    private void setUpClientUnderTest() {
        clientUnderTest = MongoClients.create(getMongoClientBuilderFromConnectionString()
                .retryWrites(true)
                .applyToServerSettings(new Block<ServerSettings.Builder>() {
                    @Override
                    public void apply(final ServerSettings.Builder builder) {
                        builder.heartbeatFrequency(60, TimeUnit.SECONDS);
                    }
                })
                .build());
    }

    private void setUpFailPointClient() {
        failPointClient = MongoClients.create(getMongoClientBuilderFromConnectionString().build());
    }

    private void setUpStepDownClient() {
        stepDownClient = MongoClients.create(getMongoClientBuilderFromConnectionString().build());
    }

    private void activateFailPoint() {
        MongoDatabase adminDB = failPointClient.getDatabase("admin");
        Document command =
                new Document("configureFailPoint", "stepdownHangBeforePerformingPostMemberStateUpdateActions")
                        .append("mode", "alwaysOn");
        adminDB.runCommand(command, new SingleResultCallback<Document>() {
            @Override
            public void onResult(final Document result, final Throwable t) {
                System.out.println("Executed command to activate fail point");
            }
        });
    }

    private void stepDownPrimary() {
        MongoDatabase stepDownDB = stepDownClient.getDatabase("admin");
        stepDownDB.runCommand(new Document("replSetStepDown", 60).append("force", true), new SingleResultCallback<Document>() {
            @Override
            public void onResult(final Document result, final Throwable t) {
                System.out.println("Executed command to step down primary");
            }
        });
    }
}
