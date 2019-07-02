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
import com.mongodb.ConnectionString;
import com.mongodb.MongoNamespace;
import com.mongodb.ReadConcern;
import com.mongodb.ReadConcernLevel;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.test.CollectionHelper;
import com.mongodb.connection.SocketSettings;
import com.mongodb.connection.SslSettings;
import com.mongodb.event.CommandEvent;
import com.mongodb.internal.connection.TestCommandListener;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import util.JsonPoweredTestHelper;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.mongodb.ClusterFixture.getDefaultDatabaseName;
import static com.mongodb.ClusterFixture.isSharded;
import static com.mongodb.JsonTestServerVersionChecker.skipTest;
import static com.mongodb.async.client.Fixture.getConnectionString;
import static com.mongodb.async.client.Fixture.getDefaultDatabase;
import static com.mongodb.async.client.Fixture.getStreamFactoryFactory;
import static com.mongodb.client.CommandMonitoringTestHelper.assertEventsEquality;
import static com.mongodb.client.CommandMonitoringTestHelper.getExpectedEvents;
import static org.junit.Assert.assertEquals;
import static org.junit.Assume.assumeFalse;

// See https://github.com/mongodb/specifications/tree/master/source/crud/tests
@RunWith(Parameterized.class)
public class CrudTest {
    private final String filename;
    private final String description;
    private final String databaseName;
    private final BsonArray data;
    private final BsonDocument definition;
    private final boolean skipTest;
    private ConnectionString connectionString;
    private CollectionHelper<Document> collectionHelper;
    private MongoClient mongoClient;
    private MongoCollection<BsonDocument> collection;
    private JsonPoweredCrudTestHelper helper;
    private final TestCommandListener commandListener;

    private static final long MIN_HEARTBEAT_FREQUENCY_MS = 50L;

    public CrudTest(final String filename, final String description, final String databaseName,
                    final BsonArray data, final BsonDocument definition, final boolean skipTest) {
        this.filename = filename;
        this.description = description;
        this.databaseName = databaseName;
        this.data = data;
        this.definition = definition;
        this.skipTest = skipTest;
        this.commandListener = new TestCommandListener();
    }

    @Before
    public void setUp() {
        assumeFalse(skipTest);
        // No runOn syntax for legacy CRUD, so skipping these manually for now
        assumeFalse(isSharded() && description.startsWith("Aggregate with $currentOp"));

        String collectionName = "test";
        collectionHelper = new CollectionHelper<Document>(new DocumentCodec(), new MongoNamespace(databaseName, collectionName));

        collectionHelper.killAllSessions();
        collectionHelper.create(collectionName, new CreateCollectionOptions(), WriteConcern.MAJORITY);

        final BsonDocument clientOptions = definition.getDocument("clientOptions", new BsonDocument());

        connectionString = getConnectionString();
        MongoClientSettings.Builder builder = MongoClientSettings.builder().applyConnectionString(connectionString)
                .streamFactoryFactory(getStreamFactoryFactory());

        if (System.getProperty("java.version").startsWith("1.6.")) {
            builder.applyToSslSettings(new Block<SslSettings.Builder>() {
                @Override
                public void apply(final SslSettings.Builder builder) {
                    builder.invalidHostNameAllowed(true);
                }
            });
        }
        builder.addCommandListener(commandListener)
                .applyToSocketSettings(new Block<SocketSettings.Builder>() {
                    @Override
                    public void apply(final SocketSettings.Builder builder) {
                        builder.readTimeout(5, TimeUnit.SECONDS);
                    }
                })
                .writeConcern(getWriteConcern(clientOptions))
                .readConcern(getReadConcern(clientOptions))
                .readPreference(getReadPreference(clientOptions));

        mongoClient = MongoClients.create(builder.build());
        MongoDatabase database = mongoClient.getDatabase(databaseName);

        collection = database.getCollection(collectionName, BsonDocument.class);
        helper = new JsonPoweredCrudTestHelper(description, database, collection);
        if (!data.isEmpty()) {
            List<BsonDocument> documents = new ArrayList<BsonDocument>();
            for (BsonValue document : data) {
                documents.add(document.asDocument());
            }

            collectionHelper.drop();
            if (documents.size() > 0) {
                collectionHelper.insertDocuments(documents, WriteConcern.MAJORITY);
            }
        }
        commandListener.reset();
    }

    private ReadConcern getReadConcern(final BsonDocument clientOptions) {
        if (clientOptions.containsKey("readConcernLevel")) {
            return new ReadConcern(ReadConcernLevel.fromString(clientOptions.getString("readConcernLevel").getValue()));
        } else {
            return ReadConcern.DEFAULT;
        }
    }

    private WriteConcern getWriteConcern(final BsonDocument clientOptions) {
        if (clientOptions.containsKey("w")) {
            if (clientOptions.isNumber("w")) {
                return new WriteConcern(clientOptions.getNumber("w").intValue());
            } else {
                return new WriteConcern(clientOptions.getString("w").getValue());
            }
        } else {
            return WriteConcern.ACKNOWLEDGED;
        }
    }

    private ReadPreference getReadPreference(final BsonDocument clientOptions) {
        if (clientOptions.containsKey("readPreference")) {
            return ReadPreference.valueOf(clientOptions.getString("readPreference").getValue());
        } else {
            return ReadPreference.primary();
        }
    }

    @After
    public void cleanUp() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    @Test
    public void shouldPassAllOutcomes() {
        BsonDocument expectedOutcome = definition.getDocument("outcome");

        // check if v1 test
        if (definition.containsKey("operation")) {
            runOperation(expectedOutcome, helper.getOperationResults(definition.getDocument("operation")),
                    expectedOutcome.containsKey("result") && expectedOutcome.isDocument("result")
                            ? expectedOutcome.get("result").asDocument() : null);
        } else {  // v2 test
            BsonArray operations = definition.getArray("operations");
            for (BsonValue operation : operations) {
                runOperation(expectedOutcome, helper.getOperationResults(operation.asDocument()),
                        operation.asDocument().containsKey("result") ? operation.asDocument().getDocument("result") : null);
            }
        }
    }

    private void runOperation(final BsonDocument expectedOutcome, final BsonDocument outcome, final BsonDocument expectedResult) {
        if (expectedOutcome.containsKey("error")) {
            assertEquals("Expected error", expectedOutcome.getBoolean("error"), outcome.get("error"));
        }

        if (expectedResult != null) {
            // Hack to workaround the lack of upsertedCount
            BsonValue actualResult = outcome.get("result");
            if (actualResult.isDocument()
                    && actualResult.asDocument().containsKey("upsertedCount")
                    && actualResult.asDocument().getNumber("upsertedCount").intValue() == 0
                    && !expectedResult.asDocument().containsKey("upsertedCount")) {
                expectedResult.asDocument().append("upsertedCount", actualResult.asDocument().get("upsertedCount"));
            }
            // Hack to workaround the lack of insertedIds
            if (expectedResult.isDocument()
                    && !expectedResult.asDocument().containsKey("insertedIds")) {
                actualResult.asDocument().remove("insertedIds");
            }

            assertEquals(description, expectedResult, actualResult);
        }


        if (definition.containsKey("expectations")) {
            List<CommandEvent> expectedEvents = getExpectedEvents(definition.getArray("expectations"), databaseName, null);
            List<CommandEvent> events = commandListener.getCommandStartedEvents();

            assertEventsEquality(expectedEvents, events);
        }
        if (expectedOutcome.containsKey("collection")) {
            assertCollectionEquals(expectedOutcome.getDocument("collection"));
        }
    }

    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> data() throws URISyntaxException, IOException {
        List<Object[]> data = new ArrayList<Object[]>();
        for (File file : JsonPoweredTestHelper.getTestFiles("/crud")) {
            BsonDocument testDocument = JsonPoweredTestHelper.getTestDocument(file);
            for (BsonValue test : testDocument.getArray("tests")) {
                data.add(new Object[]{file.getName(), test.asDocument().getString("description").getValue(),
                        testDocument.getString("database_name", new BsonString(getDefaultDatabaseName())).getValue(),
                        testDocument.getArray("data"), test.asDocument(), skipTest(testDocument, test.asDocument())});
            }
        }
        return data;
    }

    private void assertCollectionEquals(final BsonDocument expectedCollection) {
        BsonArray actual = new MongoOperation<BsonArray>() {
            @Override
            public void execute() {
                MongoCollection<BsonDocument> collectionToCompare = collection;
                if (expectedCollection.containsKey("name")) {
                    collectionToCompare = getDefaultDatabase().getCollection(expectedCollection.getString("name").getValue(),
                            BsonDocument.class);
                }
                collectionToCompare.find().into(new BsonArray(), getCallback());
            }
        }.get();
        assertEquals(description, expectedCollection.getArray("data"), actual);
    }
}
