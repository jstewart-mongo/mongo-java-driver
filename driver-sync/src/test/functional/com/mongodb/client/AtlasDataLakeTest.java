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

package com.mongodb.client;

import com.mongodb.Block;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.WriteConcern;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.test.CollectionHelper;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.event.CommandEvent;
import com.mongodb.internal.connection.TestCommandListener;
import com.mongodb.internal.connection.TestConnectionPoolListener;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.BsonDocumentCodec;
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

import static com.mongodb.ClusterFixture.getConnectionString;
import static com.mongodb.JsonTestServerVersionChecker.skipTest;
import static com.mongodb.client.CommandMonitoringTestHelper.assertEventsEquality;
import static com.mongodb.client.CommandMonitoringTestHelper.getExpectedEvents;
import static com.mongodb.client.Fixture.getDefaultDatabaseName;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

// See https://github.com/mongodb/specifications/tree/master/source/transactions/tests
@RunWith(Parameterized.class)
public class AtlasDataLakeTest {
    private final String filename;
    private final String description;
    private final String databaseName;
    private final BsonArray data;
    private final BsonDocument definition;
    private final boolean skipTest;
    private JsonPoweredCrudTestHelper helper;
    private final TestCommandListener commandListener;
    private final TestConnectionPoolListener connectionPoolListener;
    private MongoClient mongoClient;
    private CollectionHelper<Document> collectionHelper;
    private ConnectionString connectionString = null;
    private final String collectionName;
    private MongoDatabase database;

    public AtlasDataLakeTest(final String filename, final String description, final String databaseName, final String collectionName,
                             final BsonArray data, final BsonDocument definition, final boolean skipTest) {
        this.filename = filename;
        this.description = description;
        this.databaseName = databaseName;
        this.collectionName = collectionName;
        this.data = data;
        this.definition = definition;
        this.commandListener = new TestCommandListener();
        this.connectionPoolListener = new TestConnectionPoolListener();
        this.skipTest = skipTest;
    }

    @Before
    public void setUp() {
        assumeFalse(skipTest);

        collectionHelper = new CollectionHelper<Document>(new DocumentCodec(), new MongoNamespace(databaseName, collectionName));
        connectionString = getADLConnectionString();

        MongoClientSettings.Builder builder = getMongoClientSettingsBuilder()
                .applyConnectionString(connectionString)
                .addCommandListener(commandListener)
                .retryWrites(false)
                .retryReads(false);
        mongoClient = createMongoClient(builder.build());

        database = mongoClient.getDatabase(databaseName);
        helper = new JsonPoweredCrudTestHelper(description, database, database.getCollection(collectionName, BsonDocument.class),
                null, mongoClient);
    }

    @After
    public void cleanUp() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    @Test
    public void shouldPassAllOutcomes() {
        executeOperations(definition.getArray("operations"));

        if (definition.containsKey("expectations")) {
            List<CommandEvent> expectedEvents = getExpectedEvents(definition.getArray("expectations"), databaseName, null);
            List<CommandEvent> events = commandListener.getCommandStartedEvents();

            assertEventsEquality(expectedEvents, events.subList(0, expectedEvents.size()), null);
        }

        BsonDocument expectedOutcome = definition.getDocument("outcome", new BsonDocument());
        if (expectedOutcome.containsKey("collection")) {
            BsonDocument collectionDocument = expectedOutcome.getDocument("collection");
            List<BsonDocument> collectionData;
            if (collectionDocument.containsKey("name")) {
                collectionData = new CollectionHelper<Document>(new DocumentCodec(),
                        new MongoNamespace(databaseName, collectionDocument.getString("name").getValue()))
                        .find(new BsonDocumentCodec());
            } else {
                collectionData = collectionHelper.find(new BsonDocumentCodec());
            }
            assertEquals(expectedOutcome.getDocument("collection").getArray("data").getValues(), collectionData);
        }
    }

    private void executeOperations(final BsonArray operations) {
        for (BsonValue cur : operations) {
            final BsonDocument operation = cur.asDocument();
            String operationName = operation.getString("name").getValue();
            BsonValue expectedResult = operation.get("result");
            String receiver = operation.getString("object").getValue();

            try {
                BsonDocument actualOutcome = helper.getOperationResults(operation, null);
                if (expectedResult != null) {
                    BsonValue actualResult = actualOutcome.get("result");
                    if (actualResult.isDocument()) {
                        if (((BsonDocument) actualResult).containsKey("recoveryToken")) {
                            ((BsonDocument) actualResult).remove("recoveryToken");
                        }
                    }

                    assertEquals("Expected operation result differs from actual", expectedResult, actualResult);
                }
                assertFalse(String.format("Expected error '%s' but none thrown for operation %s",
                        getErrorContainsField(expectedResult), operationName), hasErrorContainsField(expectedResult));
                assertFalse(String.format("Expected error code '%s' but none thrown for operation %s",
                        getErrorCodeNameField(expectedResult), operationName), hasErrorCodeNameField(expectedResult));
            } catch (RuntimeException e) {
                if (!assertExceptionState(e, expectedResult, operationName)) {
                    throw e;
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Parameterized.Parameters(name = "{0}: {1}")
    public static Collection<Object[]> data() throws URISyntaxException, IOException {
        List<Object[]> data = new ArrayList<Object[]>();
        for (File file : JsonPoweredTestHelper.getTestFiles("/atlas-data-lake")) {
            BsonDocument testDocument = JsonPoweredTestHelper.getTestDocument(file);
            for (BsonValue test : testDocument.getArray("tests")) {
                data.add(new Object[]{file.getName(), test.asDocument().getString("description").getValue(),
                        testDocument.getString("database_name", new BsonString(getDefaultDatabaseName())).getValue(),
                        testDocument.getString("collection_name", new BsonString("test")).getValue(),
                        testDocument.getArray("data", new BsonArray()), test.asDocument(), skipTest(testDocument, test.asDocument())});
            }
        }
        return data;
    }

    protected MongoClient createMongoClient(final MongoClientSettings settings) {
        return MongoClients.create(settings);
    }

    private boolean assertExceptionState(final RuntimeException e, final BsonValue expectedResult, final String operationName) {
        boolean passedAssertion = false;
        if (hasErrorLabelsContainField(expectedResult)) {
            if (e instanceof MongoException) {
                MongoException mongoException = (MongoException) e;
                for (String curErrorLabel : getErrorLabelsContainField(expectedResult)) {
                    assertTrue(String.format("Expected error label '%s but found labels '%s' for operation %s",
                            curErrorLabel, mongoException.getErrorLabels(), operationName),
                            mongoException.hasErrorLabel(curErrorLabel));
                }
                passedAssertion = true;
            }
        }
        if (hasErrorLabelsOmitField(expectedResult)) {
            if (e instanceof MongoException) {
                MongoException mongoException = (MongoException) e;
                for (String curErrorLabel : getErrorLabelsOmitField(expectedResult)) {
                    assertFalse(String.format("Expected error label '%s omitted but found labels '%s' for operation %s",
                            curErrorLabel, mongoException.getErrorLabels(), operationName),
                            mongoException.hasErrorLabel(curErrorLabel));
                }
                passedAssertion = true;
            }
        }
        if (hasErrorContainsField(expectedResult)) {
            String expectedError = getErrorContainsField(expectedResult);
            assertTrue(String.format("Expected '%s' but got '%s' for operation %s", expectedError, e.getMessage(),
                    operationName), e.getMessage().toLowerCase().contains(expectedError.toLowerCase()));
            passedAssertion = true;
        }
        if (hasErrorCodeNameField(expectedResult)) {
            String expectedErrorCodeName = getErrorCodeNameField(expectedResult);
            if (e instanceof MongoCommandException) {
                assertEquals(expectedErrorCodeName, ((MongoCommandException) e).getErrorCodeName());
                passedAssertion = true;
            } else if (e instanceof MongoWriteConcernException) {
                assertEquals(expectedErrorCodeName, ((MongoWriteConcernException) e).getWriteConcernError().getCodeName());
                passedAssertion = true;
            }
        }
        return passedAssertion;
    }

    private String getErrorContainsField(final BsonValue expectedResult) {
        return getErrorField(expectedResult, "errorContains");
    }

    private String getErrorCodeNameField(final BsonValue expectedResult) {
        return getErrorField(expectedResult, "errorCodeName");
    }

    private String getErrorField(final BsonValue expectedResult, final String key) {
        if (hasErrorField(expectedResult, key)) {
            return expectedResult.asDocument().getString(key).getValue();
        } else {
            return "";
        }
    }

    private boolean hasErrorContainsField(final BsonValue expectedResult) {
        return hasErrorField(expectedResult, "errorContains");
    }

    private boolean hasErrorCodeNameField(final BsonValue expectedResult) {
        return hasErrorField(expectedResult, "errorCodeName");
    }

    private boolean hasErrorLabelsContainField(final BsonValue expectedResult) {
        return hasErrorField(expectedResult, "errorLabelsContain");
    }

    private boolean hasErrorField(final BsonValue expectedResult, final String key) {
        return expectedResult != null && expectedResult.isDocument() && expectedResult.asDocument().containsKey(key);
    }

    private List<String> getErrorLabelsContainField(final BsonValue expectedResult) {
        return getListOfStringsFromBsonArrays(expectedResult.asDocument(), "errorLabelsContain");
    }

    private List<String> getListOfStringsFromBsonArrays(final BsonDocument expectedResult, final String arrayFieldName) {
        List<String> errorLabelContainsList = new ArrayList<String>();
        for (BsonValue cur : expectedResult.asDocument().getArray(arrayFieldName)) {
            errorLabelContainsList.add(cur.asString().getValue());
        }
        return errorLabelContainsList;
    }

    private boolean hasErrorLabelsOmitField(final BsonValue expectedResult) {
        return hasErrorField(expectedResult, "errorLabelsOmit");
    }

    private List<String> getErrorLabelsOmitField(final BsonValue expectedResult) {
        return getListOfStringsFromBsonArrays(expectedResult.asDocument(), "errorLabelsOmit");
    }

    private ConnectionString getADLConnectionString() {
        // NOTE: create a system property for this value
        return new ConnectionString("mongodb://mhuser:pencil@localhost");
    }
}
