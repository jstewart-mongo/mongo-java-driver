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

import com.mongodb.ClientSessionOptions;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.WriteConcern;
import com.mongodb.client.test.CollectionHelper;
import com.mongodb.connection.ServerVersion;
import com.mongodb.lang.Nullable;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.BsonDocumentCodec;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import util.JsonPoweredTestHelper;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.mongodb.ClusterFixture.isSharded;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

@RunWith(Parameterized.class)
public class PinMongosTest extends DatabaseTestCase {
    private final String filename;
    private final String description;
    private final BsonArray data;
    private final BsonDocument definition;
    private MongoClient mongoClient;
    private CollectionHelper<Document> collectionHelper;
    private MongoCollection<BsonDocument> collection;
    private JsonPoweredCrudTestHelper helper;
    private Map<String, ClientSession> sessionsMap;
    private ClientSession clientSession;
    private final MongoNamespace namespace;

    public PinMongosTest(final String filename, final String description, final BsonArray data,
                         final MongoNamespace namespace, final BsonDocument definition) {
        this.filename = filename;
        this.description = description;
        this.data = data;
        this.namespace = namespace;
        this.definition = definition;
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

        mongoClient = MongoClients.create(getMongoClientSettingsBuilder().build());

        CollectionHelper<BsonDocument> collectionHelper = new CollectionHelper<BsonDocument>(new BsonDocumentCodec(), namespace);
        collectionHelper.drop();
        collectionHelper.create();


        if (!data.isEmpty()) {
            List<BsonDocument> documents = new ArrayList<BsonDocument>();
            for (BsonValue document : data) {
                documents.add(document.asDocument());
            }
            collectionHelper.insertDocuments(documents, WriteConcern.MAJORITY);
        }


        ClientSession sessionZero = createSession();
        ClientSession sessionOne = createSession();

        sessionsMap = new HashMap<String, ClientSession>();
        sessionsMap.put("session0", sessionZero);
        sessionsMap.put("session1", sessionOne);
    }

    @After
    public void cleanUp() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    private void closeAllSessions() {
        for (final ClientSession cur : sessionsMap.values()) {
            cur.close();
        }
    }

    @Test
    public void shouldPassAllOutcomes() {
        try {
            for (BsonValue cur : definition.getArray("operations")) {
                BsonDocument operation = cur.asDocument();
                String operationName = operation.getString("name").getValue();
                BsonValue expectedResult = operation.get("result");
                try {
                    if (operationName.equals("startTransaction")) {
                        clientSession = sessionsMap.get(operation.getString("object").getValue());
                        nonNullClientSession(clientSession).startTransaction();
                    } else if (operationName.equals("commitTransaction")) {
                        nonNullClientSession(clientSession).commitTransaction();
                    } else if (operationName.equals("abortTransaction")) {
                        nonNullClientSession(clientSession).abortTransaction();
                    } else {
                        BsonDocument actualOutcome = createJsonPoweredCrudTestHelper(mongoClient, namespace)
                                .getOperationResults(operation, clientSession);
                        if (expectedResult != null) {
                            BsonValue actualResult = actualOutcome.get("result");

                            assertEquals("Expected operation result differs from actual", expectedResult, actualResult);
                        }
                    }
                    assertFalse(String.format("Expected error '%s' but none thrown for operation %s",
                            getErrorContainsField(expectedResult), operationName), hasErrorContainsField(expectedResult));
                    assertFalse(String.format("Expected error code '%s' but none thrown for operation %s",
                            getErrorCodeNameField(expectedResult), operationName), hasErrorCodeNameField(expectedResult));
                } catch (RuntimeException e) {
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
                    if (!passedAssertion) {
                        throw e;
                    }
                }
            }
        } finally {
            closeAllSessions();
        }
    }

    private JsonPoweredCrudTestHelper createJsonPoweredCrudTestHelper(final MongoClient localMongoClient, final MongoNamespace namespace) {
        return new JsonPoweredCrudTestHelper(description, localMongoClient.getDatabase(namespace.getDatabaseName()),
                localMongoClient.getDatabase(namespace.getDatabaseName()).getCollection(namespace.getCollectionName(), BsonDocument.class));
    }

    @Parameterized.Parameters(name = "{1}")
    public static Collection<Object[]> data() throws URISyntaxException, IOException {
        List<Object[]> data = new ArrayList<Object[]>();
        for (File file : JsonPoweredTestHelper.getTestFiles("/sharded-transactions")) {
            BsonDocument testDocument = JsonPoweredTestHelper.getTestDocument(file);
            MongoNamespace namespace = new MongoNamespace(testDocument.getString("database_name").getValue(),
                    testDocument.getString("collection_name").getValue());
            for (BsonValue test : testDocument.getArray("tests")) {
                data.add(new Object[]{file.getName(), test.asDocument().getString("description").getValue(),
                        testDocument.getArray("data"), namespace, test.asDocument()});
            }
        }
        return data;
    }

    private ClientSession createSession() {
        return mongoClient.startSession(ClientSessionOptions.builder().build());
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

    private boolean hasErrorLabelsContainField(final BsonValue expectedResult) {
        return hasErrorField(expectedResult, "errorLabelsContain");
    }

    private List<String> getErrorLabelsContainField(final BsonValue expectedResult) {
        return getListOfStringsFromBsonArrays(expectedResult.asDocument(), "errorLabelsContain");
    }

    private boolean hasErrorLabelsOmitField(final BsonValue expectedResult) {
        return hasErrorField(expectedResult, "errorLabelsOmit");
    }

    private List<String> getErrorLabelsOmitField(final BsonValue expectedResult) {
        return getListOfStringsFromBsonArrays(expectedResult.asDocument(), "errorLabelsOmit");
    }

    private List<String> getListOfStringsFromBsonArrays(final BsonDocument expectedResult, final String arrayFieldName) {
        List<String> errorLabelContainsList = new ArrayList<String>();
        for (BsonValue cur : expectedResult.asDocument().getArray(arrayFieldName)) {
            errorLabelContainsList.add(cur.asString().getValue());
        }
        return errorLabelContainsList;
    }

    private boolean hasErrorContainsField(final BsonValue expectedResult) {
        return hasErrorField(expectedResult, "errorContains");
    }

    private boolean hasErrorCodeNameField(final BsonValue expectedResult) {
        return hasErrorField(expectedResult, "errorCodeName");
    }

    private boolean hasErrorField(final BsonValue expectedResult, final String key) {
        return expectedResult != null && expectedResult.isDocument() && expectedResult.asDocument().containsKey(key);
    }

    private ClientSession nonNullClientSession(@Nullable final ClientSession clientSession) {
        if (clientSession == null) {
            throw new IllegalArgumentException("clientSession can't be null in this context");
        }
        return clientSession;
    }

    private boolean canRunTests() {
        return serverVersionAtLeast(asList(4, 1, 6)) && isSharded();
    }

    private ServerVersion getServerVersion(final String fieldName) {
        String[] versionStringArray = definition.getString(fieldName).getValue().split("\\.");
        return new ServerVersion(Integer.parseInt(versionStringArray[0]), Integer.parseInt(versionStringArray[1]));
    }
}
