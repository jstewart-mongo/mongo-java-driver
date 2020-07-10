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

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoNamespace;
import com.mongodb.client.test.CollectionHelper;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import util.JsonPoweredTestHelper;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static com.mongodb.JsonTestServerVersionChecker.skipTest;
import static com.mongodb.client.Fixture.getDefaultDatabaseName;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static org.junit.Assume.assumeFalse;

// See https://github.com/mongodb/specifications/tree/master/source/transactions/tests
@RunWith(Parameterized.class)
public class AtlasDataLakeTest extends AbstractUnifiedTest {
    public AtlasDataLakeTest(final String filename, final String description, final String databaseName, final String collectionName,
                             final BsonArray data, final BsonDocument definition, final boolean skipTest) {
        super(filename, description, databaseName, collectionName, data, definition, skipTest);
    }

    @Before
    @Override
    public void setUp() {
        assumeFalse(getSkipTest());

        setCollectionHelper(new CollectionHelper<Document>(new DocumentCodec(),
                new MongoNamespace(getDatabaseName(), getCollectionName())));
        setConnectionString(getADLConnectionString());

        ConnectionString connectionString = getConnectionString();
        MongoClientSettings.Builder builder = getMongoClientSettingsBuilder()
                .applyConnectionString(connectionString)
                .addCommandListener(getCommandListener())
                .retryWrites(false)
                .retryReads(false);
        setMongoClient(createMongoClient(builder.build()));

        setDatabase(getMongoClient().getDatabase(getDatabaseName()));
        setTestHelper(new JsonPoweredCrudTestHelper(getDescription(), getDatabase(),
                getDatabase().getCollection(getCollectionName(), BsonDocument.class), null, getMongoClient()));
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

    @Override
    protected MongoClient createMongoClient(final MongoClientSettings settings) {
        return MongoClients.create(settings);
    }

    private ConnectionString getADLConnectionString() {
        // NOTE: create a system property for this value
        return new ConnectionString("mongodb://mhuser:pencil@localhost");
    }
}
