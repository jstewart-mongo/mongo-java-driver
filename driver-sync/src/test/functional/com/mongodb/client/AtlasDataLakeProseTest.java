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

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientException;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoException;
import com.mongodb.MongoNamespace;
import com.mongodb.client.test.CollectionHelper;
import org.bson.Document;
import org.bson.codecs.DocumentCodec;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.NoSuchElementException;

import static com.mongodb.ClusterFixture.getServerStatus;
import static com.mongodb.ClusterFixture.isStandalone;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.ClusterFixture.serverVersionLessThan;
import static com.mongodb.client.Fixture.getMongoClientSettingsBuilder;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

public class AtlasDataLakeProseTest {
    private static String databaseName = "test";
    private static String collectionName = "driverdata";
    private MongoNamespace namespace;
    private CollectionHelper<Document> collectionHelper;
    private ConnectionString connectionString = null;
    private MongoClient client;
    private MongoDatabase database;
    private MongoCollection<Document> collection;

    @Before
    public void setUp() {
        assumeTrue(canRunTests());
        namespace = new MongoNamespace(databaseName, collectionName);
        collectionHelper = new CollectionHelper<Document>(new DocumentCodec(), new MongoNamespace(databaseName, collectionName));
        connectionString = getADLConnectionString();

        MongoClientSettings.Builder builder = getMongoClientSettingsBuilder()
                .applyConnectionString(connectionString)
                .retryWrites(false)
                .retryReads(false);
        client = MongoClients.create(builder.build());

        database = client.getDatabase(databaseName);
        collection = database.getCollection(collectionName);
    }

    @Test(expected = NoSuchElementException.class)
    public void testKillCursorsOnAtlasDataLake() {
        List<Document> documents = asList(Document.parse("{a: 1, b: 2, c: 3}"), Document.parse("{a: 2, b: 3, c: 4}"),
                Document.parse("{a: 3, b: 4, c: 5}"));
        MongoCursor<Document> cursor = collection.find().batchSize(3).iterator();
        assertEquals(documents, asList(cursor.next(), cursor.next(), cursor.next()));
        collectionHelper.killCursor(namespace, cursor.getServerCursor());

        // should throw exception
        cursor.next();
    }

    private ConnectionString getADLConnectionString() {
        // NOTE: create a system property for this value
        return new ConnectionString("mongodb://mhuser:pencil@localhost");
    }

    private boolean canRunTests() {
        return isStandalone() && serverVersionAtLeast(4, 2);
    }
}
