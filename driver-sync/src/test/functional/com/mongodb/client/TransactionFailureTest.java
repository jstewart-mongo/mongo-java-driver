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

import com.mongodb.MongoClientException;
import org.bson.Document;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.mongodb.ClusterFixture.isSharded;
import static com.mongodb.ClusterFixture.serverVersionLessThan;
import static org.junit.Assume.assumeTrue;

public class TransactionFailureTest {
    private MongoClient mongoClient;
    private MongoDatabase database;
    private MongoCollection<Document> collection;

    public TransactionFailureTest() {
    }

    @BeforeClass
    public static void beforeClass() {
    }

    @AfterClass
    public static void afterClass() {
    }

    @Before
    public void setUp() {
        assumeTrue(canRunTests());

        mongoClient = MongoClients.create();
        database = mongoClient.getDatabase("testTransaction");
        collection = database.getCollection("test");
    }

    @After
    public void cleanUp() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    @Test(expected = MongoClientException.class)
    public void testTransactionFails() {
        ClientSession clientSession = mongoClient.startSession();
        try {
            clientSession.startTransaction();
            collection.insertOne(clientSession, Document.parse("{_id: 1, a: 1}"));
        } finally {
            clientSession.close();
        }
    }

    private boolean canRunTests() {
        return serverVersionLessThan("3.7.0")
                || (serverVersionLessThan("4.1.0") && isSharded());
    }
}
