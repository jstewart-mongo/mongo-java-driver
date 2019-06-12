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

import com.mongodb.MongoChangeStreamException;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.Before;
import org.junit.Test;

import static com.mongodb.ClusterFixture.isDiscoverableReplicaSet;
import static com.mongodb.ClusterFixture.serverVersionAtLeast;
import static com.mongodb.ClusterFixture.serverVersionLessThan;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

// See https://github.com/mongodb/specifications/tree/master/source/change-streams/tests/README.rst#prose-tests
public class ChangeStreamProseTest extends DatabaseTestCase {
    private BsonDocument failPointDocument;

    @Before
    @Override
    public void setUp() {
        assumeTrue(canRunTests());
        super.setUp();

        // create the collection before starting transactions
        collection.insertOne(Document.parse("{ _id : 0 }"));
    }

    //
    // Test that the ChangeStream continuously tracks the last seen resumeToken.
    //
    @Test
    public void testChangeStreamTracksResumeToken() {
        MongoCursor<ChangeStreamDocument<Document>> cursor = collection.watch().iterator();

        try {
            collection.insertOne(Document.parse("{x: 1}"));

            BsonDocument initialResumeToken = cursor.next().getResumeToken();
            assertNotNull(initialResumeToken);
        } finally {
            cursor.close();
        }
    }

    //
    // Test that the ChangeStream will throw an exception if the server response is missing the resume token (if wire version is < 8).
    //
    @Test
    public void testMissingResumeTokenThrowsException() {
        assumeTrue(serverVersionLessThan(asList(4, 0, 7)));
        boolean exceptionFound = false;

        MongoCursor<ChangeStreamDocument<Document>> cursor = collection.watch(asList(Aggregates.project(Document.parse("{ _id : 0 }"))))
                .iterator();
        try {
            collection.insertOne(Document.parse("{ x: 1 }"));
            assertNull(cursor.next());
        } catch (MongoChangeStreamException e) {
            exceptionFound = true;
        } finally {
            cursor.close();
        }
        assumeTrue(exceptionFound);
    }

    //
    // Test that the ChangeStream will automatically resume one time on a resumable error (including not master)
    // with the initial pipeline and options, except for the addition/update of a resumeToken.
    //
    @Test
    public void testResumeOneTimeOnError() {
        assumeTrue(serverVersionAtLeast(4, 0));
        MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = collection.watch().iterator();
        collection.insertOne(Document.parse("{ x: 1 }"));
        setFailPoint("getMore", 10107);
        try {
            assertNotNull(cursor.next());
        } finally {
            disableFailPoint();
            cursor.close();
        }
    }

    //
    // Test that ChangeStream will not attempt to resume on any error encountered while executing an aggregate command.
    //
    @Test
    public void testNoResumeForAggregateErrors() {
        boolean exceptionFound = false;
        MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = null;

        try {
            cursor = collection.watch(asList(Document.parse("{ $unsupportedStage: { _id : 0 } }"))).iterator();
        } catch (MongoCommandException e) {
            exceptionFound = true;
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
        assertTrue(exceptionFound);
    }

    //
    // ChangeStream will not attempt to resume after encountering error code 11601 (Interrupted), 136 (CappedPositionLost),
    // or 237 (CursorKilled) while executing a getMore command.
    //
    @Test
    public void testNoResumeErrors() {
        assumeTrue(serverVersionAtLeast(4, 0));
        MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = collection.watch().iterator();
        collection.insertOne(Document.parse("{ x: 1 }"));

        for (int errCode : asList(136, 237, 11601)) {
            try {
                setFailPoint("getMore", errCode);
                cursor.next();
            } catch (MongoException e) {
                assertEquals(errCode, e.getCode());
            } finally {
                disableFailPoint();
            }
        }
        cursor.close();
    }

    //
    // Ensure that a cursor returned from an aggregate command with a cursor id and an initial empty batch
    // is not closed on the driver side.
    //
    @Test
    public void testCursorNotClosed() {
        MongoCursor<ChangeStreamDocument<Document>> cursor = collection.watch().iterator();
        assertNotNull(cursor.getServerCursor());
        cursor.close();
    }

    @Test
    public void testGetResumeTokenFields() {
        assumeTrue(serverVersionLessThan(asList(4, 0, 7)));

        MongoCursor<ChangeStreamDocument<Document>> cursor = collection.watch().iterator();
        collection.insertOne(Document.parse("{ _id: 42, x: 1 }"));
        collection.insertOne(Document.parse("{ _id: 43, x: 1 }"));
        BsonDocument startAfterResumeToken = cursor.next().getResumeToken();
        BsonDocument expectedResumeToken = cursor.next().getResumeToken();
        cursor.close();

        cursor = collection.watch().resumeAfter(startAfterResumeToken).iterator();
        assertEquals(expectedResumeToken, cursor.next().getResumeToken());
        cursor.close();
    }

    //
    // For a ChangeStream under these conditions:
    //   Running against a server >=4.0.7.
    //   The batch is empty or has been iterated to the last document.
    // Expected result:
    //   getResumeToken must return the postBatchResumeToken from the current command response.
    //
    @Test
    public void testGetResumeTokenReturnsPostBatchResumeToken() {
        assumeTrue(serverVersionAtLeast(asList(4, 0, 7)));

        MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = collection.watch().iterator();
        collection.insertOne(Document.parse("{ _id: 42, x: 1 }"));
        BsonDocument resumeToken = cursor.next().getResumeToken();
        assertNotNull(resumeToken);
        assertEquals(resumeToken, cursor.getPostBatchResumeToken());
        cursor.close();
    }

    //
    // For a ChangeStream under these conditions:
    //   Running against a server <4.0.7.
    //   The batch is not empty.
    //   The batch hasn’t been iterated at all.
    //   The stream has iterated beyond a previous batch and a getMore command has just been executed.
    // Expected result:
    //   getResumeToken must return the _id of the previous document returned if one exists.
    //   getResumeToken must return resumeAfter from the initial aggregate if the option was specified.
    //   If the resumeAfter option was not specified, the getResumeToken result must be empty.
    //
    @Test
    public void testGetResumeTokenReturnsIdOfPreviousDocument() {
        assumeTrue(serverVersionLessThan(asList(4, 0, 7)));

        MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = collection.watch().iterator();
        assertNull(cursor.getResumeToken());
        cursor.close();

        cursor = collection.watch().iterator();
        collection.insertOne(Document.parse("{ _id: 42, x: 1 }"));
        cursor.next();
        BsonDocument resumeToken = cursor.getResumeToken();
        assertNotNull(resumeToken);

        collection.insertOne(Document.parse("{ _id: 43, x: 1 }"));
        cursor.next();
        assertNotNull(cursor.getResumeToken());
        assertNotEquals(resumeToken, cursor.getResumeToken());
        cursor.close();

        MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor2 = collection.watch().resumeAfter(resumeToken).iterator();
        assertEquals(resumeToken, cursor2.getResumeToken());
        cursor2.close();
    }

    //
    // For a ChangeStream under these conditions:
    //   The batch is not empty.
    //   The batch has been iterated up to but not including the last element.
    // Expected result:
    //   getResumeToken must return the _id of the previous document returned.
    //
    @Test
    public void testGetResumeTokenEqualsIdOfPreviousDocument() {
        MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = collection.watch().batchSize(3).iterator();
        collection.insertOne(Document.parse("{ _id: 42, x: 1 }"));
        collection.insertOne(Document.parse("{ _id: 43, x: 1 }"));
        collection.insertOne(Document.parse("{ _id: 44, x: 1 }"));
        cursor.next();
        cursor.next();
        try {
            assertEquals(cursor.getPostBatchResumeToken(), cursor.getResumeToken());
        } finally {
            cursor.close();
        }
    }

    private void setFailPoint(final String command, final int errCode) {
        failPointDocument = new BsonDocument("configureFailPoint", new BsonString("failCommand"))
                .append("mode", new BsonDocument("times", new BsonInt32(1)))
                .append("data", new BsonDocument("failCommands", new BsonArray(asList(new BsonString(command))))
                        .append("errorCode", new BsonInt32(errCode)));
        getCollectionHelper().runAdminCommand(failPointDocument);
    }

    private void disableFailPoint() {
        getCollectionHelper().runAdminCommand(failPointDocument.append("mode", new BsonString("off")));
    }

    private boolean canRunTests() {
        return isDiscoverableReplicaSet() && serverVersionAtLeast(3, 6);
    }
}
