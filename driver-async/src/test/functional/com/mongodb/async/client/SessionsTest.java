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

package com.mongodb.async.client;

import com.mongodb.event.CommandEvent;
import com.mongodb.event.CommandStartedEvent;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

// See https://github.com/mongodb/specifications/tree/master/source/sessions/tests
@RunWith(Parameterized.class)
public class SessionsTest extends AbstractTransactionsTest {
    public SessionsTest(final String filename, final String description, final BsonArray data, final BsonDocument definition,
                        final boolean skipTest) {
        super(filename, description, data, definition, skipTest);
    }

    @Override
    public boolean processExtendedTestOperation(final BsonDocument operation, final ClientSession clientSession) {
        String operationName = operation.getString("name").getValue();

        if (operationName.equals("assertDifferentLsidOnLastTwoCommands")) {
            List<CommandEvent> events = lastTwoCommandEvents();
            assertFalse(((CommandStartedEvent) events.get(0)).getCommand().getDocument("lsid").equals(
                    ((CommandStartedEvent) events.get(1)).getCommand().getDocument("lsid")));
        } else if (operationName.equals("assertSameLsidOnLastTwoCommands")) {
            List<CommandEvent> events = lastTwoCommandEvents();
            assertTrue(((CommandStartedEvent) events.get(0)).getCommand().getDocument("lsid").equals(
                    ((CommandStartedEvent) events.get(1)).getCommand().getDocument("lsid")));
        } else if (operationName.equals("assertSessionDirty")) {
            assertNotNull(clientSession);
            assertNotNull(clientSession.getServerSession());
            assertTrue(clientSession.getServerSession().isMarkedDirty());
        } else if (operationName.equals("assertSessionNotDirty")) {
            assertNotNull(clientSession);
            assertNotNull(clientSession.getServerSession());
            assertFalse(clientSession.getServerSession().isMarkedDirty());
        } else {
            return false;
        }
        return true;
    }

    private List<CommandEvent> lastTwoCommandEvents() {
        List<CommandEvent> events = getCommandListener().getCommandStartedEvents();
        assertTrue(events.size() >= 2);
        return events.subList(events.size() - 2, events.size());
    }

    @Parameterized.Parameters(name = "{0}: {1}")
    public static Collection<Object[]> data() throws URISyntaxException, IOException {
        List<Object[]> data = new ArrayList<Object[]>();
        for (File file : JsonPoweredTestHelper.getTestFiles("/sessions")) {
            BsonDocument testDocument = JsonPoweredTestHelper.getTestDocument(file);
            for (BsonValue test : testDocument.getArray("tests")) {
                data.add(new Object[]{file.getName(), test.asDocument().getString("description").getValue(),
                        testDocument.getArray("data"), test.asDocument(), skipTest(testDocument, test.asDocument())});
            }
        }
        return data;
    }
}
