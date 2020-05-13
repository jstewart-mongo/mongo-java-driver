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

package com.mongodb.internal.connection

import com.mongodb.OperationFunctionalSpecification
import com.mongodb.ServerAddress
import com.mongodb.Tag
import com.mongodb.TagSet
import com.mongodb.connection.ServerDescription
import com.mongodb.connection.ServerType
import org.bson.types.ObjectId

import static com.mongodb.connection.ServerConnectionState.CONNECTED
import static com.mongodb.connection.ServerConnectionState.CONNECTING
import static com.mongodb.connection.ServerDescription.builder
import static com.mongodb.internal.connection.ServerDescriptionChangeEventHelper.shouldPublishChangeEvent
import static java.util.Arrays.asList

class ServerDescriptionChangeEventHelperSpecification extends OperationFunctionalSpecification {

    def 'should log state change if significant properties have changed'() {
        given:
        ServerDescription.Builder builder = createBuilder()
        ServerDescription description = builder.build()
        ServerDescription otherDescription

        expect:
        !shouldPublishChangeEvent(description, builder.build())

        when:
        otherDescription = createBuilder().address(new ServerAddress('localhost:27018')).build();

        then:
        shouldPublishChangeEvent(description, otherDescription)

        when:
        otherDescription = createBuilder().type(ServerType.STANDALONE).build();

        then:
        shouldPublishChangeEvent(description, otherDescription)

        when:
        otherDescription = createBuilder().tagSet(null).build();

        then:
        shouldPublishChangeEvent(description, otherDescription)

        when:
        otherDescription = createBuilder().setName('test2').build();

        then:
        shouldPublishChangeEvent(description, otherDescription)

        when:
        otherDescription = createBuilder().primary('localhost:27018').build();

        then:
        shouldPublishChangeEvent(description, otherDescription)

        when:
        otherDescription = createBuilder().canonicalAddress('localhost:27018').build();

        then:
        shouldPublishChangeEvent(description, otherDescription)

        when:
        otherDescription = createBuilder().hosts(new HashSet<String>(asList('localhost:27018'))).build();

        then:
        shouldPublishChangeEvent(description, otherDescription)

        when:
        otherDescription = createBuilder().arbiters(new HashSet<String>(asList('localhost:27018'))).build();

        then:
        shouldPublishChangeEvent(description, otherDescription)

        when:
        otherDescription = createBuilder().passives(new HashSet<String>(asList('localhost:27018'))).build();

        then:
        shouldPublishChangeEvent(description, otherDescription)

        when:
        otherDescription = createBuilder().ok(false).build();

        then:
        shouldPublishChangeEvent(description, otherDescription)

        when:
        otherDescription = createBuilder().state(CONNECTING).build();

        then:
        shouldPublishChangeEvent(description, otherDescription)

        when:
        otherDescription = createBuilder().electionId(new ObjectId()).build();

        then:
        shouldPublishChangeEvent(description, otherDescription)

        when:
        otherDescription = createBuilder().setVersion(3).build();

        then:
        shouldPublishChangeEvent(description, otherDescription)

        // test exception state changes
        shouldPublishChangeEvent(createBuilder().exception(new IOException()).build(),
                createBuilder().exception(new RuntimeException()).build())
        shouldPublishChangeEvent(createBuilder().exception(new IOException('message one')).build(),
                createBuilder().exception(new IOException('message two')).build())
    }

    private static ServerDescription.Builder createBuilder() {
        builder().ok(true)
                .state(CONNECTED)
                .address(new ServerAddress())
                .type(ServerType.SHARD_ROUTER)
                .tagSet(new TagSet(asList(new Tag('dc', 'ny'))))
                .setName('test')
                .primary('localhost:27017')
                .canonicalAddress('localhost:27017')
                .hosts(new HashSet<String>(asList('localhost:27017', 'localhost:27018')))
                .passives(new HashSet<String>(asList('localhost:27019')))
                .arbiters(new HashSet<String>(asList('localhost:27020')))
                .electionId(new ObjectId('abcdabcdabcdabcdabcdabcd'))
                .setVersion(2)
    }
}
