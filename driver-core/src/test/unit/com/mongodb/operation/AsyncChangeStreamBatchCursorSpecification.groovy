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

package com.mongodb.operation

import com.mongodb.MongoException
import com.mongodb.async.SingleResultCallback
import com.mongodb.binding.AsyncReadBinding
import spock.lang.Specification

class AsyncChangeStreamBatchCursorSpecification extends Specification {

    def 'should call the underlying AsyncQueryBatchCursor'() {
        given:
        def changeStreamOperation = Stub(ChangeStreamOperation)
        def binding = Mock(AsyncReadBinding) {
            getCount() >>> [2, 1, 0]
        }
        def wrapped = Mock(AsyncQueryBatchCursor)
        def callback = Stub(SingleResultCallback)
        def cursor = new AsyncChangeStreamBatchCursor(changeStreamOperation, wrapped, binding, null)

        when:
        cursor.setBatchSize(10)

        then:
        1 * wrapped.setBatchSize(10)

        when:
        cursor.tryNext(callback)

        then:
        1 * wrapped.tryNext(_) >> { it[0].onResult(null, null) }
        1 * binding.retain()
        1 * binding.release()

        when:
        cursor.next(callback)

        then:
        1 * wrapped.next(_) >> { it[0].onResult(null, null) }
        1 * binding.retain()
        1 * binding.release()

        when:
        cursor.next(callback)

        then:
        1 * wrapped.next(_) >> { it[0].onResult(null, new MongoException(11601, 'Failure')) }
        1 * binding.retain()
        1 * binding.release()

        when:
        cursor.retain()
        cursor.close()

        then:
        1 * wrapped.isClosed() >> {
            false
        }
        0 * wrapped.close()
        0 * binding.release()

        when:
        cursor.release()
        cursor.close()

        then:
        1 * wrapped.isClosed() >> {
            false
        }
        1 * wrapped.close()
        2 * binding.release()

        when:
        cursor.close()

        then:
        1 * wrapped.isClosed() >> {
            true
        }
        0 * wrapped.close()
        0 * binding.release()
    }
}
