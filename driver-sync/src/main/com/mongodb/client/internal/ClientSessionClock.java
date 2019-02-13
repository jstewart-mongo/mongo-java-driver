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

package com.mongodb.client.internal;

public final class ClientSessionClock implements Clock {
    public static final ClientSessionClock INSTANCE = new ClientSessionClock(0L);

    private long currentTime;

    private ClientSessionClock(final long millis) {
        currentTime = millis;
    }

    @Override
    public long now() {
        if (currentTime == 0L) {
            return System.currentTimeMillis();
        }
        return currentTime;
    }

    @Override
    public void setTime(final long millis) {
        currentTime = millis;
    }
}
