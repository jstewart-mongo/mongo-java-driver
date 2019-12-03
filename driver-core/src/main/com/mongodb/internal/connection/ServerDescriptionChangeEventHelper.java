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

package com.mongodb.internal.connection;

import com.mongodb.connection.ServerDescription;

final class ServerDescriptionChangeEventHelper {
    /*
     * Returns true if this instance can be published for SDAM change events.
     */
    static boolean shouldPublishChangeEvent(final ServerDescription currentDescription,
                                            final ServerDescription previousDescription) {
        if (currentDescription == null) {
            return previousDescription == null;
        }
        if (previousDescription == null) {
            return true;
        }
        if (currentDescription.isOk() != previousDescription.isOk()) {
            return true;
        }
        if (!currentDescription.getAddress().equals(previousDescription.getAddress())) {
            return true;
        }
        if (currentDescription.getCanonicalAddress() != null
                ? !currentDescription.getCanonicalAddress().equals(previousDescription.getCanonicalAddress())
                : previousDescription.getCanonicalAddress() != null) {
            return true;
        }
        if (!currentDescription.getHosts().equals(previousDescription.getHosts())) {
            return true;
        }
        if (!currentDescription.getArbiters().equals(previousDescription.getArbiters())) {
            return true;
        }
        if (!currentDescription.getPassives().equals(previousDescription.getPassives())) {
            return true;
        }
        if (currentDescription.getPrimary() != null ? !currentDescription.getPrimary().equals(previousDescription.getPrimary())
                : previousDescription.getPrimary() != null) {
            return true;
        }
        if (currentDescription.getSetName() != null ? !currentDescription.getSetName().equals(previousDescription.getSetName())
                : previousDescription.getSetName() != null) {
            return true;
        }
        if (currentDescription.getState() != previousDescription.getState()) {
            return true;
        }
        if (!currentDescription.getTagSet().equals(previousDescription.getTagSet())) {
            return true;
        }
        if (currentDescription.getType() != previousDescription.getType()) {
            return true;
        }
        if (!currentDescription.getVersion().equals(previousDescription.getVersion())) {
            return true;
        }
        if (currentDescription.getElectionId() != null ? !currentDescription.getElectionId().equals(previousDescription.getElectionId())
                : previousDescription.getElectionId() != null) {
            return true;
        }
        if (currentDescription.getSetVersion() != null ? !currentDescription.getSetVersion().equals(previousDescription.getSetVersion())
                : previousDescription.getSetVersion() != null) {
            return true;
        }

        // Compare class equality and message as exceptions rarely override equals
        Class<?> thisExceptionClass = currentDescription.getException() != null ? currentDescription.getException().getClass() : null;
        Class<?> thatExceptionClass = previousDescription.getException() != null ? previousDescription.getException().getClass() : null;
        if (thisExceptionClass != null ? !thisExceptionClass.equals(thatExceptionClass) : thatExceptionClass != null) {
            return true;
        }

        String thisExceptionMessage = currentDescription.getException() != null ? currentDescription.getException().getMessage() : null;
        String thatExceptionMessage = previousDescription.getException() != null ? previousDescription.getException().getMessage() : null;
        if (thisExceptionMessage != null ? !thisExceptionMessage.equals(thatExceptionMessage) : thatExceptionMessage != null) {
            return true;
        }

        return false;
    }

    private ServerDescriptionChangeEventHelper() {
    }
}
