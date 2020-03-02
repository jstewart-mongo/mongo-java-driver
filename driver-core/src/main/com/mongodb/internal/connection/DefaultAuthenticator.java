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

import com.mongodb.AuthenticationMechanism;
import com.mongodb.MongoException;
import com.mongodb.MongoSecurityException;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.connection.ConnectionDescription;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;

import static com.mongodb.AuthenticationMechanism.MONGODB_X509;
import static com.mongodb.AuthenticationMechanism.SCRAM_SHA_1;
import static com.mongodb.AuthenticationMechanism.SCRAM_SHA_256;
import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.internal.operation.ServerVersionHelper.serverIsAtLeastVersionThreeDotZero;
import static com.mongodb.internal.operation.ServerVersionHelper.serverIsLessThanVersionFourDotZero;
import static java.lang.String.format;

class DefaultAuthenticator extends SpeculativeAuthenticator {
    static final int USER_NOT_FOUND_CODE = 11;
    private static final BsonString DEFAULT_MECHANISM_NAME = new BsonString(SCRAM_SHA_256.getMechanismName());
    private SpeculativeAuthenticator speculativeAuthenticator;

    DefaultAuthenticator(final MongoCredentialWithCache credential) {
        super(credential);
        isTrueArgument("unspecified authentication mechanism", credential.getAuthenticationMechanism() == null);
    }

    @Override
    void authenticate(final InternalConnection connection, final ConnectionDescription connectionDescription) {
        if (serverIsLessThanVersionFourDotZero(connectionDescription)) {
            getLegacyDefaultAuthenticator(connectionDescription)
                    .authenticate(connection, connectionDescription);
        } else {
            try {
                setSpeculativeAuthenticator(connectionDescription);
                speculativeAuthenticator.authenticate(connection, connectionDescription);
            } catch (Exception e) {
                throw wrapException(e);
            }
        }
    }

    @Override
    void authenticateAsync(final InternalConnection connection, final ConnectionDescription connectionDescription,
                           final SingleResultCallback<Void> callback) {
        if (serverIsLessThanVersionFourDotZero(connectionDescription)) {
            getLegacyDefaultAuthenticator(connectionDescription)
                    .authenticateAsync(connection, connectionDescription, callback);
        } else {
            setSpeculativeAuthenticator(connectionDescription);
            speculativeAuthenticator.authenticateAsync(connection, connectionDescription, callback);
        }
    }

    @Override
    public BsonDocument createSpeculativeAuthenticateCommand(final InternalConnection connection) {
        speculativeAuthenticator = getAuthenticatorForIsMaster();
        return speculativeAuthenticator != null ? speculativeAuthenticator.createSpeculativeAuthenticateCommand(connection) : null;
    }

    @Override
    public BsonDocument getSpeculativeAuthenticateResponse() {
        if (speculativeAuthenticator != null) {
            return speculativeAuthenticator.getSpeculativeAuthenticateResponse();
        }
        return null;
    }

    @Override
    public void setSpeculativeAuthenticateResponse(final BsonDocument response) {
        speculativeAuthenticator.setSpeculativeAuthenticateResponse(response);
    }

    private Authenticator getLegacyDefaultAuthenticator(final ConnectionDescription connectionDescription) {
        if (serverIsAtLeastVersionThreeDotZero(connectionDescription)) {
            return new ScramShaAuthenticator(getMongoCredentialWithCache().withMechanism(SCRAM_SHA_1));
        } else {
            return new NativeAuthenticator(getMongoCredentialWithCache());
        }
    }

    protected SpeculativeAuthenticator getAuthenticatorForIsMaster() {
        AuthenticationMechanism mechanism = getMongoCredential().getAuthenticationMechanism();

        if (mechanism == null) {
            return new ScramShaAuthenticator(getMongoCredentialWithCache().withMechanism(SCRAM_SHA_256));
        } else if (mechanism.equals(SCRAM_SHA_1) || mechanism.equals(SCRAM_SHA_256)) {
            return new ScramShaAuthenticator(getMongoCredentialWithCache().withMechanism(mechanism));
        } else if (mechanism.equals(MONGODB_X509)) {
            return new X509Authenticator(getMongoCredentialWithCache().withMechanism(mechanism));
        }
        return null;
    }

    private void setSpeculativeAuthenticator(final ConnectionDescription connectionDescription) {
        BsonArray saslSupportedMechanisms = connectionDescription.getSaslSupportedMechanisms();
        AuthenticationMechanism mechanism = saslSupportedMechanisms == null || saslSupportedMechanisms.contains(DEFAULT_MECHANISM_NAME)
                ? SCRAM_SHA_256 : SCRAM_SHA_1;

        if (speculativeAuthenticator == null || speculativeAuthenticator.getMongoCredential().getAuthenticationMechanism() != mechanism) {
            speculativeAuthenticator = new ScramShaAuthenticator(getMongoCredentialWithCache().withMechanism(mechanism));
        }
    }

    private MongoException wrapException(final Throwable t) {
        if (t instanceof MongoSecurityException) {
            return (MongoSecurityException) t;
        } else if (t instanceof MongoException && ((MongoException) t).getCode() == USER_NOT_FOUND_CODE) {
            return new MongoSecurityException(getMongoCredential(), format("Exception authenticating %s", getMongoCredential()), t);
        } else {
            return MongoException.fromThrowable(t);
        }
    }
}
