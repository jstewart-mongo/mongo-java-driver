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

package com.mongodb.client.internal;

import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.binding.ClusterBinding;
import com.mongodb.binding.ConnectionSource;
import com.mongodb.binding.ReadWriteBinding;
import com.mongodb.binding.SingleServerBinding;
import com.mongodb.client.ClientSession;
import com.mongodb.connection.Cluster;
import com.mongodb.connection.ClusterType;
import com.mongodb.connection.Connection;
import com.mongodb.connection.Server;
import com.mongodb.connection.ServerDescription;
import com.mongodb.internal.session.ClientSessionContext;
import com.mongodb.selector.ReadPreferenceServerSelector;
import com.mongodb.session.SessionContext;

import static org.bson.assertions.Assertions.notNull;

/**
 * This class is not part of the public API and may be removed or changed at any time.
 */
public class ClientSessionBinding implements ReadWriteBinding {
    private ReadWriteBinding wrapped;
    private final ClientSession session;
    private final boolean ownsSession;
    private final ClientSessionContext sessionContext;
    private final Cluster cluster;

    public ClientSessionBinding(final ClientSession session, final boolean ownsSession, final ReadWriteBinding wrapped) {
        this.session = notNull("session", session);
        this.ownsSession = ownsSession;
        this.sessionContext = new SyncClientSessionContext(session);
        this.cluster = ((ClusterBinding) wrapped).getCluster();
        this.wrapped = notNull("wrapped", initShardedTxnWrapped(wrapped));
    }

    private ReadWriteBinding initShardedTxnWrapped(final ReadWriteBinding wrapped) {
        if (isActiveShardedTxn()) {
            setPinnedMongosAddress(wrapped);
            ReadPreference readPreference = wrapped.getReadPreference();
            wrapped.release();
            return new SingleServerBinding(cluster, session.getPinnedMongosAddress(), readPreference);
        }
        return wrapped;
    }

    private boolean isActiveShardedTxn() {
        return session.hasActiveTransaction() && cluster.getDescription().getType() == ClusterType.SHARDED;
    }

    private void setPinnedMongosAddress(final ReadWriteBinding wrapped) {
        if (session.getPinnedMongosAddress() == null) {
            Server server = cluster.selectServer(new ReadPreferenceServerSelector(wrapped.getReadPreference()));
            session.setPinnedMongosAddress(server.getDescription().getAddress());
        }
    }

    @Override
    public ReadPreference getReadPreference() {
        return wrapped.getReadPreference();
    }

    @Override
    public int getCount() {
        return wrapped.getCount();
    }

    @Override
    public ReadWriteBinding retain() {
        wrapped.retain();
        return this;
    }

    @Override
    public void release() {
        wrapped.release();
        closeSessionIfCountIsZero();
    }

    private void closeSessionIfCountIsZero() {
        if (getCount() == 0 && ownsSession) {
            session.close();
        }
    }

    @Override
    public ConnectionSource getReadConnectionSource() {
        setWrappedOnPinnedMongosReset();
        ConnectionSource readConnectionSource = wrapped.getReadConnectionSource();
        return new SessionBindingConnectionSource(readConnectionSource);
    }

    @Override
    public SessionContext getSessionContext() {
        return sessionContext;
    }

    @Override
    public ConnectionSource getWriteConnectionSource() {
        setWrappedOnPinnedMongosReset();
        ConnectionSource writeConnectionSource = wrapped.getWriteConnectionSource();
        return new SessionBindingConnectionSource(writeConnectionSource);
    }

    private void setWrappedOnPinnedMongosReset() {
        if (isActiveShardedTxn() && session.getPinnedMongosAddress() == null) {
            setPinnedMongosAddress(wrapped);
            wrapped.release();
            wrapped = new SingleServerBinding(cluster, session.getPinnedMongosAddress(), wrapped.getReadPreference());
        }
    }

    private class SessionBindingConnectionSource implements ConnectionSource {
        private ConnectionSource wrapped;

        SessionBindingConnectionSource(final ConnectionSource wrapped) {
            this.wrapped = wrapped;
        }

        @Override
        public ServerDescription getServerDescription() {
            return wrapped.getServerDescription();
        }

        @Override
        public SessionContext getSessionContext() {
            return sessionContext;
        }

        @Override
        public Connection getConnection() {
            return wrapped.getConnection();
        }

        @Override
        @SuppressWarnings("checkstyle:methodlength")
        public ConnectionSource retain() {
            wrapped = wrapped.retain();
            return this;
        }

        @Override
        public int getCount() {
            return wrapped.getCount();
        }

        @Override
        public void release() {
            wrapped.release();
            closeSessionIfCountIsZero();
        }
    }

    private final class SyncClientSessionContext extends ClientSessionContext implements SessionContext {

        private final ClientSession clientSession;

        SyncClientSessionContext(final ClientSession clientSession) {
            super(clientSession);
            this.clientSession = clientSession;
        }

        @Override
        public boolean isImplicitSession() {
            return ownsSession;
        }

        @Override
        public boolean notifyMessageSent() {
            return clientSession.notifyMessageSent();
        }

        @Override
        public boolean hasActiveTransaction() {
            return clientSession.hasActiveTransaction();
        }

        @Override
        public ReadConcern getReadConcern() {
            if (clientSession.hasActiveTransaction()) {
                return clientSession.getTransactionOptions().getReadConcern();
            } else {
               return wrapped.getSessionContext().getReadConcern();
            }
        }
    }
}
