package com.mongodb.client;

import com.mongodb.MongoException;
import com.mongodb.MongoTimeoutException;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.junit.Test;

import static com.mongodb.ClusterFixture.getOcspShouldSucceed;

import static org.junit.Assert.fail;

public class OcspTest {
    @Test
    public void testTLS() {
        String options = "tls=true";
        try {
            connect(options);
        } catch (MongoTimeoutException e) {
            if (getOcspShouldSucceed()) {
                fail("Unexpected exception when using OCSP with tls=true: " + e);
            }
        }
    }

    private void connect(final String options) {
        String uri = "mongodb://localhost/?serverSelectionTimeoutMS=2000&" + options;
        try (MongoClient client = MongoClients.create(uri)) {
            client.getDatabase("admin").runCommand(new BsonDocument("ping", new BsonInt32(1)));
        }
    }
}
