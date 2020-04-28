package com.mongodb.client;

import com.mongodb.MongoException;
import com.mongodb.MongoTimeoutException;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.junit.Test;

import static org.junit.Assert.fail;

public class OcspTest {
    private final boolean shouldSucceed = Integer.parseInt(System.getenv("OCSP_TLS_SHOULD_SUCCEED")) == 1;

    @Test
    public void testTLS() {
        String options = "tls=true";
        try {
            connect(options);
        } catch (MongoTimeoutException e) {
            if (shouldSucceed) {
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
