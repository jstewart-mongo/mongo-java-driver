/*
 * Copyright 2008-present MongoDB, Inc.
 * Copyright 2010 The Guava Authors
 * Copyright 2011 The Guava Authors
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

package org.bson.internal;

import org.junit.Test;

import java.math.BigInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class UnsignedIntsTest {

    @Test
    public void testCompare() {
        // max value
        assertTrue(UnsignedInts.compare(0, 0xffffffff) < 0);
        assertTrue(UnsignedInts.compare(0xffffffff, 0) > 0);

        // both with high bit set
        assertTrue(UnsignedInts.compare(0xff1a618b, 0xffffffff) < 0);
        assertTrue(UnsignedInts.compare(0xffffffff, 0xff1a618b) > 0);

        // one with high bit set
        assertTrue(UnsignedInts.compare(0x5a4316b8, 0xff1a618b) < 0);
        assertTrue(UnsignedInts.compare(0xff1a618b, 0x5a4316b8) > 0);

        // neither with high bit set
        assertTrue(UnsignedInts.compare(0x5a4316b8, 0x6cf78a4b) < 0);
        assertTrue(UnsignedInts.compare(0x6cf78a4b, 0x5a4316b8) > 0);

        // same value
        assertTrue(UnsignedInts.compare(0xff1a618b, 0xff1a618b) == 0);
    }

    @Test
    public void testParseInt() {
        assertEquals(0xffffffff, UnsignedInts.parse("4294967295"));
        assertEquals(0x7fffffff, UnsignedInts.parse("2147483647"));
        assertEquals(0xff1a618b, UnsignedInts.parse("4279918987"));
        assertEquals(0x5a4316b8, UnsignedInts.parse("1514346168"));
        assertEquals(0x6cf78a4b, UnsignedInts.parse("1828162123"));
    }

    @Test
    public void testToString() {
        String[] tests = {
                "ffffffff",
                "7fffffff",
                "ff1a618b",
                "5a4316b8",
                "6cf78a4b"
        };
        for (String x : tests) {
            BigInteger xValue = new BigInteger(x, 16);
            int xInt = xValue.intValue(); // signed
            assertEquals(xValue.toString(10), UnsignedInts.toString(xInt));
        }
    }

}
