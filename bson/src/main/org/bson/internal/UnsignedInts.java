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

import java.math.BigInteger;

/**
 * Utilities for treating int values as unsigned.
 *
 * <p>
 * Similar methods are now available in Java 8, but are required here for Java 6/7 compatibility.
 * </p>
 * <p>
 * This class is not part of the public API and may be removed or changed at any time.
 * </p>
 */
public final class UnsignedInts {
    /**
     * Equivalent of Integer.compareUnsigned in Java 8.
     *
     * @param first  the first value
     * @param second the second value
     * @return 0 if the values are equal, a value greater than zero if first is greater than second,
     * a value less than zero if first is less than second
     */
    public static int compare(final int first, final int second) {
        return compareIntegers(first + Integer.MIN_VALUE, second + Integer.MIN_VALUE);
    }

    /**
     * Equivalent to Integer.toUnsignedString in Java 8.
     *
     * @param value the int value to treat as unsigned
     * @return the string representation of unsignedInteger treated as an unsigned value
     */
    public static String toString(final int value) {
        if (value >= 0) {
            return Integer.toString(value);
        } else {
            // emulate unsigned division and then append the remainder
            int quotient = (value >>> 1) / 5;   // Unsigned divide by 10 and floor
            int remainder = value - quotient * 10;
            return Integer.toString(quotient) + remainder;
        }
    }

    //

    /**
     * Equivalent to Integer.parseUnsignedInt in Java 8.
     *
     * @param string the string representation of an unsigned int
     * @return the unsigned int
     */
    public static int parse(final String string) {
        if (string.length() == 0) {
            throw new NumberFormatException("empty string");
        }
        int radix = 10;
        int maxSafePos = MAX_SAFE_DIGITS[radix] - 1;
        int value = 0;
        for (int pos = 0; pos < string.length(); pos++) {
            int digit = Character.digit(string.charAt(pos), radix);
            if (digit == -1) {
                throw new NumberFormatException(string);
            }
            if (pos > maxSafePos && overflowInParse(value, digit, radix)) {
                throw new NumberFormatException("Too large for unsigned int: " + string);
            }
            value = (value * radix) + digit;
        }

        return value;
    }

    // Returns true if (current * radix) + digit is a number too large to be represented by an
    // unsigned int. This is useful for detecting overflow while parsing a string representation of a
    // number.
    private static boolean overflowInParse(final int current, final int digit, final int radix) {
        if (current >= 0) {
            if (current < MAX_VALUE_DIVS[radix]) {
                return false;
            }
            if (current > MAX_VALUE_DIVS[radix]) {
                return true;
            }
            // current == maxValueDivs[radix]
            return (digit > MAX_VALUE_MODS[radix]);
        }

        // current < 0: high bit is set
        return true;
    }

    // this is the equivalent of Integer.compare in Java 7
    private static int compareIntegers(final int x, final int y) {
        return (x < y) ? -1 : ((x == y) ? 0 : 1);
    }

    // Returns dividend / divisor, where the dividend and divisor are treated as unsigned 32-bit quantities.
    private static int divide(final int dividend, final int divisor) {
        if (divisor < 0) { // i.e., divisor >= 2^31:
            if (compare(dividend, divisor) < 0) {
                return 0; // dividend < divisor
            } else {
                return 1; // dividend >= divisor
            }
        }

        // Optimization - use signed division if dividend < 2^31
        if (dividend >= 0) {
            return dividend / divisor;
        }


        // Otherwise, approximate the quotient, check, and correct if necessary. Our approximation is
        // guaranteed to be either exact or one less than the correct value. This follows from fact that
        // floor(floor(x)/i) == floor(x/i) for any real x and integer i != 0. The proof is not quite
        // trivial.
        int quotient = ((dividend >>> 1) / divisor) << 1;
        int rem = dividend - quotient * divisor;
        return quotient + (compare(rem, divisor) >= 0 ? 1 : 0);
    }

    // Returns dividend % divisor, where the dividend and divisor are treated as unsigned 32-bit* quantities.
    private static int remainder(final int dividend, final int divisor) {
        if (divisor < 0) { // i.e., divisor >= 2^31:
            if (compare(dividend, divisor) < 0) {
                return dividend; // dividend < divisor
            } else {
                return dividend - divisor; // dividend >= divisor
            }
        }

        // Optimization - use signed modulus if dividend < 2^31
        if (dividend >= 0) {
            return dividend % divisor;
        }


        // Otherwise, approximate the quotient, check, and correct if necessary. Our approximation is
        // guaranteed to be either exact or one less than the correct value. This follows from the fact
        // that floor(floor(x)/i) == floor(x/i) for any real x and integer i != 0. The proof is not
        // quite trivial.
        int quotient = ((dividend >>> 1) / divisor) << 1;
        int rem = dividend - quotient * divisor;
        return rem - (compare(rem, divisor) >= 0 ? divisor : 0);
    }

    private static final int MAX_VALUE = -1; // Equivalent to 2^31 - 1
    private static final int[] MAX_VALUE_DIVS = new int[Character.MAX_RADIX + 1];
    private static final int[] MAX_VALUE_MODS = new int[Character.MAX_RADIX + 1];
    private static final int[] MAX_SAFE_DIGITS = new int[Character.MAX_RADIX + 1];

    static {
        BigInteger overflow = new BigInteger("100000000", 16);
        for (int i = Character.MIN_RADIX; i <= Character.MAX_RADIX; i++) {
            MAX_VALUE_DIVS[i] = divide(MAX_VALUE, i);
            MAX_VALUE_MODS[i] = (int) remainder(MAX_VALUE, i);
            MAX_SAFE_DIGITS[i] = overflow.toString(i).length() - 1;
        }
    }

    private UnsignedInts() {
    }

}
